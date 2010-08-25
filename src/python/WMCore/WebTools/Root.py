#!/usr/bin/env python
"""
_Root_

The root object for a webtools application. It loads all the different views and
starts an appropriately configured CherryPy instance. Views are loaded 
dynamically and can be turned on/off via configuration file.

"""

__revision__ = "$Id: Root.py,v 1.39 2010/01/21 14:47:28 valya Exp $"
__version__ = "$Revision: 1.39 $"

# CherryPy
import cherrypy
from cherrypy import quickstart, expose, server, log, tree, engine, dispatch, tools
from cherrypy import config as cpconfig
# configuration and arguments
#FIXME
from WMCore.Configuration import Configuration
from WMCore.Configuration import loadConfigurationFile
from optparse import OptionParser
# Factory to load pages dynamically
from WMCore.WMFactory import WMFactory
# Database access and DAO 
#from WMCore.Database.DBCore import DBInterface
#from WMCore.Database.DBFactory import DBFactory
#from WMCore.DAOFactory import DAOFactory
# Logging
import WMCore.WMLogging
import logging 
from WMCore.DataStructs.WMObject import WMObject
from WMCore.WebTools.Welcome import Welcome

from WMCore.WebTools.OidConsumer import OidConsumer 

class Root(WMObject):
    def __init__(self, config):
        self.config = config.section_("Webtools")
        self.appconfig = config.section_(self.config.application)
        self.app = self.config.application
        config_dict = self.appconfig.dictionary_()
        must_have_keys = ['admin', 'description', 'title', 'templates']
        for key in must_have_keys:
            if  not config_dict.has_key(key):
                msg  = "Application configuration "
                msg += "'%s' does not contain '%s' key"\
                        % (self.app, key)
                raise Exception(msg)
        self.homepage = None
        self.secconfig = config.section_("SecurityModule")
        
    def configureCherryPy(self):
        #Configure CherryPy
        try:
            cpconfig.update ({"server.environment": self.config.environment})
        except:
            cpconfig.update ({"server.environment": 'production'})
        try:
            cpconfig.update ({"server.socket_port": int(self.config.port)})
        except:
            cpconfig.update ({"server.socket_port": 8080})
        try:
            cpconfig.update ({"server.socket_host": self.config.host})
        except:
            cpconfig.update ({"server.socket_host": 'localhost'})
        try:
            cpconfig.update ({'tools.expires.secs': int(self.config.expires)})
        except:
            cpconfig.update ({'tools.expires.secs': 300})
        try:
            cpconfig.update ({'log.screen': bool(self.config.log_screen)})
        except:
            cpconfig.update ({'log.screen': True})
        try:
            cpconfig.update ({'log.access_file': self.config.access_log_file})
        except:
            cpconfig.update ({'log.access_file': None})
        try:
            cpconfig.update ({'log.error_file': self.config.error_log_file})
        except:
            cpconfig.update ({'log.error_file': None})
        try:
            log.error_log.setLevel(self.config.error_log_level)
        except:     
            log.error_log.setLevel(logging.DEBUG)
        try:
            log.access_log.setLevel(self.config.access_log_level)
        except:
            log.access_log.setLevel(logging.DEBUG)
        cpconfig.update ({
                          'tools.expires.on': True,
                          'tools.response_headers.on':True,
                          'tools.etags.on':True,
                          'tools.etags.autotags':True,
                          'tools.encode.on': True,
                          'tools.gzip.on': True
                          })
        #cpconfig.update ({'request.show_tracebacks': False})
        #cpconfig.update ({'request.error_response': self.handle_error})
        #cpconfig.update ({'tools.proxy.on': True})
        #cpconfig.update ({'proxy.tool.base': '%s:%s' % (socket.gethostname(), opts.port)})

        # SecurityModule config
        if hasattr(self.secconfig, 'enabled'):
            cpconfig.update({'tools.sessions.on': True,
                             'tools.sessions.name': 'oidconsumer_sid'})
            tools.cernoid = OidConsumer(self.secconfig)
            pagecfg = self.secconfig
            pagecfg.object = pagecfg.handler
            self.mountPage(pagecfg, 
                           pagecfg.mount_point, 
                           self.appconfig.dictionary_(), 
                           WMFactory('webtools_factory'))    
            root.auth = tools.cernoid.defhandler

        log("loading config: %s" % cpconfig, 
                                   context=self.app, 
                                   severity=logging.DEBUG, 
                                   traceback=False)

    def loadPages(self):
        factory = WMFactory('webtools_factory')
        
        globalconf = self.appconfig.dictionary_()
        del globalconf['views'] 
        the_index = ''
        if 'index' in globalconf.keys():
            the_index = globalconf['index']
            del globalconf['index']
         
        for view in self.appconfig.views.active:
            #Iterate through each view's configuration and instantiate the class
            if view._internal_name != the_index:
                self.mountPage(view, view._internal_name, globalconf, factory)
                
        if hasattr(self.appconfig.views, 'maintenance'):
            for i in self.appconfig.views.maintenance:
                #TODO: Show a maintenance page with a 503 Service Unavailable header
                pass
    
    def mountPage(self, view, mount_point, globalconf, factory):
        config = Configuration()
        component = config.component_(view._internal_name)
        component.application = self.config.application
        for k in globalconf.keys():
            # Add the global config to the view
            component.__setattr__(k, globalconf[k])
        
        dict = view.dictionary_()
        for k in dict.keys():
            component.__setattr__(k, dict[k])
        # component now contains the full configuration (global + view)  
        # use this throughout 
        log("loading %s" % component._internal_name, context=self.app, 
            severity=logging.INFO, traceback=False)
        
        log("configuration for %s: %s" % (component._internal_name, 
                                component), 
                                context=self.app, 
                                severity=logging.INFO, traceback=False)
                            
        log("Loading %s" % (component._internal_name), 
                                context=self.app,
                                severity=logging.DEBUG, 
                                traceback=False)
        # Load the object
        obj = factory.loadObject(component.object, component)
        # Attach the object to cherrypy's tree, at the name of the component
        tree.mount(obj, "/%s" % mount_point)         
        log("%s available on %s/%s" % (component._internal_name, 
                                       server.base(), 
                                       component._internal_name), 
                                       context=self.app, 
                                       severity=logging.INFO, 
                                       traceback=False)
            
    def makeIndex(self):
        # now make the index page
        if hasattr(self.appconfig, 'index'):
            factory = WMFactory('webtools_factory')
            globalconf = self.appconfig.dictionary_()
            view = getattr(self.appconfig.views.active, globalconf['index'])
            del globalconf['views'] 
            del globalconf['index']
            self.mountPage(view, '/', globalconf, factory)
            
        else:
            log("No index defined for %s - instantiating default Welcome page" 
                                             % (self.app), 
                                           context=self.app, 
                                           severity=logging.INFO, 
                                           traceback=False)
            namesAndDocstrings = []
            # make a default Welcome
            for view in self.appconfig.views.active:
                if not getattr(view, "hidden", False):
                    viewName = view._internal_name
                    viewObj = tree.apps['/%s' % viewName].root
                    docstring = viewObj.__doc__
                    namesAndDocstrings.append((viewName, docstring))
            tree.mount(Welcome(namesAndDocstrings), "/")
    
    def start(self, blocking=True):
        # Configure and start the server 
        self.configureCherryPy()
        self.loadPages()        
        self.makeIndex()
        engine.start()
        if blocking:
            engine.block()
            
    def stop(self):
        engine.exit()
        engine.stop()
        
if __name__ == "__main__":
    config = __file__.rsplit('/', 1)[0] + '/DefaultConfig.py'
    parser = OptionParser()
    parser.add_option("-i", "--ini", dest="inifile", default=config,
                      help="write the configuration to FILE", metavar="FILE")
    parser.add_option("-v", "--verbose",
                      action="store_true", dest="verbose", default=False,
                      help="Be more verbose")
    opts, args = parser.parse_args()
    cfg = loadConfigurationFile(opts.inifile)
    root = Root(cfg)
    root.start()
