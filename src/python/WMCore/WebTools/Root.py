#!/usr/bin/env python
"""
_Root_

The root object for a webtools application. It loads all the different views and
starts an appropriately configured CherryPy instance. Views are loaded 
dynamically and can be turned on/off via configuration file.

"""

__revision__ = "$Id: Root.py,v 1.44 2010/02/10 16:33:06 sfoulkes Exp $"
__version__ = "$Revision: 1.44 $"

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
from WMCore.Agent.Harness import Harness

class Root(WMObject, Harness):
    def __init__(self, config):
        Harness.__init__(self, config, compName = "Webtools")
        self.appconfig = config.section_(self.config.Webtools.application)
        self.app = self.config.Webtools.application
        self.homepage = None
        self.secconfig = getattr(config, "SecurityModule", None)     

    def initInThread(self):
        return

    def preInitialization(self):
        self.start()
    
    def validateConfig(self):
        # Check that the configuration has the required sections
        config_dict = self.appconfig.dictionary_()
        must_have_keys = ['admin', 'description', 'title', 'templates']
        for key in must_have_keys:
            msg  = "Application configuration '%s' does not contain '%s' key"\
                    % (self.app, key)
            assert config_dict.has_key(key), msg

    def configureCherryPy(self):
        #Configure CherryPy
        try:
            cpconfig.update ({"server.environment": self.config.Webtools.environment})
        except:
            cpconfig.update ({"server.environment": 'production'})
        try:
            cpconfig.update ({"server.socket_port": int(self.config.Webtools.port)})
        except:
            cpconfig.update ({"server.socket_port": 8080})
        try:
            cpconfig.update ({"server.socket_host": self.config.Webtools.host})
        except:
            cpconfig.update ({"server.socket_host": 'localhost'})
        try:
            cpconfig.update ({'tools.expires.secs': int(self.config.Webtools.expires)})
        except:
            cpconfig.update ({'tools.expires.secs': 300})
        try:
            cpconfig.update ({'log.screen': bool(self.config.Webtools.log_screen)})
        except:
            cpconfig.update ({'log.screen': True})
        try:
            cpconfig.update ({'log.access_file': self.config.Webtools.access_log_file})
        except:
            cpconfig.update ({'log.access_file': None})
        try:
            cpconfig.update ({'log.error_file': self.config.Webtools.error_log_file})
        except:
            cpconfig.update ({'log.error_file': None})
        try:
            log.error_log.setLevel(self.config.Webtools.error_log_level)
        except:     
            log.error_log.setLevel(logging.DEBUG)
        try:
            log.access_log.setLevel(self.config.Webtools.access_log_level)
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
        if self.secconfig:
            from WMCore.WebTools.OidConsumer import OidConsumer
            
            cpconfig.update({'tools.sessions.on': True,
                             'tools.sessions.name': 'oidconsumer_sid'})
            tools.cernoid = OidConsumer(self.secconfig)
            if hasattr(self.secconfig, "default"):
                # The following will force the auth stuff to be called
                # even for non-decorated methods
                cpconfig.update({'tools.cernoid.on': True})
                cpconfig.update({'tools.cernoid.role': self.secconfig.default.role})
                cpconfig.update({'tools.cernoid.group': self.secconfig.default.group})
                cpconfig.update({'tools.cernoid.site': self.secconfig.default.site})
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
        component.application = self.config.Webtools.application
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
        self.validateConfig()
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
