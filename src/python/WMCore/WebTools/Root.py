#!/usr/bin/env python
"""
_Root_

The root object for a webtools application. It loads all the different views and
starts an appropriately configured CherryPy instance. Views are loaded 
dynamically and can be turned on/off via configuration file.

"""

__revision__ = "$Id: Root.py,v 1.52 2010/04/26 21:16:09 sryu Exp $"
__version__ = "$Revision: 1.52 $"

# CherryPy
import cherrypy
from cherrypy import quickstart, expose, server, log, tree, engine, dispatch, tools
from cherrypy import config as cpconfig
# configuration and arguments
#FIXME
from WMCore.Agent.Daemon.Create import createDaemon
from WMCore.Agent.Daemon.Details import Details
from WMCore.Configuration import Configuration
from WMCore.Configuration import loadConfigurationFile
from optparse import OptionParser
# Factory to load pages dynamically
from WMCore.WMFactory import WMFactory
# Logging
import WMCore.WMLogging
import logging 
from WMCore.DataStructs.WMObject import WMObject
from WMCore.WebTools.Welcome import Welcome
from WMCore.Agent.Harness import Harness

class Root(WMObject, Harness):
    def __init__(self, config, webApp = None):
        self.homepage = None        
        if webApp == None:
            Harness.__init__(self, config, compName = "Webtools")
            self.appconfig = config.section_(self.config.Webtools.application)
            self.app = self.config.Webtools.application
            self.secconfig = config.component_("SecurityModule")
            self.serverConfig = config.section_("Webtools")
        else:
            Harness.__init__(self, config, compName = webApp)            
            self.appconfig = config.section_(webApp)            
            self.app = webApp
            self.secconfig = getattr(self.appconfig, "security")
            self.serverConfig = config.section_(webApp).section_("server")
            self.coreDatabase = config.section_("CoreDatabase")

        return

    def startComponent(self):
        """
        _startComponent_

        Called by the WMAgent harness code.  This will never return.
        """
        self.start()
        return
    
    def validateConfig(self):
        # Check that the configuration has the required sections
        config_dict = self.appconfig.dictionary_()
        must_have_keys = ['admin', 'description', 'title']
        for key in must_have_keys:
            msg  = "Application configuration '%s' does not contain '%s' key"\
                    % (self.app, key)
            assert config_dict.has_key(key), msg

    def configureCherryPy(self):
        """
        _configureCherryPy_

        """
        configDict = self.serverConfig.dictionary_()

        cpconfig["server.environment"] = configDict.get("environment", "production")
        cpconfig["server.thread_pool"] = configDict.get("thread_pool", 10)
        cpconfig["server.socket_port"] = configDict.get("port", 8080)
        cpconfig["server.socket_host"] = configDict.get("host", "localhost")
        cpconfig["tools.expires.secs"] = configDict.get("expires", 300)
        cpconfig["log.screen"] = bool(configDict.get("log_screen", True))
        cpconfig["log.access_file"] = configDict.get("access_log_file", None)
        cpconfig["log.error_file"] = configDict.get("error_log_file", None)
        
        #A little hacky way to pass the expire second to config
        self.appconfig.default_expires = cpconfig["tools.expires.secs"]
        
        log.error_log.setLevel(configDict.get("error_log_level", logging.DEBUG))
        log.access_log.setLevel(configDict.get("access_log_level", logging.DEBUG))

        cpconfig.update ({
                          'tools.expires.on': True,
                          'tools.response_headers.on':True,
                          'tools.etags.on':True,
                          'tools.etags.autotags':True,
                          'tools.encode.on': True,
                          'tools.gzip.on': True
                          })

        # SecurityModule config
        if len(self.secconfig.listSections_()) > 0:
            if self.secconfig.listSections_() == ["componentDir"]:
                return
            
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
            self.auth = tools.cernoid.defhandler

        log("loading config: %s" % cpconfig, 
                                   context=self.app, 
                                   severity=logging.DEBUG, 
                                   traceback=False)
        return

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
        """
        _mountPage_

        """
        config = Configuration()
        component = config.component_(view._internal_name)
        component.application = self.app
        
        for k in globalconf.keys():
            # Add the global config to the view
            component.__setattr__(k, globalconf[k])
        
        dict = view.dictionary_()
        for k in dict.keys():
            component.__setattr__(k, dict[k])

        if component.dictionary_().has_key('database'):
            if not type(component.database) == str:
                print component.database.listSections_()
                if len(component.database.listSections_()) == 0:
                    if len(self.coreDatabase.listSections_()) > 0:
                        component.database.connectUrl = self.coreDatabase.connectUrl
                        if hasattr(self.coreDatabase, "socket"):
                            component.database.socket = self.coreDatabase.socket

            print component.database

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
    parser.add_option("-d", "--daemonise",
                      action="store_true", dest="daemon", default=False,
                      help="Daemonise the cherrypy process, and return the PID")
    parser.add_option("-s", "--status",
                      action="store_true", dest="status", default=False,
                      help="Return the status of the daemon")
    parser.add_option("-k", "--kill",
                      action="store_true", dest="kill", default=False,
                      help="Kill the daemon")
    parser.add_option("-t", "--terminate",
                      action="store_true", dest="terminate", default=False,
                      help="Terminate the daemon (kill, wait, kill -9)")
    opts, args = parser.parse_args()
    cfg = loadConfigurationFile(opts.inifile)
    
    component = cfg.Webtools.application
    workdir = getattr(cfg.Webtools, 'componentDir', '/tmp/webtools')
    if workdir == None:
        workdir = '/tmp/webtools'
    root = Root(cfg)
    if opts.status:
        daemon = Details('%s/Daemon.xml' % workdir)
        
        if not daemon.isAlive():
            print "Component:%s Not Running" % component
        else:
            print "Component:%s Running:%s" % (component, daemon['ProcessID'])
    elif opts.kill:
        daemon = Details('%s/Daemon.xml' % workdir)
        daemon.kill()
        daemon.removeAndBackupDaemonFile()
    elif opts.terminate:
        daemon = Details('%s/Daemon.xml' % workdir)
        daemon.killWithPrejudice()
        daemon.removeAndBackupDaemonFile()  
    elif opts.daemon:
        createDaemon(workdir)
        root.start(False)
    else:
        root.start()
