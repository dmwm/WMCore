#!/usr/bin/env python
"""
_SiteDB_

The root object for SiteDB. It loads all the different views and starts 
CherryPy. When called it will load the SiteList view by default. Views are 
loaded dynamically and can be turned on/off via configuration file.


"""

__revision__ = "$Id: Root.py,v 1.12 2009/01/26 23:45:17 rpw Exp $"
__version__ = "$Revision: 1.12 $"

# CherryPy
from cherrypy import quickstart, expose, server, log
from cherrypy import config as cpconfig
# configuration and arguments
#FIXME
from WMCore.Configuration import Configuration
from WMCore.Configuration import loadConfigurationFile
from optparse import OptionParser
# Factory to load pages dynamically
from WMCore.WMFactory import WMFactory
# Database access and DAO 
from WMCore.Database.DBCore import DBInterface
from WMCore.Database.DBFactory import DBFactory
from WMCore.DAOFactory import DAOFactory
# Logging
import WMCore.WMLogging
import logging 
from WMCore.DataStructs.WMObject import WMObject

class Root(WMObject):
    def __init__(self, config):
        self.config = config
        self.config = config.section_("Webtools")
        self.appconfig = config.section_(self.config.application)
        self.app = self.config.application
        #self.configureCherryPy()
        #self.loadPages()
 
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
            cpconfig.update ({'log.error_file': int(self.config.error_log_file)})
        except:
            cpconfig.update ({'log.error_file': None})

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
        log("loading config: %s" % cpconfig, context=self.app, severity=logging.DEBUG, traceback=False)

    def loadPages(self):
        factory = WMFactory('webtools_factory')

        for view in self.appconfig.views.active:
            config = Configuration()
            component = config.component_(view._internal_name)
            component.application = self.config.application
            
            if hasattr(self.appconfig, 'templates'):
                component.templates = self.appconfig.templates
            if hasattr(self.appconfig, 'database'):
                component.database = self.appconfig.database
            
            dict = view.dictionary_()
            for k in dict.keys():
                component.__setattr__(k, dict[k])
            
            log("loading %s" % view._internal_name, context=self.app, 
                severity=logging.INFO, traceback=False)
            
            log("configuration for %s: %s" % (view._internal_name, view), 
                context=self.app, 
                severity=logging.INFO, traceback=False)
            
            if hasattr(view, 'database'):
                log("loading database for %s" % (view._internal_name), 
                    context=self.app, 
                severity=logging.INFO, traceback=False)
                view.database = self.loadDatabase(view)
        
            log("Loading %s" % (view._internal_name), 
                                context=self.app,
                                severity=logging.DEBUG, 
                                traceback=False)
            # Load the object
            obj = factory.loadObject(view.object, component)
            # Attach the object to cherrypy's root, at the name of the view 
            eval(compile("self.%s = obj" % view._internal_name, '<string>', 'single'))
        
            log("%s available on %s/%s" % (view._internal_name, 
                                           server.base(), 
                                           view._internal_name), 
                                           context=self.app, 
                                           severity=logging.INFO, 
                                           traceback=False)
        
        for i in self.appconfig.views.maintenance:
            #TODO: Show a maintenance page
            pass
    
    def loadDatabase(self, configSection):
        dblist = []
        if hasattr(configSection, 'database'):
            #Configure the database if needed, replace with thread style?
            for dburl in self.makelist(configSection.database):
                try:
                    conn = DBFactory(log.error_log, dburl).connect()
                    daofactory = DAOFactory(package=self.app, logger=log.error_log, dbinterface=conn)
                except:
                    log("Cannot connect to %s" % config['database'], context=self.app, 
                        severity=logging.WARNING, traceback=False)
    
        return self.flatten(dblist)
    
    @expose
    def index(self):
        index = self.appconfig.index
        return eval('self.%s.index()' % index)
    
    @expose
    def default(self, *args, **kwargs):
        index = self.appconfig.index
        return eval('self.%s.default(*args, **kwargs)' % index)

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
    root.configureCherryPy()
    root.loadPages()
    quickstart(root)
    #quickstart(Root(cfg))
