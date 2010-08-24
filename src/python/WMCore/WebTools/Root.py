#!/usr/bin/env python
"""
_SiteDB_

The root object for SiteDB. It loads all the different views and starts 
CherryPy. When called it will load the SiteList view by default. Views are 
loaded dynamically and can be turned on/off via configuration file.


"""

__revision__ = "$Id: Root.py,v 1.4 2009/01/10 16:00:39 metson Exp $"
__version__ = "$Revision: 1.4 $"

# CherryPy
from cherrypy import quickstart, expose, server, log
from cherrypy import config as cpconfig
# configuration and arguments
from ConfigParser import ConfigParser
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
    def __init__(self, opts):
        self.opts = opts
        self.configfile = self.loadConfig(opts.inifile)
        self.app = self.configfile.get('root', 'application')
        
        #Configure CherryPy
        try:
            cpconfig.update ({"server.environment": self.configfile.get('root', 'environment')})
        except:
            cpconfig.update ({"server.environment": 'production'})
        try:
            cpconfig.update ({"server.socket_port": int(self.configfile.get('root', 'port'))})
        except:
            cpconfig.update ({"server.socket_port": 8080})
        try:
            cpconfig.update ({'tools.expires.secs': int(self.configfile.get('root', 'expires'))})
        except:
            cpconfig.update ({'tools.expires.secs': 300})
        try:
            cpconfig.update ({'log.screen': bool(self.configfile.get('root', 'log_screen'))})
        except:
            cpconfig.update ({'log.screen': True})
        try:
            cpconfig.update ({'log.access_file': self.configfile.get('root', 'access_log_file')})
        except:
            cpconfig.update ({'log.access_file': None})
        try:
            cpconfig.update ({'log.error_file': int(self.configfile.get('root', 'error_log_file'))})
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
        
        self.loadPages()
        
    def loadPages(self):
        factory = WMFactory('webtools_factory')
        views = eval(self.configfile.get('views', 'active'))
        # Read in global configuration, to be over-ridden if needs be
        globalconfig = {}
        globalconfig['application'] = self.configfile.get('root', 'application')
        try:
            globalconfig['templates'] = self.configfile.get('root', 'templates')
        except:
            pass
        try:
            globalconfig['database'] = self.configfile.get('root', 'database')
        except:
            pass
        
        for i in views:
            log("loading %s" % i, context=self.app, 
                severity=logging.INFO, traceback=False)
            
            config = {} 
            config.update(globalconfig)
            config.update(self.configToDict(self.configfile, i))
            
            log("configuration for %s: %s" % (i, config), context=self.app, 
                severity=logging.INFO, traceback=False)
            
            if 'database' in config.keys():
                config['database'] = self.loadDatabase(config)
            
            theclass = self.configfile.get(i, 'class')
                    
            obj = factory.loadObject(theclass, config)
            eval(compile("self.%s = obj" % i, '<string>', 'single'))
        
            log("%s available on %s/%s" % (i, server.base(), i), 
                                        context=self.app, 
                                        severity=logging.DEBUG, 
                                        traceback=False)
        
        maint = eval(self.configfile.get('views', 'maintenance'))
        for i in maint:
            #TODO: Show a maintenance page
            pass
    
    def loadDatabase(self, config):
        dblist = []
        if 'database' in config.keys():
            #Configure the database if needed, replace with thread style?
            for dburl in self.makelist(config['database']):
                try:
                    conn = DBFactory(log.error_log, dburl).connect()
                    daofactory = DAOFactory(package=self.app, logger=log.error_log, dbinterface=conn)
                except:
                    log("Cannot connect to %s" % config['database'], context=self.app, 
                        severity=logging.WARNING, traceback=False)
    
        return self.flatten(dblist)
    
    def configToDict(self, config, section):
        dict = {}
        for option in config.items(section):
            if option[1][0] in ['[', '{', '(']:
                dict[option[0]] = eval(option[1])
            else:
                dict[option[0]] = option[1]
        return dict
    
    def loadConfig(self, inifile):
        # Check permissions are 600
        # assert inifile is read only
        config = ConfigParser()
        config.read(inifile)
        return config
    
    @expose
    def index(self):
        index = self.configfile.get('root', 'index')
        return eval('self.%s.index()' % index)
    
    @expose
    def default(self, *args, **kwargs):
        index = self.configfile.get('root', 'index')
        return eval('self.%s.default(*args, **kwargs)' % index)

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-i", "--ini", dest="inifile", default='application.ini',
                      help="write the configuration to FILE", metavar="FILE")
    parser.add_option("-v", "--verbose",
                      action="store_true", dest="verbose", default=False,
                      help="Be more verbose")
    opts, args = parser.parse_args()
    
    quickstart(Root(opts))