#!/usr/bin/env python
"""
_SiteDB_

The root object for SiteDB. It loads all the different views and starts 
CherryPy. When called it will load the SiteList view by default. Views are 
loaded dynamically and can be turned on/off via configuration file.


"""

__revision__ = "$Id: Root.py,v 1.1 2009/01/07 11:57:32 metson Exp $"
__version__ = "$Revision: 1.1 $"

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

class Root(object):
    def __init__(self, opts):
        self.config = self.loadConfig(opts.inifile)
        self.app = self.config.get('root', 'application')
        
        #Configure the database if needed
        try:
            dburl = self.config.get('database', 'dburl')
            conn = DBFactory(log.error_log, dburl).connect()
            self.daofactory = DAOFactory(package=self.app, logger=log.error_log, dbinterface=conn)
            opts.dao = self.daofactory
        except:
            log("No database defined for %s" % self.app, context=self.app, 
                severity=logging.WARNING, traceback=False)
            
        #Configure CherryPy
        cpconfig.update ({"server.environment": self.config.get('root', 'environment')})
        cpconfig.update ({"server.socket_port": int(self.config.get('root', 'port'))})
        #cpconfig.update ({'request.show_tracebacks': False})
        #cpconfig.update ({'request.error_response': self.handle_error})
        #cpconfig.update ({'tools.proxy.on': True})
        #cpconfig.update ({'proxy.tool.base': '%s:%s' % (socket.gethostname(), opts.port)})    
        
        
        self.loadPages(opts, self.config)
        
    def loadPages(self, opts, config):
        factory = WMFactory('webtools_factory')
        views = eval(config.get('views', 'active'))
        
        for i in views:
            log("loading %s" % i, context=self.app, severity=logging.DEBUG, traceback=False)
            theclass = config.get(i, 'class')
            
            args = [self.configToDict(config, i)]
            args[0]['root'] = self.configToDict(config, 'root')
            tmp = eval(config.get(i, 'init'))
            if type(tmp) == type([]):
                args.extend(tmp)
            elif tmp:
                args.append(tmp)
            
            database = eval(config.get(i, 'database'))
            if database:
                args.append(self.daofactory)
            if len(args):
                obj = factory.loadObject(theclass, args)
                eval(compile("self.%s = obj" % i, '<string>', 'single'))
            else:
                obj = factory.loadObject(theclass)
                eval(compile("self.%s = obj" % i, '<string>', 'single'))
            
            log("%s available on %s/%s" % (i, server.base(), i), 
                                        context=self.app, 
                                        severity=logging.DEBUG, 
                                        traceback=False)
        
        maint = eval(config.get('views', 'maintenance'))
        for i in maint:
            #TODO: Show a maintenance page
            pass
    
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
        index = self.config.get('root', 'index')
        return eval('self.%s.index()' % index)
    
    @expose
    def default(self, *args, **kwargs):
        index = self.config.get('root', 'index')
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