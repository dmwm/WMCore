import os
import cherrypy
import logging 
from WMCore.Configuration import Configuration
from WMCore.WebTools.Root import Root

class DefaultConfig(Configuration):
    
    def __init__(self, model=None):
        Configuration.__init__(self)
        
        self.component_('Webtools')
        self.Webtools.application = 'UnitTests'
        self.Webtools.log_screen = False
        self.Webtools.access_file = '/tmp/webtools/log_access'
        self.Webtools.error_file = '/tmp/webtools/log_error'
        self.Webtools.port = 8888
        self.Webtools.host = "localhost"
        self.component_('UnitTests')
        self.UnitTests.title = 'CMS WMCore/WebTools Unit Tests'
        self.UnitTests.description = 'Dummy server for the running of unit tests' 
        self.UnitTests.admin ="UnitTestAdmin"
        self.UnitTests.templates = "/tmp"
        self.UnitTests.section_('views')
        
        active = self.UnitTests.views.section_('active')
        active.section_('rest')
        active.rest.application = 'UnitTestRESTApp'
        active.rest.object = 'WMCore.WebTools.RESTApi'
        active.rest.templates = '/tmp'
        active.rest.section_('database')
        active.rest.database.connectUrl = 'sqlite://'
        #active.rest.database = 'sqlite:////tmp/resttest.db'
        active.rest.section_('model')
        active.rest.model.object = model or 'WMCore.WebTools.RESTModel'
        active.rest.section_('formatter')
        active.rest.formatter.object = 'WMCore.WebTools.RESTFormatter'
        active.rest.formatter.templates = '/tmp'
    
    def getServerUrl(self):
        return "http://%s:%s/rest/" % (self.Webtools.host, self.Webtools.port)
                                     
    def getDBUrl(self):
        return self.UnitTests.views.active.rest.database.connectUrl
    
    def getDBSocket(self):
        return self.UnitTests.views.active.rest.database.socket
    
    def setDBUrl(self, dbUrl):
        self.UnitTests.views.active.rest.database.connectUrl = dbUrl 
    
    def setDBSocket(self, socket):
        self.UnitTests.views.active.rest.database.socket = socket 
        
    def setModel(self, model):
        self.UnitTests.views.active.rest.model.object = model
    
    def setHost(self, host):
        self.Webtools.host = host
    
    def setPort(self, port):
        self.Webtools.port = port
        
    def setFormatter(self, formatter):
        self.UnitTests.views.active.rest.formatter.object = formatter
        
    def getModelConfig(self):
        return self.UnitTests.views.active.rest
        

def configureServer(restModel='WMCore.WebTools.RESTModel', das=False, config=None):
    """
    either pass custom config or use default config,
    if default config is used, rest model and format time can be rest.
    """
    if config:
        dummycfg = config
    else:
        dummycfg = DefaultConfig(restModel)
    dummycfg.setModel(restModel)
    
    if das:
        dummycfg.setFormatter('WMCore.WebTools.DASRESTFormatter')
    rt = Root(dummycfg)
    return rt

def setUpDAS(func):
    def wrap_function(self):
        self.dasFlag = True
        func(self)
    return wrap_function

def serverSetup(func):
    def wrap_function(self):
        rt = configureServer(restModel=self.restModel, das=self.dasFlag, config=self.config)
        rt.start(blocking=False)
        cherrypy.log.error_log.setLevel(logging.WARNING)
        cherrypy.log.access_log.setLevel(logging.WARNING)
        func(self)
        rt.stop()
    return wrap_function
