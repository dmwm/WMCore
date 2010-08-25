import cherrypy
import logging 
from WMCore.Configuration import Configuration
from WMCore.WebTools.Root import Root

def getDefaultConfig():
    dummycfg = Configuration()
    dummycfg.component_('Webtools')
    dummycfg.Webtools.application = 'UnitTests'
    dummycfg.Webtools.log_screen = False
    dummycfg.Webtools.access_file = '/dev/null'
    dummycfg.Webtools.error_file = '/dev/null'
    dummycfg.Webtools.port = 8080
    dummycfg.Webtools.host = "localhost"
    dummycfg.component_('UnitTests')
    dummycfg.UnitTests.title = 'CMS WMCore/WebTools Unit Tests'
    dummycfg.UnitTests.description = 'Dummy server for the running of unit tests' 
    dummycfg.UnitTests.section_('views')
    
    active = dummycfg.UnitTests.views.section_('active')
    active.section_('rest')
    active.rest.object = 'WMCore.WebTools.RESTApi'
    active.rest.templates = '/tmp'
    active.rest.database = 'sqlite://'
    active.rest.section_('model')
    active.rest.model.object = 'WMCore.WebTools.RESTModel'
    active.rest.section_('formatter')
    active.rest.formatter.object = 'WMCore.WebTools.RESTFormatter'
    active.rest.formatter.templates = '/tmp'
    return dummycfg

def getDefaultServerURL():
    return "http://localhost:8080/rest"
    
def configureServer(restModel='WMCore.WebTools.RESTModel', das=False, config=None):
    """
    either pass custom config or use default config,
    if default config is used, rest model and format time can be rest.
    """
    if config:
        dummycfg = config
    else:
        dummycfg = getDefaultConfig()
        active = dummycfg.UnitTests.views.active
        active.rest.model.object = restModel
        active.rest.section_('formatter')
        if das:
            active.rest.formatter.object = 'WMCore.WebTools.DASRESTFormatter'
    rt = Root(dummycfg)
    return rt

def setUpDAS(func):
    def wrap_function(self):
        self.dasFlag = True
        func(self)
    return wrap_function

def serverSetup(func):
    def wrap_function(self):
        rt = configureServer(restModel=self.restModel, das=self.dasFlag)
        rt.start(blocking=False)
        cherrypy.log.error_log.setLevel(logging.WARNING)
        cherrypy.log.access_log.setLevel(logging.WARNING)
        func(self)
        rt.stop()
    return wrap_function