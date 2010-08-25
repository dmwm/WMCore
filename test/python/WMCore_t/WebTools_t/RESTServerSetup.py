import cherrypy
import logging 
import urllib, urllib2
from WMCore.Configuration import Configuration
from WMCore.WebTools.Root import Root
from httplib import HTTPConnection

def configureServer(restModel='WMCore.WebTools.RESTModel', das=False):
    dummycfg = Configuration()
    dummycfg.component_('Webtools')
    dummycfg.Webtools.application = 'UnitTests'
    dummycfg.Webtools.log_screen = False
    dummycfg.Webtools.access_file = '/dev/null'
    dummycfg.Webtools.error_file = '/dev/null'
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
    active.rest.model.object = restModel
    active.rest.section_('formatter')
    if das:
        active.rest.formatter.object = 'WMCore.WebTools.DASRESTFormatter'
    else:
        active.rest.formatter.object = 'WMCore.WebTools.RESTFormatter'
    active.rest.formatter.templates = '/tmp'
    
    rt = Root(dummycfg)
    return rt

def setUpDummyRESTModel(func):
    def wrap_function(self):
        self.restModel = "DummyRESTModel"
        func(self)
    return wrap_function

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

def makeRequest(uri='/rest/', values=None, type='GET', accept="text/plain"):
    headers = {}
    headers = {"Content-type": "application/x-www-form-urlencoded",
               "Accept": accept}
    data = None
    if values:
        data = urllib.urlencode(values)
    if type != 'POST' and data != None:
        uri = '%s?%s' % (uri, data)
    conn = HTTPConnection('localhost:8080')
    conn.connect()
    conn.request(type, uri, data, headers)
    response = conn.getresponse()
    
    data = response.read()
    conn.close()
    type = response.getheader('content-type').split(';')[0]
    return data, response.status, type, response