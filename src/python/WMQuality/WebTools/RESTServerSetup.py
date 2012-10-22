import os
import cherrypy
import logging
from functools import wraps

from WMCore.WebTools.Root import Root
from WMCore.Configuration import Configuration

#this function will be used for cherrypy set up for test
def cherrypySetup(config = None):
    if config == None:
        config = DefaultConfig()
    def chSetup(f):
        @wraps(f)
        def wrapper(self):
            self.rt = Root(config, testName = f.__name__)
            self.rt.start(blocking=False)
            self.urlbase = config.getServerUrl()
            f(self)
            self.rt.stop()
            self.rt.setLastTest()
        return wrapper

    return chSetup


class DefaultConfig(Configuration):

    def __init__(self, model=None):
        Configuration.__init__(self)
        self.component_('SecurityModule')
        self.SecurityModule.dangerously_insecure = True

        self.component_('Webtools')
        self.Webtools.application = 'UnitTests'
        self.Webtools.log_screen = False
        self.Webtools.environment = "development"
        self.Webtools.error_log_level = logging.DEBUG
        self.Webtools.access_log_level = logging.DEBUG
        self.Webtools.port = 8888
        self.Webtools.host = "localhost"
        self.Webtools.expires = 300

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
        #active.rest.templates = '/tmp'
        active.rest.section_('database')
        active.rest.database.connectUrl = 'sqlite://'
        #active.rest.database = 'sqlite:////tmp/resttest.db'
        active.rest.section_('model')
        active.rest.model.object = model or 'WMCore.WebTools.RESTModel'
        active.rest.section_('formatter')
        active.rest.formatter.object = 'WMCore.WebTools.RESTFormatter'
        active.rest.formatter.templates = '/tmp'
        #WARNING: need is not actual config - if cherrypy is started by Root.py
        #This will handled automatically - added here just for DummyModel test.
        active.rest.default_expires = self.Webtools.expires

    def getServerUrl(self):
        return "http://%s:%s/%s/rest/" % (self.Webtools.host, self.Webtools.port, self.Webtools.application.lower())

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

    def setWorkQueueLevel(self, queueLevel):
        """only set this for workqueue restmodel test
           queueLevel should be 'GlobalQueue' or 'LocalQueue'
        """

        self.UnitTests.views.active.rest.level = queueLevel

    def setWorkQueueCouchConfig(self):
        """only set this for workqueue restmodel test
           queueLevel should be 'GlobalQueue' or 'LocalQueue'
        """
        self.UnitTests.views.active.rest.section_('couchConfig')
        self.UnitTests.views.active.rest.couchConfig.couchURL = "http://blah.blah"
        self.UnitTests.views.active.rest.couchConfig.acdcDBName = "TestACDC"
        self.UnitTests.views.active.rest.couchConfig.jobDumpDBName = "TestJobDB"

    def getModelConfig(self):
        return self.UnitTests.views.active.rest
