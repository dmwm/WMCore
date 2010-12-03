import unittest
import cherrypy
import logging

#decorator import for RESTServer setup
from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMQuality.WebTools.RESTServerSetup import DefaultConfig
from WMCore.WebTools.Root import Root

class RESTBaseUnitTest(unittest.TestCase):
    
    def setUp(self):
        # default set
        self.schemaModules = []
        self.initialize()
        if self.schemaModules:
            print "Initializing schema : %s" % self.schemaModules
            self.testInit = TestInitCouchApp(__file__)
            self.testInit.setLogging() # logLevel = logging.SQLDEBUG
            print "Database URL: %s" % self.config.getDBUrl()
            self.testInit.setDatabaseConnection(self.config.getDBUrl())
            #self.testInit.setDatabaseConnection()
            self.testInit.setSchema(customModules = self.schemaModules,
                                    useDefault = False)
        
        print "Starting Cherrypy server ..."
        self.rt = Root(self.config)
        self.rt.start(blocking=False)
        cherrypy.log.error_log.setLevel(logging.WARNING)
        cherrypy.log.access_log.setLevel(logging.WARNING)
        
    def tearDown(self):

        print "Stopping Cherrypy server ..."
        self.rt.stop()
        
        if self.schemaModules:
            print "Cleaning up database ..."
            self.testInit.clearDatabase()

        self.config = None
        
    
    def initialize(self):
        """
        i.e.
        
        self.config = DefaultConfig('WMCore.WebTools.RESTModel')
        self.config.setDBUrl("sqlite://")
        self.schemaModules = ["WMCore.ThreadPool", WMCore.WMBS"]
        """
        
        message = "initialize method has to be implemented, self.restModel, self.schemaModules needs to be set"
        raise NotImplementedError, message
