import unittest
import cherrypy
import logging

#decorator import for RESTServer setup
from WMQuality.WebTools.RESTServerSetup import DefaultConfig
from WMCore.WebTools.Root import Root

class RESTBaseUnitTest(unittest.TestCase):
    
    def setUp(self):
        # default set
        self.schemaModules = []
        self.initialize()
        if self.schemaModules:
            self.testInit.setLogging() # logLevel = logging.SQLDEBUG
            self.testInit.setDatabaseConnection(self.config.getDBUrl())
            self.testInit.setSchema(customModules = self.schemaModules,
                                    useDefault = False)
        
        self.rt = Root(self.config)
        self.rt.start(blocking=False)
        
    def tearDown(self):
        self.rt.stop()
        if self.schemaModules:
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
