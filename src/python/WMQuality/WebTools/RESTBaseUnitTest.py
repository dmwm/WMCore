import unittest
import cherrypy
import logging
import threading

#decorator import for RESTServer setup
from WMQuality.WebTools.RESTServerSetup import DefaultConfig
from WMCore.WebTools.Root import Root

class RESTBaseUnitTest(unittest.TestCase):
    
    def setUp(self, initRoot = True):
        # default set
        self.schemaModules = []
        self.initialize()
        if self.schemaModules:
            import warnings
            warnings.warn("use RESTAndCouchUnitTest instead", DeprecationWarning)
            from WMQuality.TestInitCouchApp import TestInitCouchApp
            self.testInit = TestInitCouchApp(__file__)
            self.testInit.setLogging() # logLevel = logging.SQLDEBUG
            self.testInit.setDatabaseConnection()
            self.testInit.setSchema(customModules = self.schemaModules,
                                    useDefault = False)
            # Now pull the dbURL from the factory
            # I prefer this method because the factory has better error handling
            # Also because then you know everything is the same
            myThread = threading.currentThread()
            self.config.setDBUrl(myThread.dbFactory.dburl)
            
        logging.info("This is our config: %s" % self.config)

        self.initRoot = initRoot
        if initRoot:
            self.rt = Root(self.config)
            self.rt.start(blocking=False)
        return
        
    def tearDown(self):
        if self.initRoot:
            self.rt.stop()
        if self.schemaModules:
            self.testInit.clearDatabase()
        self.config = None
        return
    
    def initialize(self):
        """
        i.e.
        
        self.config = DefaultConfig('WMCore.WebTools.RESTModel')
        self.config.setDBUrl('sqlite://')
        self.schemaModules = ['WMCore.ThreadPool', 'WMCore.WMBS']
        """
        
        message = "initialize method has to be implemented, self.restModel, self.schemaModules needs to be set"
        raise NotImplementedError, message
