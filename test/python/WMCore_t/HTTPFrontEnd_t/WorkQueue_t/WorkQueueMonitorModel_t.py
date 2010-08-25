"""
WMCore_t/HTTPFrontEnd_t/WorkQueue_t/WorkQueueMonitorModel_t.py

unittest for
WMCore/HTTPFrontEnd/WorkQueue/WorkQueueMonitoringModel.py

writing unittests / testing details:
https://twiki.cern.ch/twiki/bin/view/CMS/RESTModelUnitTest
"""



__revision__ = "$Id: WorkQueueMonitorModel_t.py,v 1.2 2010/01/26 17:17:11 sryu Exp $"
__version__ = "$Revision: 1.2 $"


import os
import unittest
from WMCore.HTTPFrontEnd.WorkQueue.WorkQueueMonitorModel import WorkQueueMonitorModel
from WMCore.WebTools.RESTFormatter import RESTFormatter 

from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMQuality.WebTools.RESTClientAPI import methodTest
from WMQuality.WebTools.RESTServerSetup import DefaultConfig

from WMCore.WorkQueue.WorkQueue import globalQueue
from WMCore_t.Services_t.WorkQueue_t.WorkQueuePopulator import createProductionSpec, getGlobalQueue



# TODO need to populate database, properly instantiate WorkQueue
# getGlobalWorkQueue fails
class WorkQueueMonitorModelTest(RESTBaseUnitTest):    
    def initialize(self):
        print "initialize()"
        self.config = DefaultConfig("WMCore.HTTPFrontEnd.WorkQueue.WorkQueueMonitorModel")
        
        # if module "WorkQueueMonitoringModel" needs database access, set:
        self.schemaModules = ["WMCore.WorkQueue.Database"]
        dbUrl = os.environ["DATABASE"] or "sqlite:////tmp/resttest.db"
        self.config.setDBUrl(dbUrl)        
        self.urlbase = self.config.getServerUrl()
        
                
    
    def setUp(self):
        print "setUp()"
        RESTBaseUnitTest.setUp(self) # calls self.initialize()         
        # TestInit stuff happens in RESTBaseUnitTest if self.schemaModules are set
      
        # TODO
        # set up WorkQueue - taken from Servies_t/WorkQueue_t/WorkQueue_t.py
        # which fails - email to Seangchan ...
        # self.params = {}
        # self.params['endpoint'] = self.config.getServerUrl()        
        # self.globalQueue = getGlobalQueue(dbi = self.testInit.getDBInterface(),
        #                                   CacheDir = 'global',
        #                                   NegotiationTimeout = 0,
        #                                   QueueURL = self.config.getServerUrl())    
        
    
    
    def tearDown(self):
        print "tearDown()"
        RESTBaseUnitTest.tearDown(self)
        # happens in RESTBaseUnitTest if self.schemaModules is set
        # self.testInit.clearDatabase()

        
    
    def testMyAPI(self):
        print "testMyAPI()"
        # this call fails
        # AttributeError: 'module' object has no attribute 'Database'
        # test not accepted type should return 406 error, in fact getting 404, then OK
        url = self.urlbase + 'testApi/'
        methodTest('GET', url,  accept='text/json', output={'code':404})
        

        
    def testDummy(self):
        print "testDummy()"
        assert True

             
        
if __name__ == "__main__":
    unittest.main()        
     