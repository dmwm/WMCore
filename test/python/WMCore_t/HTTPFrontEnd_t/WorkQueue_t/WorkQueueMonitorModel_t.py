"""
WMCore_t/HTTPFrontEnd_t/WorkQueue_t/WorkQueueMonitorModel_t.py

unittest for
WMCore/HTTPFrontEnd/WorkQueue/WorkQueueMonitoringModel.py

writing unittests / testing details:
https://twiki.cern.ch/twiki/bin/view/CMS/RESTModelUnitTest - doesn't work (26/01)

guiding / reference classes:
test/python/WMCore_t/Services_t/WorkQueue_t/WorkQueue_t.py - fails (26/01)
test/python/WMCore_t/WorkQueue_t/WorkQueue_t.py (use use WMCore_t.WMSpec_t.samples.*) - works
"""



__revision__ = "$Id: WorkQueueMonitorModel_t.py,v 1.3 2010/01/27 14:59:14 maxa Exp $"
__version__ = "$Revision: 1.3 $"


import os
import shutil
import unittest
from WMCore.HTTPFrontEnd.WorkQueue.WorkQueueMonitorModel import WorkQueueMonitorModel
from WMCore.WebTools.RESTFormatter import RESTFormatter 

from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMQuality.WebTools.RESTClientAPI import methodTest
from WMQuality.WebTools.RESTServerSetup import DefaultConfig

from WMCore.WorkQueue.WorkQueue import WorkQueue, globalQueue, localQueue

from WMCore_t.WorkQueue_t.WorkQueueTestCase import WorkQueueTestCase
from WMCore_t.Services_t.WorkQueue_t.WorkQueuePopulator import createProductionSpec, getGlobalQueue
from WMCore_t.WMSpec_t.samples.BasicProductionWorkload import workload as BasicProductionWorkload
from WMCore_t.WMSpec_t.samples.Tier1ReRecoWorkload import workload as Tier1ReRecoWorkload



# in testing methods:
# this fails (actually, should not be called here) with:
# AttributeError: 'module' object has no attribute 'Database'
# self.testInit.initializeSchema(["WMCore.WorkQueue.Database"]) (mentioned on twiki)

class WorkQueueMonitorModelTest(RESTBaseUnitTest, WorkQueueTestCase):    
    def initialize(self):
        print "initialize()"
        self.config = DefaultConfig("WMCore.HTTPFrontEnd.WorkQueue.WorkQueueMonitorModel")
        
        # if module "WorkQueueMonitoringModel" needs database access, set:
        #self.schemaModules = ["WMCore.WorkQueue.Database"]
        dbUrl = os.environ["DATABASE"] or "sqlite:////tmp/resttest.db"
        self.config.setDBUrl(dbUrl)        
        self.urlbase = self.config.getServerUrl()
        # however the line self.schemaModules = ["WMCore.WorkQueue.Database"]
        # need to be commented out if populating
        # database as done in  test/python/WMCore_t/WorkQueue_t/WorkQueue_t.py
        # otherwise getting issues when creating db tables later:
        # "Table 'wq_wmspec' already exists"
        
                
    
    def setUp(self):
        print "setUp()"
        RESTBaseUnitTest.setUp(self) # calls self.initialize()         
        # TestInit stuff happens in RESTBaseUnitTest if self.schemaModules are set
      
        # this is used in Servies_t/WorkQueue_t/WorkQueue_t.py, but test fails with:
        # AttributeError: TestInit instance has no attribute 'getDBInterface'
        # self.params = {}
        # self.params['endpoint'] = self.config.getServerUrl()        
        # self.globalQueue = getGlobalQueue(dbi = self.testInit.getDBInterface(),
        #                                   CacheDir = 'global',
        #                                   NegotiationTimeout = 0,
        #                                   QueueURL = self.config.getServerUrl())
        
        
        
        # populate - as done in test/python/WMCore_t/WorkQueue_t/WorkQueue_t.py
        # create WMSpec first
        WorkQueueTestCase.setUp(self)

        # Basic production Spec
        self.spec = BasicProductionWorkload
        self.spec.setSpecUrl(os.path.join(os.getcwd(), 'testworkflow.spec'))
        self.spec.save(self.spec.specUrl())
        
        # Sample Tier1 ReReco spec
        self.processingSpec = Tier1ReRecoWorkload
        self.processingSpec.setSpecUrl(os.path.join(os.getcwd(),
                                                    'testProcessing.spec'))
        self.processingSpec.save(self.processingSpec.specUrl())
        
        
        # Create queues
        self.globalQueue = globalQueue(CacheDir = 'global',
                                       NegotiationTimeout = 0,
                                       QueueURL = 'global.example.com')
#        self.midQueue = WorkQueue(SplitByBlock = False, # mid-level queue
#                            PopulateFilesets = False,
#                            ParentQueue = self.globalQueue,
#                            CacheDir = None)
        # ignore mid queue as it causes database duplication's
        self.localQueue = localQueue(ParentQueue = self.globalQueue,
                                     CacheDir = 'local',
                                     ReportInterval = 0,
                                     QueueURL = "local.example.com")
        # standalone queue for unit tests
        self.queue = WorkQueue(CacheDir = 'standalone')
                   
    
    
    def tearDown(self):
        print "tearDown()"
        RESTBaseUnitTest.tearDown(self)
        # happens in RESTBaseUnitTest if self.schemaModules is set
        # self.testInit.clearDatabase()
        
        # clean up WorkQueues - as done in test/python/WMCore_t/WorkQueue_t/WorkQueue_t.py
        WorkQueueTestCase.tearDown(self)

        for f in (self.spec.specUrl(), self.processingSpec.specUrl()):
            os.unlink(f)
        for d in ('standalone', 'global', 'local'):
            shutil.rmtree(d, ignore_errors = True)
        

    
    def testNonExistingMethod(self):
        print "testNonExistingMethod()"
        # test not accepted type should return 406 error, in fact getting 404, then OK
        url = self.urlbase + "someWrongMethod"
        methodTest("GET", url,  accept = "text/json", output = {"code": 404})
        
        

    def testExistingMethod(self):
        print "testExistingMethod()"
        url = self.urlbase + "test"
        data, expires = methodTest("GET", url,  accept = "text/json",
                                   output = {"code": 200})
        # beware of the starting quote - HTTPConnection class seems to wrap
        # the return data this way ...
        dataPrefix =  "\"date/time:"
        errMsg = "Expect data starting with '%s', got '%s'" % (dataPrefix, data)
        assert data.startswith(dataPrefix), errMsg 
        
        
        
    def testDummy(self):
        print "testDummy()"
        assert True

             
        
if __name__ == "__main__":
    unittest.main()        
     