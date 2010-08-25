"""
WMCore_t/HTTPFrontEnd_t/WorkQueue_t/WorkQueueMonitorService_t.py

unittest for
WMCore/HTTPFrontEnd/WorkQueue/Services/WorkQueueMonitorService.py

writing unittests / testing details:
https://twiki.cern.ch/twiki/bin/view/CMS/RESTModelUnitTest - doesn't work (26/01)

guiding / reference classes:
test/python/WMCore_t/Services_t/WorkQueue_t/WorkQueue_t.py - fails (26/01)
test/python/WMCore_t/WorkQueue_t/WorkQueue_t.py (use use WMCore_t.WMSpec_t.samples.*) - works
"""


__revision__ = "$Id: WorkQueueMonitorService_t.py,v 1.1 2010/02/03 14:16:55 maxa Exp $"
__version__ = "$Revision: 1.1 $"



import os
import shutil
import unittest

from WMCore.Wrappers import JsonWrapper
from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMQuality.WebTools.RESTClientAPI import methodTest

from WMQuality.WebTools.RESTServerSetup import DefaultConfig

from WMCore.WorkQueue.WorkQueue import WorkQueue, globalQueue, localQueue

from WMCore_t.WorkQueue_t.WorkQueueTestCase import WorkQueueTestCase
from WMCore_t.Services_t.WorkQueue_t.WorkQueuePopulator import createProductionSpec, getGlobalQueue, createProcessingSpec
from WMCore_t.WMSpec_t.samples.BasicProductionWorkload import workload as BasicProductionWorkload
from WMCore_t.WMSpec_t.samples.Tier1ReRecoWorkload import workload as Tier1ReRecoWorkload



class WorkQueueMonitorServiceTest(RESTBaseUnitTest):    
    def initialize(self):
        print "initialize()"
        
        self.config = DefaultConfig("WMCore.HTTPFrontEnd.WorkQueue.WorkQueueRESTModel")
        # TODO
        # will this be enough to provide DAS-compatible output?
        self.config.setFormatter("WMCore.WebTools.DASRESTFormatter")

        # set database
        dbUrl = os.environ["DATABASE"] or "sqlite:////tmp/resttest.db"
        self.config.setDBUrl(dbUrl)        
        self.urlbase = self.config.getServerUrl()
        
        self.schemaModules = ["WMCore.WorkQueue.Database"]
        wqConfig = self.config.getModelConfig()
        wqConfig.queueParams = {}
        wqConfig.serviceModules = ["WMCore.HTTPFrontEnd.WorkQueue.Services.WorkQueueMonitorService"]
                
                

    def setUp(self):
        print "setUp()"
        RESTBaseUnitTest.setUp(self) # calls self.initialize()         
        # TestInit stuff happens in RESTBaseUnitTest if self.schemaModules are set
      
        self.params = {}
        self.params['endpoint'] = self.config.getServerUrl()        
        self.globalQueue = getGlobalQueue(dbi = self.testInit.getDBInterface(),
                                          CacheDir = 'global',
                                          NegotiationTimeout = 0,
                                          QueueURL = self.config.getServerUrl())
        
        
        # the rest of this method used for populating db, workqueues
        # is probably no longer necessary - remove later (is likely not compatible with
        # current set up / initialize stuff anyway)
        return

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

        # the rest of this method used for populating db, workqueues
        # is probably no longer necessary - remove later (is likely not compatible with
        # current set up / initialize stuff anyway)
        # see setUp() method comment
        return
        
        # clean up WorkQueues - as done in test/python/WMCore_t/WorkQueue_t/WorkQueue_t.py
        WorkQueueTestCase.tearDown(self)

        for f in (self.spec.specUrl(), self.processingSpec.specUrl()):
            os.unlink(f)
        for d in ('standalone', 'global', 'local'):
            shutil.rmtree(d, ignore_errors = True)




        
    # starting with X to prevent from running for now 
    def XtestNonExistingMethod(self):
        print "testNonExistingMethod()"
        # test not accepted type should return 406 error, in fact getting 404, then OK
        url = self.urlbase + "someWrongMethod"
        methodTest("GET", url,  accept = "text/json", output = {"code": 404})
        
        

    # starting with X to prevent from running for now
    def XtestExistingMethod(self):
        print "testExistingMethod()"
        url = self.urlbase + "test"
    
        #output is dictionary for the output matching 
        # there are four keys you can check:
        # {'code': code, 'data': data, 'type': type, 'response': response}
        data, expires = methodTest("GET", url,  accept = "text/json",
                                   output = {"code": 200})
        # beware of the starting quote - HTTPConnection class seems to wrap
        # the return data this way ...
        dataPrefix =  "\"date/time:"
        errMsg = "Expect data starting with '%s', got '%s'" % (dataPrefix, data)
        assert data.startswith(dataPrefix), errMsg         
        


                
    # return number of WorkQueue elements, perhaps with IDs?
    # obvious thing - to monitor status of WorkQueue elements identified by IDs
    # DAS testing - see RESTServerSetup, model class has to be DASFormatter (or just
    # specified in configuration)
    # DAO stuff
    
    

    def XtestElementStatus(self):
        print "testElementStatus()"
        
        self.globalQueue.queueWork(createProductionSpec())
        
        verb = "POST"
        url = self.urlbase + "status"
        input = None
        contentType = "application/json"
        output = {"code": 200, "type": "text/json"} # gets already tested in methodTest
        # no input specified
        data, expires = methodTest(verb, url, contentType = contentType, output = output)
        
        print "\n\n"
        print "data: '%s'" % data
        print "expires: '%s'" % expires
        print "\n\n"
        
        data = JsonWrapper.loads(data)
        
        assert len(data) == 1, "Only 1 element needs to be back, got '%s'" % len(data)
        id = data[0]["Id"]
        assert id == 1, "Had 1 element, Id should be 1, got '%s'" % id  
        
        

    
    def testElementStatusAvailable(self):
        print "testElementStatusAvailable()"
        
        self.globalQueue.queueWork(createProductionSpec())

        verb = "POST"
        url = self.urlbase + "status"
        input = {"status": "Available"}
        input = JsonWrapper.dumps(input)
        contentType = "application/json"
        output = {"code": 200, "type": "text/json"} # gets already tested in methodTest
        
        data, expires = methodTest(verb, url, input = input, contentType = contentType, output = output)
        
        data = JsonWrapper.loads(data)
        
        print "\n\n"
        print "data: '%s'" % data
        print "expires: '%s'" % expires
        print "\n\n"
        
        status = data[0]["Status"]
        assert status == "Available", "Had 'Available' element but status is '%s'" % status  
        

                     
        
if __name__ == "__main__":
    unittest.main()
     