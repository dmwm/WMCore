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


__revision__ = "$Id: WorkQueueMonitorService_t.py,v 1.5 2010/03/09 14:06:25 maxa Exp $"
__version__ = "$Revision: 1.5 $"



import os
import shutil
import inspect
import unittest

from WMCore.Wrappers import JsonWrapper
from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMQuality.WebTools.RESTClientAPI import methodTest
from WMCore.WorkQueue.Database import States
from WMCore.WMException import WMException

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
        # to provide DAS-compatible output
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
        
        """
        the rest of this method used for populating db, workqueues
        taken from test/python/WMCore_t/WorkQueue_t/WorkQueue_t.py
        likely not compatible with latest changes, remove later (04/02)

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
        """



    def tearDown(self):
        print "tearDown()"

        RESTBaseUnitTest.tearDown(self)
        # happens in RESTBaseUnitTest if self.schemaModules is set
        # self.testInit.clearDatabase()

        """
        the rest of this method used for cleaning up workqueues, db
        taken from test/python/WMCore_t/WorkQueue_t/WorkQueue_t.py
        likely not compatible with latest changes, remove later (04/02)
        see setUp() method comment
        
        # clean up WorkQueues - as done in test/python/WMCore_t/WorkQueue_t/WorkQueue_t.py
        WorkQueueTestCase.tearDown(self)

        for f in (self.spec.specUrl(), self.processingSpec.specUrl()):
            os.unlink(f)
        for d in ('standalone', 'global', 'local'):
            shutil.rmtree(d, ignore_errors = True)
        """


    def _tester(self, testName, verb, code, partUrl, input = {}):
        print 80 * '#'
        print "test: %s" % testName

        self.globalQueue.queueWork(createProductionSpec())
        
        # when using json encoding use application/json
        contentType = "application/json"
        accept = "text/json+das"
        
        if input:
            input = JsonWrapper.dumps(input)
        
        # output is dictionary for the output matching 
        # there are four keys you can check:
        # {'code': code, 'data': data, 'type': type, 'response': response}        
        output = {"code": code, "type": accept}

        url = self.urlbase + partUrl
        data, exp = methodTest(verb, url,  accept = accept, input = input,
                               contentType = contentType, output = output)
        data = JsonWrapper.loads(data)
        print "\n\n"
        print "data: '%s'" % data
        print "expires: '%s'" % exp
        print "\n\n"
        
        return data, exp



    def _checkHTTPError(self, data):
        expected = "HTTPError"
        got = data["results"]["type"]
        self.assertEqual(got, expected, "Expected error '%s', got '%s'" % (expected, got))
    

        
    def testNonExistingUrl(self):
        testName = inspect.stack()[0][3]        
        self._tester(testName, "GET", 404, "somethingWrong")
                
        
        
    def testExistingUrl(self):
        testName = inspect.stack()[0][3]
        data, exp = self._tester(testName, "GET", 200, "test")
    
        dataPrefix =  "date/time:"
        errMsg = "Expect data starting with '%s', got '%s'" % (dataPrefix, data)
        assert data["results"].startswith(dataPrefix), errMsg         
        
        

    def testElementStatus(self):
        testName = inspect.stack()[0][3]
        data, exp = self._tester(testName, "GET", 200, "status")
        
        r = data["results"]
        self.assertEqual(len(r), 1, "Only 1 element needs to be back, got '%s'" % len(r))
        self.assertEqual(r[0]["Id"], 1, "Had 1 element, Id should be 1, got '%s'" % r[0]["Id"])
        
                

    def testElementsDAO(self):
        testName = inspect.stack()[0][3]
        data, exp = self._tester(testName, "GET", 200, "elements")
        
        r = data["results"]           
        self.assertEqual( len(r) ,  1, "Only 1 element needs to be back, got '%s'" % len(r) )
        self.assertEqual( data["request_method"] ,  "GET", "'request_method' not matching" )
        assert data["request_call"] == "dasjson", "'request_call' not matching" 

        
        
    def testElementsByStateDAO(self):
        testName = inspect.stack()[0][3]
        input = {"status": "Available"}
        data, exp = self._tester(testName, "POST", 200, "elementsbystate", input = input)
        
        statusInt = data["results"][0]["status"]
        statusStr = States[statusInt]
        assert input["status"] == statusStr, ("Expecting element status '%s', got "
            "'%s'" % (input["status"], statusStr))



    def testElementsNonExistingByStateDAO(self):
        testName = inspect.stack()[0][3]
        input = {"status": "Failed"}
        data, exp = self._tester(testName, "POST", 200, "elementsbystate", input = input)
        
        r = data["results"]
        assert len(r) == 0, ("Expecting empty result set, no elements with "
                             "status '%s'") % input["status"]
        
                         

    def testElementsByStateIntegerDAO(self):
        testName = inspect.stack()[0][3]
        input = {"status": 4} # states by integers - not supported
        # call is expected to fail with code 400 and HTTPError
        data, exp = self._tester(testName, "POST", 400, "elementsbystate", input = input)
        self._checkHTTPError(data)
        
        input = {"status": "4"} # states by integers - not supported
        data, exp = self._tester(testName, "POST", 400, "elementsbystate", input = input)
        self._checkHTTPError(data)

        input = {"status": 456} # states by integers - not supported
        data, exp = self._tester(testName, "POST", 400, "elementsbystate", input = input)
        self._checkHTTPError(data)

        

    def testElementsByStateWrongStateDAO(self):
        testName = inspect.stack()[0][3]
        # test that the exception is raise on wrong input, error will be raised
        input = {"status": "nonsensestatus"}        
        data, exp = self._tester(testName, "POST", 400, "elementsbystate", input = input)
        self._checkHTTPError(data)
        
        

    def testElementsByIdIntegerDAO(self):
        testName = inspect.stack()[0][3]
        input = {"id": 1}
        data, exp = self._tester(testName, "POST", 200, "elementsbyid", input = input)
                
        r = data["results"]
        self.assertEqual( len(r) ,  1, "Only 1 element needs to be back, got '%s'" % len(r) )
        # now could safely assume only one item in the list
        self.assertEqual( r[0]["id"] ,  1, "Returned element should have id 1, got %s" % r[0]["id"] )
        
        
        
    def testElementsByIdStringDAO(self):
        testName = inspect.stack()[0][3]
        input = {"id": "1"}
        data, exp = self._tester(testName, "POST", 200, "elementsbyid", input = input)
                
        r = data["results"]
        self.assertEqual( len(r) ,  1, "Only 1 element needs to be back, got '%s'" % len(r) )
        # now could safely assume only one item in the list
        self.assertEqual( r[0]["id"] ,  1, "Returned element should have id 1, got %s" % r[0]["id"] )
        
        
        
    def testElementsByNonExistingIdDAO(self):
        testName = inspect.stack()[0][3]
        input = {"id": 100000}
        data, exp = self._tester(testName, "POST", 200, "elementsbyid", input = input)
        
        r = data["results"]
        self.assertEqual( len(r) ,  0, "Expected empty result (0 items), got '%s'" % len(r) )
        


    def testElementsByIncorrectIntegerIdDAO(self):
        testName = inspect.stack()[0][3]        
        input = {"id": -10}
        data, exp = self._tester(testName, "POST", 400, "elementsbyid", input = input)
        
        self._checkHTTPError(data)


    def testElementsByIncorrectStringIdDAO(self):
        testName = inspect.stack()[0][3]
        input = {"id": "nonsenseelementid"}
        data, exp = self._tester(testName, "POST", 400, "elementsbyid", input = input)
        
        self._checkHTTPError(data)
        
    
        
if __name__ == "__main__":
    unittest.main()
     