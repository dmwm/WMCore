"""
Unittest file for WMCore/HTTPFrontEnd/WorkQueue/Services/WorkQueueService.py

"""

__revision__ = "$Id"
__version__ = "$Revision: 1.7 $"


#setup emulator for test, this needs to be at top of the file
from WMQuality.Emulators.EmulatorSetup import emulatorSetup, deleteConfig
ConfigFile = emulatorSetup(phedex=True, dbs=True, siteDB=True, requestMgr=True)

import os
import inspect
import unittest
try:
    # Python 2.6
    import json
except:
    # Prior to 2.6 requires simplejson
    import simplejson as json
    
from cherrypy import HTTPError


from WMCore.Wrappers import JsonWrapper
from WMCore.WorkQueue.WorkQueue import globalQueue
#decorator import for RESTServer setup
from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMQuality.WebTools.RESTServerSetup import DefaultConfig
from WMQuality.WebTools.RESTClientAPI import methodTest
from WMQuality.Emulators.WMSpecGenerator.WMSpecGenerator import WMSpecGenerator

from WMCore.Wrappers import JsonWrapper


class WorkQueueServiceTest(RESTBaseUnitTest):
    """
    Test WorkQueue Service client
    It will start WorkQueue RESTService
    Server DB is SQlite.
    Client DB sets from environment variable. 
    """
    def initialize(self):
        self.config = DefaultConfig('WMCore.HTTPFrontEnd.WorkQueue.WorkQueueRESTModel')
        # set up database
        self.config.setDBUrl("sqlite:////tmp/resttest.db")
                
        # mysql example
        #self.config.setDBUrl('mysql://username@host.fnal.gov:3306/TestDB')
        self.urlbase = self.config.getServerUrl()
                
        self.schemaModules = ["WMCore.WorkQueue.Database"]
        wqConfig = self.config.getModelConfig()
        wqConfig.queueParams = {'PopulateFilesets' : False}
        wqConfig.serviceModules = ['WMCore.HTTPFrontEnd.WorkQueue.Services.WorkQueueService']


        
    def setUp(self):
        """
        setUP global values
        """
        RESTBaseUnitTest.setUp(self)
        self.params = {}
        self.params['endpoint'] = self.config.getServerUrl()
        
        #cache location is set under current directory - Global
        self.globalQueue = globalQueue(dbi = self.testInit.getDBInterface(),
                                          CacheDir = 'Global',
                                          NegotiationTimeout = 0,
                                          QueueURL = self.config.getServerUrl())
        # original location of wmspec: under current directory - WMSpecs
        self.specGenerator = WMSpecGenerator("WMSpecs")
        
    def tearDown(self):
        RESTBaseUnitTest.tearDown(self)
        self.specGenerator.removeSpecs()
     
    def _tester(self, testName, verb, code, partUrl, inpt = {}, contentType = "application/json"):
        print 80 * '#'
        print "test: %s" % testName

        contentType = contentType
        accept = "text/json"
        
        if inpt and contentType == "application/json":
            inpt = JsonWrapper.dumps(inpt)
        
        # output is dictionary for the output matching 
        # there are four keys you can check:
        # {'code': code, 'data': data, 'type': type, 'response': response}        
        output = {"code": code, "type": accept}

        url = self.urlbase + partUrl
        print "input: %s" % inpt
        data, exp = methodTest(verb, url,  accept = accept, input = inpt,
                               contentType = contentType, output = output)
        data = JsonWrapper.loads(data)
        #print "\n\n"
        #print "data: '%s'" % data
        #print "expires: '%s'" % exp
        #print "\n\n"
        
        return data, exp
        

        
    def testGetWork(self):
        #TODO: could try different spec or multiple spec
        specName = "ProductionSpec1"
        specUrl = self.specGenerator.createProductionSpec(specName, "file")
        self.globalQueue.queueWork(specUrl)
        testName = inspect.stack()[0][3]
        inpt = {'siteJobs':{'SiteB' : 10, 'SiteA' : 100}, 
                 "pullingQueueUrl": "http://test.url"}        
        data, exp = self._tester(testName, "POST", 200, "getwork/", inpt)
        self.assertEqual(len(data), 1, "only 1 element needs to be back, got %s" % len(data))
        self.assertEqual(data[0]["wmspec_name"], specName,
                         "spec name is not BasicProduction: %s" % data[0]['wmspec_name'])
         


    def testStatus(self):
        #TODO: could try different spec or multiple spec
        specUrl = self.specGenerator.createProductionSpec("ProductionSpec1", "file")
        self.globalQueue.queueWork(specUrl)
        specUrl = self.specGenerator.createProductionSpec("ProductionSpec2", "file")
        self.globalQueue.queueWork(specUrl)
        
        testName = inspect.stack()[0][3]
        
        inpt =  [("elementIDs",  1),("elementIDs", 2)]
        data, exp = self._tester(testName, "GET", 200, "status/", inpt, contentType = "text/plain")

        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]["Id"], 1, "expected Id 1, got %s" % data[0]["Id"])

                
        
    def testServeWorkflow(self):
        #TODO: could try different spec or multiple spec
        specUrl = self.specGenerator.createProductionSpec("ProductionSpec1", "file")
        self.globalQueue.queueWork(specUrl)
        
        testName = inspect.stack()[0][3]
        contentType = "text/plain"
        accept = "text/json"
        inpt = { "name" : "some-non-existing-name" }
        # raises HTTPError in the cherrypy application
        output = {"code": 404, "type": accept}

        url = self.urlbase + 'wf/'
        print "input: %s" % inpt
        data, exp = methodTest('GET', url,  accept = accept, input = inpt,
                               contentType = contentType, output = output)
        
        # no workflow appear to the in the directory, don't know what else to test
                
        

if __name__ == '__main__':
    unittest.main()
    deleteConfig(ConfigFile)