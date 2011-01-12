#!/usr/bin/env python
import os
import unittest
import shutil

from WMCore.Wrappers import JsonWrapper
from WMCore.WorkQueue.WorkQueue import globalQueue
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue as WorkQueueDS

#decorator import for RESTServer setup
from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMQuality.WebTools.RESTServerSetup import DefaultConfig

from WMQuality.Emulators.WMSpecGenerator.WMSpecGenerator import WMSpecGenerator
from WMQuality.Emulators import EmulatorSetup
from WMCore.Services.EmulatorSwitch import EmulatorHelper

class WorkQueueTest(RESTBaseUnitTest):
    """
    Test WorkQueue Service client
    It will start WorkQueue RESTService
    Server DB sets from environment variable.
    Client DB sets from environment variable. 

    This checks whether DS call makes without error and return the results.
    Not the correctness of functions. That will be tested in different module.
    """
    def initialize(self):
        
        self.config = DefaultConfig(
                'WMCore.HTTPFrontEnd.WorkQueue.WorkQueueRESTModel')
        dbUrl = os.environ.get("DATABASE", None)
        self.config.setDBUrl(dbUrl)        
        self.config.setFormatter(
             'WMCore.HTTPFrontEnd.WorkQueue.WorkQueueRESTFormatter')
        self.config.setWorkQueueLevel("GlobalQueue")
        # mysql example
        #self.config.setDBUrl('mysql://username@host.fnal.gov:3306/TestDB')
        #self.config.setDBSocket('/var/lib/mysql/mysql.sock')
        self.schemaModules = ["WMCore.WorkQueue.Database"]
        wqConfig = self.config.getModelConfig()
        wqConfig.queueParams = {}
        wqConfig.serviceModules = [
            'WMCore.HTTPFrontEnd.WorkQueue.Services.WorkQueueService',
            'WMCore.HTTPFrontEnd.WorkQueue.Services.WorkQueueMonitorService']
        
    def setUp(self):
        """
        setUP global values
        """
        EmulatorHelper.setEmulators(phedex = True, dbs = True, 
                                    siteDB = True, requestMgr = True)
        
        RESTBaseUnitTest.setUp(self)
        self.params = {}
        self.params['endpoint'] = self.config.getServerUrl()
        
        # original location of wmspec: under current directory - WMSpecs
        self.specGenerator = WMSpecGenerator("WMSpecs")
        self.configFile = EmulatorSetup.setupWMAgentConfig()
        os.environ["COUCHDB"] = "workqueue_t"
        self.testInit.setupCouch("workqueue_t", "JobDump")
        
    def tearDown(self):
        RESTBaseUnitTest.tearDown(self)
        EmulatorSetup.deleteConfig(self.configFile)        
        self.testInit.tearDownCouch()
        self.specGenerator.removeSpecs()
        
        # following should be tearDownClass if we swithch to python 2.7
        try:
            # clean up files created by cherrypy.
            shutil.rmtree('wf')
        except:
            pass
        EmulatorHelper.resetEmulators()

    def testWorkQueueService(self):
        self.config.setWorkQueueLevel("GlobalQueue")
        # test getWork
        specName = "RerecoSpec"
        specUrl = self.specGenerator.createReRecoSpec(specName, "file")

        wqApi = WorkQueueDS(self.params)

        self.assertTrue(wqApi.queueWork(specUrl, "teamA", "RerecoSpec") > 0)

        data = wqApi.getWork({'SiteA' : 1000000}, "http://test.url")

        # testStatusChange
        self.assertTrue(len(wqApi.status()) > 1)
        self.assertEqual(wqApi.cancelWork([1]), [1])
        self.assertEqual(wqApi.doneWork([1]), [1])
        self.assertEqual(wqApi.failWork([1]), [1])

        # testSynchronize
        childUrl = "http://test.url"
        childResources = [{'ParentQueueId' : 1, 'Status' : 'Available'}]
        self.assertEqual(wqApi.synchronize(childUrl, childResources),
                         {'Canceled': set([1])})

        # testCancelWorkWithRequest
        self.assertEqual(wqApi.cancelWork(["RerecoSpec"], "request_name"),
                                          ["RerecoSpec"])
        
        # testGetChildQueues
        self.assertTrue(wqApi.getChildQueues() > 0)
        
        # testGetChildQueuesByRequest
        self.assertTrue(wqApi.getChildQueuesByRequest() > 0)

        #TODO: this needs to be tested in separate localQueue service setting.
        # testGetJobSummaryFromCouchDB
        #self.assertTrue(wqApi.getJobSummaryFromCouchDB() > 0)


if __name__ == '__main__':

    unittest.main()
    
