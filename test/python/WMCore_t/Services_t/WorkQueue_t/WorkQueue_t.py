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
from WMQuality.TestInitCouchApp import TestInitCouchApp

class WorkQueueTest(unittest.TestCase):
    """
    Test WorkQueue Service client
    It will start WorkQueue RESTService
    Server DB sets from environment variable.
    Client DB sets from environment variable.

    This checks whether DS call makes without error and return the results.
    Not the correctness of functions. That will be tested in different module.
    """
    def setUp(self):
        """
        _setUp_
        """
        EmulatorHelper.setEmulators(phedex = True, dbs = True,
                                    siteDB = True, requestMgr = True)

        self.specGenerator = WMSpecGenerator("WMSpecs")
        #self.configFile = EmulatorSetup.setupWMAgentConfig()
        self.schema = []
        self.couchApps = ["WorkQueue"]
        self.testInit = TestInitCouchApp('WorkQueueServiceTest')
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = self.schema,
                                useDefault = False)
        self.testInit.setupCouch('workqueue_t', *self.couchApps)
        self.testInit.setupCouch('workqueue_t_inbox', *self.couchApps)
        return

    def tearDown(self):
        """
        _tearDown_

        Drop all the WMBS tables.
        """
        self.testInit.tearDownCouch()
        #EmulatorSetup.deleteConfig(self.configFile)
        EmulatorHelper.resetEmulators()
        self.specGenerator.removeSpecs()


    def testWorkQueueService(self):
        # test getWork
        specName = "RerecoSpec"
        specUrl = self.specGenerator.createReRecoSpec(specName, "file")
        globalQ = globalQueue(DbName = 'workqueue_t',
                              QueueURL = self.testInit.couchUrl)
        self.assertTrue(globalQ.queueWork(specUrl, "RerecoSpec", "teamA") > 0)

        wqApi = WorkQueueDS(self.testInit.couchUrl, 'workqueue_t')
        #This only checks minimum client call not exactly correctness of return
        # values.
        self.assertEqual(wqApi.getTopLevelJobsByRequest(),
                         [{'total_jobs': 2, 'request_name': specName}])
        self.assertEqual(wqApi.getChildQueues(), [])
        self.assertEqual(wqApi.getJobStatusByRequest(),
            [{'status': 'Available', 'jobs': 2, 'request_name': specName}])
        self.assertEqual(wqApi.getChildQueuesByRequest(), [])
        self.assertEqual(wqApi.getWMBSUrl(), [])
        self.assertEqual(wqApi.getWMBSUrlByRequest(), [])


if __name__ == '__main__':

    unittest.main()
