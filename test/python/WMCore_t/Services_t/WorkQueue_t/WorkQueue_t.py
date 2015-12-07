#!/usr/bin/env python
import unittest

from WMCore.WorkQueue.WorkQueue import globalQueue
from WMCore.WorkQueue.WorkQueue import localQueue
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue as WorkQueueDS

from WMQuality.Emulators.WMSpecGenerator.WMSpecGenerator import WMSpecGenerator
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
        self.testInit.setupCouch('local_workqueue_t', *self.couchApps)
        self.testInit.setupCouch('local_workqueue_t_inbox', *self.couchApps)
        self.testInit.generateWorkDir()
        return

    def tearDown(self):
        """
        _tearDown_

        Drop all the WMBS tables.
        """
        self.testInit.tearDownCouch()
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
        #overwrite default - can't test with stale view
        wqApi.defaultOptions =  {'reduce' : True, 'group' : True}
        #This only checks minimum client call not exactly correctness of return
        # values.
        self.assertEqual(wqApi.getTopLevelJobsByRequest(),
                         [{'total_jobs': 10, 'request_name': specName}])
        self.assertEqual(wqApi.getChildQueues(), [])
        self.assertEqual(wqApi.getJobStatusByRequest(),
            [{'status': 'Available', 'jobs': 10, 'request_name': specName}])
        self.assertEqual(wqApi.getChildQueuesByRequest(), [])
        self.assertEqual(wqApi.getWMBSUrl(), [])
        self.assertEqual(wqApi.getWMBSUrlByRequest(), [])

    def testUpdatePriorityService(self):
        """
        _testUpdatePriorityService_

        Check that we can update the priority correctly also
        check the available workflows feature
        """
        specName = "RerecoSpec"
        specUrl = self.specGenerator.createReRecoSpec(specName, "file")
        globalQ = globalQueue(DbName = 'workqueue_t',
                              QueueURL = self.testInit.couchUrl)
        localQ = localQueue(DbName = 'local_workqueue_t',
                            QueueURL = self.testInit.couchUrl,
                            CacheDir = self.testInit.testDir,
                            ParentQueueCouchUrl = '%s/workqueue_t' % self.testInit.couchUrl,
                            ParentQueueInboxCouchDBName = 'workqueue_t_inbox'
                            )
        # Try a full chain of priority update and propagation
        self.assertTrue(globalQ.queueWork(specUrl, "RerecoSpec", "teamA") > 0)
        globalApi = WorkQueueDS(self.testInit.couchUrl, 'workqueue_t')
        #overwrite default - can't test with stale view
        globalApi.defaultOptions =  {'reduce' : True, 'group' : True}
        globalApi.updatePriority(specName, 100)
        self.assertEqual(globalQ.backend.getWMSpec(specName).priority(), 100)
        storedElements = globalQ.backend.getElementsForWorkflow(specName)
        for element in storedElements:
            self.assertEqual(element['Priority'], 100)
        self.assertTrue(localQ.pullWork({'T2_XX_SiteA' : 10}) > 0)
        localQ.processInboundWork(continuous = False)
        storedElements = localQ.backend.getElementsForWorkflow(specName)
        for element in storedElements:
            self.assertEqual(element['Priority'], 100)
        localApi = WorkQueueDS(self.testInit.couchUrl, 'local_workqueue_t')
        #overwrite default - can't test with stale view
        localApi.defaultOptions =  {'reduce' : True, 'group' : True}
        localApi.updatePriority(specName, 500)
        self.assertEqual(localQ.backend.getWMSpec(specName).priority(), 500)
        storedElements = localQ.backend.getElementsForWorkflow(specName)
        for element in storedElements:
            self.assertEqual(element['Priority'], 500)
        self.assertEqual(localApi.getAvailableWorkflows(), set([(specName, 500)]))
        # Attempt to update an inexistent workflow in the queue
        try:
            globalApi.updatePriority('NotExistent', 2)
        except:
            self.fail('No exception should be raised.')

if __name__ == '__main__':

    unittest.main()
