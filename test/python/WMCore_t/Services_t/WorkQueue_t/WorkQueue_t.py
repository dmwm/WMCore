#!/usr/bin/env python
from __future__ import print_function, division
import unittest
import time

from Utils.PythonVersion import PY3

from WMCore.WorkQueue.WorkQueue import globalQueue
from WMCore.WorkQueue.WorkQueue import localQueue
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue as WorkQueueDS
from WMCore.Services.WorkQueue.WorkQueue import convertWQElementsStatusToWFStatus
from WMQuality.Emulators.WMSpecGenerator.WMSpecGenerator import WMSpecGenerator
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMQuality.TestInitCouchApp import TestInitCouchApp

class WorkQueueTest(EmulatedUnitTestCase):
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
        super(WorkQueueTest, self).setUp()

        self.specGenerator = WMSpecGenerator("WMSpecs")
        # self.configFile = EmulatorSetup.setupWMAgentConfig()
        self.schema = []
        self.couchApps = ["WorkQueue"]
        self.testInit = TestInitCouchApp('WorkQueueServiceTest')
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules=self.schema,
                                useDefault=False)
        self.testInit.setupCouch('workqueue_t', *self.couchApps)
        self.testInit.setupCouch('workqueue_t_inbox', *self.couchApps)
        self.testInit.setupCouch('local_workqueue_t', *self.couchApps)
        self.testInit.setupCouch('local_workqueue_t_inbox', *self.couchApps)
        self.testInit.generateWorkDir()

        # setup rucio parameters for global/local queue
        self.queueParams = {}
        self.queueParams['log_reporter'] = "Services_WorkQueue_Unittest"
        self.queueParams['rucioAccount'] = "wma_test"
        self.queueParams['rucioAuthUrl'] = "http://cms-rucio-int.cern.ch"
        self.queueParams['rucioUrl'] = "https://cms-rucio-auth-int.cern.ch"

        if PY3:
            self.assertItemsEqual = self.assertCountEqual

    def tearDown(self):
        """
        _tearDown_

        Drop all the WMBS tables.
        """
        self.testInit.tearDownCouch()
        self.specGenerator.removeSpecs()
        super(WorkQueueTest, self).tearDown()

    def testWorkQueueService(self):
        # test getWork
        specName = "RerecoSpec"
        specUrl = self.specGenerator.createReRecoSpec(specName, "file",
                                                      assignKwargs={'SiteWhitelist': ['T2_XX_SiteA']})
        globalQ = globalQueue(DbName='workqueue_t',
                              QueueURL=self.testInit.couchUrl,
                              UnittestFlag=True,
                              **self.queueParams)
        self.assertTrue(globalQ.queueWork(specUrl, specName, "teamA") > 0)

        wqApi = WorkQueueDS(self.testInit.couchUrl, 'workqueue_t')
        # overwrite default - can't test with stale view
        wqApi.defaultOptions = {'reduce': True, 'group': True}
        # This only checks minimum client call not exactly correctness of return
        # values.
        self.assertEqual(wqApi.getTopLevelJobsByRequest(),
                         [{'total_jobs': 339, 'request_name': specName}])
        # work still available, so no childQueue
        results = wqApi.getChildQueuesAndStatus()
        self.assertItemsEqual(set([item['agent_name'] for item in results]), ["AgentNotDefined"])
        result = wqApi.getElementsCountAndJobsByWorkflow()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[specName]['Available']['Jobs'], 339)

        results = wqApi.getChildQueuesAndPriority()
        resultsPrio = set([item['priority'] for item in results if item['agent_name'] == "AgentNotDefined"])
        self.assertItemsEqual(resultsPrio, [8000])
        self.assertEqual(wqApi.getWMBSUrl(), [])
        self.assertEqual(wqApi.getWMBSUrlByRequest(), [])

    def testUpdatePriorityService(self):
        """
        _testUpdatePriorityService_

        Check that we can update the priority correctly also
        check the available workflows feature
        """
        specName = "RerecoSpec"
        specUrl = self.specGenerator.createReRecoSpec(specName, "file",
                                                      assignKwargs={'SiteWhitelist':["T2_XX_SiteA"]})
        globalQ = globalQueue(DbName='workqueue_t',
                              QueueURL=self.testInit.couchUrl,
                              UnittestFlag=True,
                              **self.queueParams)
        localQ = localQueue(DbName='local_workqueue_t',
                            QueueURL=self.testInit.couchUrl,
                            CacheDir=self.testInit.testDir,
                            ParentQueueCouchUrl='%s/workqueue_t' % self.testInit.couchUrl,
                            ParentQueueInboxCouchDBName='workqueue_t_inbox',
                            **self.queueParams)
        # Try a full chain of priority update and propagation
        self.assertTrue(globalQ.queueWork(specUrl, "RerecoSpec", "teamA") > 0)
        globalApi = WorkQueueDS(self.testInit.couchUrl, 'workqueue_t')
        # overwrite default - can't test with stale view
        globalApi.defaultOptions = {'reduce': True, 'group': True}
        globalApi.updatePriority(specName, 100)
        self.assertEqual(globalQ.backend.getWMSpec(specName).priority(), 100)
        storedElements = globalQ.backend.getElementsForWorkflow(specName)
        for element in storedElements:
            self.assertEqual(element['Priority'], 100)
        numWorks = localQ.pullWork({'T2_XX_SiteA': 10})
        self.assertTrue(numWorks > 0)
        # replicate from GQ to LQ manually
        localQ.backend.pullFromParent(continuous=False)
        # wait until replication is done
        time.sleep(2)

        localQ.processInboundWork(continuous=False)
        storedElements = localQ.backend.getElementsForWorkflow(specName)
        for element in storedElements:
            self.assertEqual(element['Priority'], 100)
        localApi = WorkQueueDS(self.testInit.couchUrl, 'local_workqueue_t')
        # overwrite default - can't test with stale view
        localApi.defaultOptions = {'reduce': True, 'group': True}
        localApi.updatePriority(specName, 500)
        self.assertEqual(localQ.backend.getWMSpec(specName).priority(), 500)
        storedElements = localQ.backend.getElementsForWorkflow(specName)
        for element in storedElements:
            self.assertEqual(element['Priority'], 500)
        availableWF = localApi.getAvailableWorkflows()
        self.assertEqual(availableWF, set([(specName, 500)]))
        # Attempt to update an inexistent workflow in the queue
        try:
            globalApi.updatePriority('NotExistent', 2)
        except Exception as ex:
            self.fail('No exception should be raised.: %s' % str(ex))

    def testCompletedWorkflow(self):
        # test getWork
        specName = "RerecoSpec"
        specUrl = self.specGenerator.createReRecoSpec(specName, "file",
                                                      assignKwargs={'SiteWhitelist':['T2_XX_SiteA']})

        globalQ = globalQueue(DbName='workqueue_t',
                              QueueURL=self.testInit.couchUrl,
                              UnittestFlag=True,
                              **self.queueParams)
        self.assertTrue(globalQ.queueWork(specUrl, specName, "teamA") > 0)

        wqApi = WorkQueueDS(self.testInit.couchUrl, 'workqueue_t')
        # overwrite default - can't test with stale view
        wqApi.defaultOptions = {'reduce': True, 'group': True}
        # This only checks minimum client call not exactly correctness of return
        # values.
        self.assertEqual(wqApi.getTopLevelJobsByRequest(),
                         [{'total_jobs': 339, 'request_name': specName}])

        results = wqApi.getJobsByStatus()
        self.assertEqual(results['Available']['sum_jobs'], 339)
        results = wqApi.getJobsByStatusAndPriority()
        resultsPrio = set([item['priority'] for item in results.get('Available')])
        self.assertItemsEqual(resultsPrio, [8000])
        resultsJobs = sum([item['sum_jobs'] for item in results.get('Available') if item['priority'] == 8000])
        self.assertTrue(resultsJobs, 339)
        result = wqApi.getElementsCountAndJobsByWorkflow()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[specName]['Available']['Jobs'], 339)
        data = wqApi.db.loadView('WorkQueue', 'elementsDetailByWorkflowAndStatus',
                                 {'startkey': [specName], 'endkey': [specName, {}],
                                  'reduce': False})
        elements = [x['id'] for x in data.get('rows', [])]
        wqApi.updateElements(*elements, Status='Canceled')
        # load this view once again to make sure it will be updated in the next assert..
        data = wqApi.db.loadView('WorkQueue', 'elementsDetailByWorkflowAndStatus',
                                 {'startkey': [specName], 'endkey': [specName, {}],
                                  'reduce': False})
        self.assertEqual(len(wqApi.getCompletedWorkflow(stale=False)), 1)
        results = wqApi.getJobsByStatusAndPriority()
        resultsPrio = set([item['priority'] for item in results.get('Canceled')])
        self.assertItemsEqual(resultsPrio, [8000])

    def testConvertWQElementsStatusToWFStatus(self):
        """
        _testConvertWQElementsStatusToWFStatus_

        Check that a set of all the workqueue element status in a request
        properly maps to a single state to trigger the ReqMgr request transition.
        """
        # workflows acquired by global_workqueue (nothing acquired by agents so far)
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Available"])), "acquired")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Negotiating"])), "acquired")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Available", "Negotiating"])), "acquired")

        # workflows not completely acquired yet by the agents
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Acquired"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Available", "Negotiating", "Acquired"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Available", "Negotiating", "Acquired", "Running"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Available", "Negotiating", "Acquired", "Running", "Done"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Negotiating", "Acquired"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Negotiating", "Acquired", "Running"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Negotiating", "Acquired", "Running", "Done"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Acquired", "Running"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Acquired", "Running", "Done"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Available", "Done"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Available", "Running", "Done", "Canceled"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Acquired", "Done"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Acquired", "Running", "Done", "Canceled"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Available", "Acquired", "Running", "Done", "Canceled"])), "running-open")

        # workflows completely acquired by the agents
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Running"])), "running-closed")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Running", "Done"])), "running-closed")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Running", "Done", "Canceled"])), "running-closed")

        # workflows completed/aborted/force-completed, thus existent elements
        # but no more active workqueue elements in the system
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Done"])), "completed")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Canceled"])), "completed")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Done", "Canceled"])), "completed")

        # workflows failed
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Failed"])), "failed")

        # non-failed workflows but with Failed elements
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Available", "Negotiating", "Acquired", "Failed"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Available", "Negotiating", "Acquired", "Running", "Done", "Failed"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Negotiating", "Acquired", "Running", "Done", "Canceled", "Failed"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Running", "Failed"])), "running-closed")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Running", "Done", "Canceled", "Failed"])), "running-closed")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Done", "Failed"])), "completed")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Canceled", "Failed"])), "completed")

        # workflows that have been aborted but still have workqueue elements around
        self.assertEqual("running-open", convertWQElementsStatusToWFStatus(
                set(["Available", "Negotiating", "Acquired", "Running", "Done", "CancelRequested"])))
        self.assertEqual("running-open", convertWQElementsStatusToWFStatus(
                set(["Available", "Negotiating", "Acquired", "Running", "Done", "CancelRequested", "Canceled"])))
        self.assertEqual("running-open", convertWQElementsStatusToWFStatus(
                set(["Negotiating", "Acquired", "Running", "Done", "CancelRequested"])))
        self.assertEqual("running-open", convertWQElementsStatusToWFStatus(
                set(["Negotiating", "Acquired", "Running", "Done", "CancelRequested", "Canceled"])))
        self.assertEqual("running-open", convertWQElementsStatusToWFStatus(
                set(["Acquired", "Running", "Done", "CancelRequested"])))
        self.assertEqual("running-open", convertWQElementsStatusToWFStatus(
                set(["Acquired", "Running", "Done", "CancelRequested", "Canceled"])))
        self.assertEqual("running-closed", convertWQElementsStatusToWFStatus(
                set(["Running", "Done", "CancelRequested"])))
        self.assertEqual("running-closed", convertWQElementsStatusToWFStatus(
                set(["Running", "Done", "CancelRequested", "Canceled"])))
        self.assertEqual("canceled", convertWQElementsStatusToWFStatus(
                set(["Done", "CancelRequested"])))
        self.assertEqual("canceled", convertWQElementsStatusToWFStatus(set(["CancelRequested"])))

    def test2ConvertWQElementsStatusToWFStatus(self):
        """
        Same as the test 'testConvertWQElementsStatusToWFStatus', but testing
        'convertWQElementsStatusToWFStatus' function from a different angle.
        """
        # single WQE with standard state transition
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Available"])), "acquired")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Negotiating"])), "acquired")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Acquired"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Running"])), "running-closed")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Done"])), "completed")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["CancelRequested"])), "canceled")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Canceled"])), "completed")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Failed"])), "failed")

        # double WQE with standard state transition
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Available", "Negotiating"])), "acquired")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Available", "Acquired"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Available", "Running"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Available", "Done"])), 'running-open')
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Available", "Failed"])), "acquired")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Negotiating", "Acquired"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Negotiating", "Running"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Negotiating", "Done"])), 'running-open')
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Negotiating", "Failed"])), "acquired")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Acquired", "Running"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Acquired", "Done"])), 'running-open')
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Acquired", "Failed"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Running", "Done"])), 'running-closed')
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Running", "Failed"])), "running-closed")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Done", "Failed"])), "completed")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Done", "CancelRequested"])), "canceled")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Done", "Canceled"])), "completed")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["CancelRequested", "Canceled"])), "canceled")

        # triple WQE with standard state transition
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Available", "Negotiating", "Acquired"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Available", "Negotiating", "Running"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Available", "Negotiating", "Done"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Available", "Negotiating", "Failed"])), "acquired")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Available", "Acquired", "Running"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Available", "Acquired", "Done"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Available", "Acquired", "Failed"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Available", "Running", "Done"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Available", "Running", "Failed"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Available", "Done", "Failed"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Negotiating", "Acquired", "Running"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Negotiating", "Acquired", "Done"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Negotiating", "Acquired", "Failed"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Negotiating", "Running", "Done"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Negotiating", "Running", "Failed"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Negotiating", "Done", "Failed"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Acquired", "Running", "Done"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Acquired", "Running", "Failed"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Acquired", "Done", "Failed"])), "running-open")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Running", "Done", "Failed"])), "running-closed")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["CancelRequested", "Done", "Failed"])), "canceled")
        self.assertEqual(convertWQElementsStatusToWFStatus(set(["Canceled", "Done", "Failed"])), "completed")


if __name__ == '__main__':
    unittest.main()
