#!/usr/bin/env python

"""
WorkQueueRegMgrInterface test
"""

import unittest

from WMCore_t.WorkQueue_t.WorkQueueTestCase import WorkQueueTestCase
from nose.plugins.attrib import attr

from WMCore.WorkQueue.WorkQueue import globalQueue, localQueue
from WMCore.WorkQueue.WorkQueueReqMgrInterface import WorkQueueReqMgrInterface
from WMQuality.Emulators.DataBlockGenerator.Globals import GlobalParams
from WMQuality.Emulators.ReqMgrClient.ReqMgr import ReqMgr as fakeReqMgr


def getFirstTask(wmspec):
    """Return the 1st top level task"""
    # http://www.logilab.org/ticket/8774
    # pylint: disable=E1101,E1103
    return next(wmspec.taskIterator())


class WorkQueueReqMgrInterfaceTest(WorkQueueTestCase):
    """
    TestCase for WorkQueueReqMgrInterface module
    """

    def setSchema(self):
        self.schema = []
        self.couchApps = ["WorkQueue"]

    def setUp(self):
        self.reqmgr2_endpoint = "https://cmsweb-testbed.cern.ch/reqmgr2"

        WorkQueueTestCase.setUp(self)
        GlobalParams.resetParams()
        self.globalQCouchUrl = "%s/%s" % (self.testInit.couchUrl, self.globalQDB)
        self.localQCouchUrl = "%s/%s" % (self.testInit.couchUrl,
                                         self.localQInboxDB)

    def tearDown(self):
        WorkQueueTestCase.tearDown(self)
        GlobalParams.resetParams()

    def getConfig(self):
        """
        _createConfig_

        General config file
        """
        # configPath=os.path.join(WMCore.WMInit.getWMBASE(), \
        #                        'src/python/WMComponent/WorkQueueManager/DefaultConfig.py')):

        config = self.testInit.getConfiguration()
        # http://www.logilab.org/ticket/8961
        # pylint: disable=E1101, E1103
        config.component_("WorkQueueManager")
        config.section_("General")
        config.General.workDir = "."

        config.WorkQueueManager.logLevel = 'INFO'
        config.WorkQueueManager.pollInterval = 10
        config.WorkQueueManager.level = "GlobalQueue"
        return config

    def setupGlobalWorkqueue(self, **kwargs):
        """Return a workqueue instance"""
        kwargs.setdefault('rucioAccount', "wmcore_transferor")
        kwargs.setdefault('rucioAuthUrl', "https://cms-rucio-auth-int.cern.ch")
        kwargs.setdefault('rucioUrl', "http://cms-rucio-int.cern.ch")
        globalQ = globalQueue(DbName=self.globalQDB,
                              InboxDbName=self.globalQInboxDB,
                              QueueURL=self.globalQCouchUrl,
                              **kwargs)
        return globalQ

    def setupLocalQueue(self):
        """Create a local queue"""
        localQ = localQueue(DbName=self.localQDB,
                            InboxDbName=self.localQInboxDB,
                            QueueURL=self.localQCouchUrl,
                            Teams=["The A-Team", "some other bloke"],
                            ParentQueueCouchUrl=self.globalQCouchUrl,
                            CacheDir=self.testInit.testDir)
        return localQ

    @attr('integration')  # BROKEN
    def testReqMgrPollerAlgorithm(self):
        """ReqMgr reporting"""
        # don't actually talk to ReqMgr - mock it.
        globalQ = self.setupGlobalWorkqueue()
        localQ = self.setupLocalQueue()
        reqMgr = fakeReqMgr(splitter='Block')
        reqMgrInt = WorkQueueReqMgrInterface()
        reqMgrInt.reqMgr = reqMgr

        # 1st run should pull a request
        self.assertEqual(len(globalQ), 0)
        reqMgrInt(globalQ)
        self.assertEqual(len(globalQ), 2)
        self.assertEqual(reqMgr.status[reqMgr.names[0]], 'acquired')

        # local queue acquires and runs
        globalQ.updateLocationInfo()
        work = localQ.pullWork({'T2_XX_SiteA': 10000, 'T2_XX_SiteB': 10000})
        self.assertEqual(len(globalQ), 0)
        reqMgrInt(globalQ)
        self.assertEqual(reqMgr.status[reqMgr.names[0]], 'acquired')

        # start running work
        globalQ.setStatus('Running', WorkflowName=reqMgr.names[0])
        globalQ.performQueueCleanupActions()
        reqMgrInt(globalQ)  # report back to ReqMgr
        self.assertEqual(reqMgr.status[reqMgr.names[0]], 'running-closed')

        # finish work
        work = globalQ.status()
        globalQ.setStatus('Done', elementIDs=[x.id for x in work])
        reqMgrInt(globalQ)
        globalQ.performQueueCleanupActions()
        reqMgrInt(globalQ)
        self.assertEqual(reqMgr.status[reqMgr.names[0]], 'completed')
        # and removed from WorkQueue
        self.assertEqual(len(globalQ.status()), 0)

        # reqMgr problems should not crash client
        reqMgrInt.reqMgr = None
        reqMgrInt(globalQ)
        reqMgr._removeSpecs()

    @attr('integration')  # BROKEN
    def testReqMgrProgress(self):
        """ReqMgr interaction with block level splitting"""
        globalQ = self.setupGlobalWorkqueue()
        reqMgr = fakeReqMgr(splitter='Block')
        # reqMgrInt = WorkQueueReqMgrInterface(reqmgr2_endpoint=self.reqmgr2_endpoint)
        reqMgrInt = WorkQueueReqMgrInterface(reqmgr2_endpoint="https://fakeurl")
        reqMgrInt.reqMgr2 = reqMgr

        self.assertEqual(len(globalQ), 0)
        reqMgrInt(globalQ)
        self.assertEqual(len(globalQ), 2)
        globalQ.setStatus('Acquired', WorkflowName=reqMgr.names[0])
        reqMgrInt(globalQ)  # report back to ReqMgr
        self.assertEqual(reqMgr.status[reqMgr.names[0]], 'acquired')
        globalQ.setStatus('Running', WorkflowName=reqMgr.names[0])
        elements = globalQ.status()
        self.assertEqual(len(elements), 2)
        [globalQ.backend.updateElements(x.id, PercentComplete=75, PercentSuccess=25) for x in elements]
        elements = globalQ.status()
        globalQ.performQueueCleanupActions()
        reqMgrInt(globalQ)  # report back to ReqMgr
        self.assertEqual(reqMgr.status[reqMgr.names[0]], 'running-closed')
        globalQ.performQueueCleanupActions()
        reqMgrInt(globalQ)
        self.assertEqual(reqMgr.progress[reqMgr.names[0]]['percent_complete'],
                         75)
        self.assertEqual(reqMgr.progress[reqMgr.names[0]]['percent_success'],
                         25)
        self.assertEqual(reqMgr.status[reqMgr.names[0]], 'running-closed')
        globalQ.setStatus('Done', WorkflowName=reqMgr.names[0])
        reqMgrInt(globalQ)  # report back to ReqMgr
        globalQ.performQueueCleanupActions()
        reqMgrInt(globalQ)
        self.assertEqual(reqMgr.progress[reqMgr.names[0]]['percent_complete'],
                         75)
        self.assertEqual(reqMgr.status[reqMgr.names[0]], 'completed')
        reqMgr._removeSpecs()

    @attr('integration')  # BROKEN
    def testInvalidSpec(self):
        """Report invalid spec back to ReqMgr"""
        globalQ = self.setupGlobalWorkqueue()
        reqMgr = fakeReqMgr(inputDataset='thisdoesntexist')
        reqMgrInt = WorkQueueReqMgrInterface()
        reqMgrInt.reqMgr = reqMgr
        reqMgrInt(globalQ)
        self.assertEqual('failed', reqMgr.status[reqMgr.names[0]])
        self.assertTrue('No work in spec:' in reqMgr.msg[reqMgr.names[0]])
        reqMgr._removeSpecs()

        globalQ = self.setupGlobalWorkqueue()
        reqMgr = fakeReqMgr(dbsUrl='wrongprot://dbs.example.com')
        reqMgrInt = WorkQueueReqMgrInterface()
        reqMgrInt.reqMgr = reqMgr
        reqMgrInt(globalQ)
        self.assertEqual('failed', reqMgr.status[reqMgr.names[0]])
        self.assertTrue('DBS config error' in reqMgr.msg[reqMgr.names[0]])
        reqMgr._removeSpecs()

    @attr('integration')  # BROKEN
    def testCancelPickedUp(self):
        """WorkQueue cancels if canceled in ReqMgr"""
        globalQ = self.setupGlobalWorkqueue()
        reqMgr = fakeReqMgr()
        reqMgrInt = WorkQueueReqMgrInterface()
        reqMgrInt.reqMgr = reqMgr
        reqMgrInt(globalQ)
        # abort in reqmgr
        reqMgr.status[reqMgr.names[0]] = 'aborted'
        # workqueue will detect abort and cancel workflow
        reqMgrInt(globalQ)
        self.assertEqual(globalQ.status(WorkflowName=reqMgr.names[0])[0]['Status'], 'Canceled')
        # check stays canceled
        reqMgrInt(globalQ)
        self.assertEqual(globalQ.status(WorkflowName=reqMgr.names[0])[0]['Status'], 'Canceled')

    @attr('integration')  # BROKEN
    def testReqMgrWaitTime(self):
        """If running request finished in Reqmgr and no update locally for a long time decalre request done"""
        globalQ = self.setupGlobalWorkqueue()
        reqMgr = fakeReqMgr()
        reqMgrInt = WorkQueueReqMgrInterface()
        reqMgrInt.reqMgr = reqMgr
        reqMgrInt(globalQ)
        # set reqmgr status to done
        reqMgr.status[reqMgr.names[0]] = 'completed'
        # Only running elements are eligible for cleanup
        reqMgrInt(globalQ)
        self.assertEqual(globalQ.status(WorkflowName=reqMgr.names[0])[0]['Status'], 'Available')
        # Set running and element update time to the past (mimic time elapsing)
        globalQ.setStatus('Running', WorkflowName=reqMgr.names[0])
        element = globalQ.backend.inbox.document(reqMgr.names[0])
        element['updatetime'] = 0
        element['WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement']['Status'] = 'Running'
        globalQ.backend.inbox.commitOne(element)
        elements = [globalQ.backend.db.document(x.id) for x in globalQ.status(RequestName=reqMgr.names[0])]
        for element in elements:
            element['updatetime'] = 0
            element['WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement']['Status'] = 'Running'
            globalQ.backend.db.queue(element)
        globalQ.backend.db.commit()
        # workqueue should see an old done request and update status to match
        reqMgrInt(globalQ)
        self.assertEqual(globalQ.status(WorkflowName=reqMgr.names[0])[0]['Status'], 'Done')

    @attr('integration')  # BROKEN
    def testReqMgrOpenRequests(self):
        """Check the mechanics of open running requests"""
        # don't actually talk to ReqMgr - mock it.
        globalQ = self.setupGlobalWorkqueue()
        localQ = self.setupLocalQueue()
        reqMgr = fakeReqMgr(splitter='Block', openRunningTimeout=3600)
        reqMgrInt = WorkQueueReqMgrInterface()
        reqMgrInt.reqMgr = reqMgr

        # 1st run should pull a request
        self.assertEqual(len(globalQ), 0)
        reqMgrInt(globalQ)
        self.assertEqual(len(globalQ), 2)
        self.assertEqual(reqMgr.status[reqMgr.names[0]], 'acquired')

        # local queue acquires and runs
        globalQ.updateLocationInfo()
        localQ.pullWork({'T2_XX_SiteA': 10000, 'T2_XX_SiteB': 10000})
        self.assertEqual(len(globalQ), 0)
        reqMgrInt(globalQ)
        self.assertEqual(reqMgr.status[reqMgr.names[0]], 'acquired')

        # start running work
        globalQ.setStatus('Running', WorkflowName=reqMgr.names[0])
        globalQ.performQueueCleanupActions()
        reqMgrInt(globalQ)  # report back to ReqMgr
        self.assertEqual(reqMgr.status[reqMgr.names[0]], 'running-open')

        # Work should not be closed yet
        reqMgrInt(globalQ)
        globalQ.performQueueCleanupActions()
        self.assertEqual(reqMgr.status[reqMgr.names[0]], 'running-open')

        # Now add 2 new blocks to dbs, the reqMgr should put more work in the queue for the request
        GlobalParams.setNumOfBlocksPerDataset(GlobalParams.numOfBlocksPerDataset() + 2)
        reqMgrInt(globalQ)
        globalQ.performQueueCleanupActions()
        self.assertEqual(len(globalQ), 2)

        # Work that can be pulled normally and request stays in running-open
        globalQ.updateLocationInfo()
        localQ.pullWork({'T2_XX_SiteC': 10000})
        self.assertEqual(len(globalQ), 0)
        reqMgrInt(globalQ)
        self.assertEqual(reqMgr.status[reqMgr.names[0]], 'running-open')

        # Put the latest work to run
        globalQ.setStatus('Running', WorkflowName=reqMgr.names[0])
        globalQ.performQueueCleanupActions()
        reqMgrInt(globalQ)  # report back to ReqMgr
        self.assertEqual(reqMgr.status[reqMgr.names[0]], 'running-open')

        # Change the request status manually to close it
        reqMgr.status[reqMgr.names[0]] = 'running-closed'
        globalQ.performQueueCleanupActions()
        reqMgrInt(globalQ)  # report back to WorkQueue
        self.assertEqual(len(globalQ.backend.getInboxElements(OpenForNewData=False)), 1)

        # Put 1 more block in DBS for the dataset, request is closed so no more data is added
        GlobalParams.setNumOfBlocksPerDataset(GlobalParams.numOfBlocksPerDataset() + 1)
        reqMgrInt(globalQ)
        globalQ.performQueueCleanupActions()
        self.assertEqual(len(globalQ), 0)

        # finish work
        work = globalQ.status()
        globalQ.setStatus('Done', elementIDs=[x.id for x in work])
        reqMgrInt(globalQ)
        globalQ.performQueueCleanupActions()
        reqMgrInt(globalQ)
        self.assertEqual(reqMgr.status[reqMgr.names[0]], 'completed')
        # and removed from WorkQueue
        self.assertEqual(len(globalQ.status()), 0)

        # reqMgr problems should not crash client
        reqMgrInt.reqMgr = None
        reqMgrInt(globalQ)
        reqMgr._removeSpecs()


if __name__ == '__main__':
    unittest.main()
