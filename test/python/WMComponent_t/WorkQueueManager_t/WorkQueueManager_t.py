#!/usr/bin/env python

"""
WorkQueuManager test
"""

import os
import logging
import threading
import unittest
import time
import shutil
import WMCore.WMInit
from subprocess import Popen, PIPE
import types

from WMCore.Agent.Configuration import loadConfigurationFile

from WMComponent.WorkQueueManager.WorkQueueManager import WorkQueueManager
from WMComponent.WorkQueueManager.WorkQueueManagerReqMgrPoller \
    import WorkQueueManagerReqMgrPoller
from WMCore.WorkQueue.WorkQueue import WorkQueue, globalQueue, localQueue

from WMQuality.Emulators.RequestManagerClient.RequestManager \
    import RequestManager as fakeReqMgr

from WMCore_t.WorkQueue_t.WorkQueueTestCase import WorkQueueTestCase
from WMQuality.Emulators.EmulatorSetup import EmulatorHelper

def getFirstTask(wmspec):
    """Return the 1st top level task"""
    # http://www.logilab.org/ticket/8774
    # pylint: disable-msg=E1101,E1103
    return wmspec.taskIterator().next()

class WorkQueueManagerTest(WorkQueueTestCase):
    """
    TestCase for WorkQueueManagerTest module 
    """


    _maxMessage = 10

    def setSchema(self):
        self.schema = ["WMCore.WorkQueue.Database", "WMCore.WMBS",
                        "WMCore.MsgService", "WMCore.ThreadPool"]
    
    def setUp(self):
        WorkQueueTestCase.setUp(self)
        EmulatorHelper.setEmulators(phedex = True, dbs = True, 
                                    siteDB = True, requestMgr = False)
    def tearDown(self):
        WorkQueueTestCase.tearDown(self)
        EmulatorHelper.resetEmulators()
        
    def getConfig(self):
        """
        _createConfig_

        General config file
        """
        #configPath=os.path.join(WMCore.WMInit.getWMBASE(), \
        #                        'src/python/WMComponent/WorkQueueManager/DefaultConfig.py')):


        config = self.testInit.getConfiguration()
        # http://www.logilab.org/ticket/8961
        # pylint: disable-msg=E1101, E1103
        config.component_("WorkQueueManager")
        config.section_("General")
        config.General.workDir = "."
        config.WorkQueueManager.team = 'team_usa'
        config.WorkQueueManager.requestMgrHost = 'cmssrv49.fnal.gov:8585'
        config.WorkQueueManager.serviceUrl = "http://cmssrv18.fnal.gov:6660"
        
        config.WorkQueueManager.logLevel = 'INFO'
        config.WorkQueueManager.pollInterval = 10
        config.WorkQueueManager.level = "GlobalQueue"
        return config        
        


    def testComponentBasic(self):
        """
        Tests the components, as in sees if they load.
        Otherwise does nothing.
        """
        return
        myThread = threading.currentThread()

        config = self.getConfig()

        testWorkQueueManager = WorkQueueManager(config)
        testWorkQueueManager.prepareToStart()
        
        time.sleep(30)
        print "Killing"
        myThread.workerThreadManager.terminateWorkers()

        return

    def setupGlobalWorkqueue(self):
        """Return a workqueue instance"""

        globalQ = globalQueue(CacheDir = self.workDir,
                           NegotiationTimeout = 0,
                           QueueURL = 'global.example.com',
                           Teams = ["The A-Team", "some other bloke"])
        return globalQ

    def testReqMgrPollerAlgorithm(self):
        """ReqMgr reporting"""
        # don't actually talk to ReqMgr - mock it.

        globalQ = self.setupGlobalWorkqueue()
        reqMgr = fakeReqMgr()
        reqPoller = WorkQueueManagerReqMgrPoller(reqMgr, globalQ, {})

        # 1st run should pull a request
        self.assertEqual(len(globalQ), 0)
        reqPoller.algorithm({})
        self.assertEqual(len(globalQ), 1)

        # local queue acquires and runs
        work = globalQ.getWork({'SiteA' : 10000, 'SiteB' : 10000})
        globalQ.setStatus('Acquired', 1)
        self.assertEqual(len(globalQ), 0)
        reqPoller.algorithm({})
        self.assertEqual(reqMgr.status[reqMgr.names[0]], 'running')

        # finish work
        globalQ.setStatus('Done', 1)
        reqPoller.algorithm({})
        self.assertEqual(reqMgr.status[reqMgr.names[0]], 'completed')
        # and removed from WorkQueue
        self.assertEqual(len(globalQ.status()), 0)

        # reqMgr problems should not crash client
        reqPoller = WorkQueueManagerReqMgrPoller(None, globalQ, {})
        reqPoller.algorithm({})
        reqMgr._removeSpecs()


    def testReqMgrBlockSplitting(self):
        """ReqMgr interaction with block level splitting"""
        globalQ = self.setupGlobalWorkqueue()
        reqMgr = fakeReqMgr(splitter = 'Block')
        reqPoller = WorkQueueManagerReqMgrPoller(reqMgr, globalQ, {})

        self.assertEqual(len(globalQ), 0)
        reqPoller.algorithm({})
        self.assertEqual(len(globalQ), 2)
        globalQ.setStatus('Acquired', [1, 2])
        elements = globalQ.status()
        self.assertEqual(len(elements), 2)
        elements[0]['PercentComplete'] = 25
        elements[1]['PercentComplete'] = 75
        globalQ.setProgress(elements[0])
        globalQ.setProgress(elements[1])
        elements = globalQ.status()
        self.assertEqual(elements[0]['PercentComplete'], 25)
        self.assertEqual(elements[1]['PercentComplete'], 75)
        reqPoller.algorithm({}) # report back to ReqMgr
        self.assertEqual(reqMgr.progress[reqMgr.names[0]]['percent_complete'],
                         50)
        self.assertEqual(reqMgr.status[reqMgr.names[0]], 'running')
        globalQ.setStatus('Done', [1, 2])
        elements[0]['PercentComplete'] = 100
        elements[1]['PercentComplete'] = 100
        globalQ.setProgress(elements[0])
        globalQ.setProgress(elements[1])
        reqPoller.algorithm({}) # report back to ReqMgr
        self.assertEqual(reqMgr.progress[reqMgr.names[0]]['percent_complete'],
                         100)
        self.assertEqual(reqMgr.status[reqMgr.names[0]], 'completed')
        reqMgr._removeSpecs()


    def testInvalidSpec(self):
        """Report invalid spec back to ReqMgr"""
        globalQ = self.setupGlobalWorkqueue()
        reqMgr = fakeReqMgr(inputDataset = 'thisdoesntexist')
        #reqMgr = fakeReqMgr()

        reqPoller = WorkQueueManagerReqMgrPoller(reqMgr, globalQ, {})
        reqPoller.algorithm({})
        self.assertEqual('failed', reqMgr.status[reqMgr.names[0]])
        self.assertTrue('No work in spec:' in reqMgr.msg[reqMgr.names[0]])
        reqMgr._removeSpecs()

        globalQ = self.setupGlobalWorkqueue()
        reqMgr = fakeReqMgr(dbsUrl = 'wrongprot://dbs.example.com')
        reqPoller = WorkQueueManagerReqMgrPoller(reqMgr, globalQ, {})
        reqPoller.algorithm({})
        self.assertEqual('failed', reqMgr.status[reqMgr.names[0]])
        self.assertTrue('DBS config error' in reqMgr.msg[reqMgr.names[0]])
        reqMgr._removeSpecs()


if __name__ == '__main__':
    unittest.main()
