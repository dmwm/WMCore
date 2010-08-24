#!/usr/bin/env python

"""
JobArchiver test 
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



from WMQuality.TestInit   import TestInit

from WMComponent.WorkQueueManager.WorkQueueManager import WorkQueueManager
from WMComponent.WorkQueueManager.WorkQueueManagerReqMgrPoller import WorkQueueManagerReqMgrPoller
from WMCore.WorkQueue.WorkQueue import WorkQueue, globalQueue, localQueue
from WMCore_t.WorkQueue_t.MockDBSReader import MockDBSReader
from WMCore_t.WorkQueue_t.MockPhedexService import MockPhedexService
from WMCore_t.WorkQueue_t.WorkQueue_t import TestReRecoFactory, rerecoArgs
from WMCore_t.WorkQueue_t.WorkQueue_t import getFirstTask

class WorkQueueManagerTest(unittest.TestCase):
    """
    TestCase for WorkQueueManagerTest module 
    """


    _maxMessage = 10

    def setUp(self):
        """
        setup for test.
        """

        myThread = threading.currentThread()
        
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        #self.tearDown()
        self.testInit.setSchema(customModules = ["WMCore.WorkQueue.Database", "WMCore.WMBS", 
                                                 "WMCore.MsgService", "WMCore.ThreadPool"],
                                useDefault = False)
        self.workDir = self.testInit.generateWorkDir()

    def tearDown(self):
        """
        Database deletion
        """

        self.testInit.clearDatabase()


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

    def setupGlobalWorkqueue(self, spec):
        """Return a workqueue instance"""
        dataset = getFirstTask(spec).getInputDatasetPath()
        inputDataset = getFirstTask(spec).inputDataset()
        mockDBS = MockDBSReader('http://example.com', dataset)
        dbsHelpers = {'http://example.com' : mockDBS,
                      'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet' : mockDBS,
                      inputDataset.dbsurl : MockDBSReader(inputDataset.dbsurl, dataset),
                      }

        globalQ = globalQueue(CacheDir = self.workDir,
                           NegotiationTimeout = 0,
                           QueueURL = 'global.example.com',
                           DBSReaders = dbsHelpers,
                           Teams = ["The A-Team", "some other bloke"])
        globalQ.phedexService = MockPhedexService(dataset)
        return globalQ

    def createProcessingSpec(self, splitter = 'DatasetBlock'):
        """Return a processing spec"""
        wf = TestReRecoFactory()('ReRecoWorkload', rerecoArgs)
        wf.setSpecUrl(os.path.join(self.workDir, 'testworkflow.spec'))
        wf.setStartPolicy(splitter)
        wf.save(wf.specUrl())
        return wf

    def testReqMgrPollerAlgorithm(self):
        """ReqMgr reporting"""
        # don't actually talk to ReqMgr - mock it.



        spec = self.createProcessingSpec()
        globalQ = self.setupGlobalWorkqueue(spec)
        reqMgr = fakeReqMgr(spec)
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
        self.assertEqual(reqMgr.status[str(reqMgr.count)], 'running')

        # finish work
        globalQ.setStatus('Done', 1)
        reqPoller.algorithm({})
        self.assertEqual(reqMgr.status[str(reqMgr.count)], 'completed')

        globalQ.setStatus('Failed', 1)
        reqPoller.algorithm({})
        self.assertEqual(reqMgr.status[str(reqMgr.count)], 'failed')

        # reqMgr problems should not crash client
        reqPoller = WorkQueueManagerReqMgrPoller(None, globalQ, {})
        reqPoller.algorithm({})


    def testReqMgrBlockSplitting(self):
        """ReqMgr interaction with block level splitting"""
        spec = self.createProcessingSpec(splitter = 'Block')
        globalQ = self.setupGlobalWorkqueue(spec)
        reqMgr = fakeReqMgr(spec)
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
        self.assertEqual(reqMgr.progress[str(reqMgr.count)]['percent_complete'],
                         50)
        self.assertEqual(reqMgr.status[str(reqMgr.count)], 'running')
        globalQ.setStatus('Done', [1, 2])
        elements[0]['PercentComplete'] = 100
        elements[1]['PercentComplete'] = 100
        globalQ.setProgress(elements[0])
        globalQ.setProgress(elements[1])
        reqPoller.algorithm({}) # report back to ReqMgr
        self.assertEqual(reqMgr.progress[str(reqMgr.count)]['percent_complete'],
                         100)
        self.assertEqual(reqMgr.status[str(reqMgr.count)], 'completed')

class fakeReqMgr():
    """Fake ReqMgr stuff"""
    def __init__(self, spec):
        self.spec = spec
        self.count = 0
        self.status = {}
        self.progress = {}

    def getAssignment(self, team):
        assert(type(team) in types.StringTypes)
        if not self.count and team == 'The A-Team':
            self.count += 1
            return {str(self.count) : self.spec.specUrl()}
        else:
            return {}

    def putWorkQueue(self, reqName, url):
        self.status[reqName] = 'assigned-prodmgr'

    def reportRequestStatus(self, name, status):
        self.status[name] = status

    def reportRequestProgress(self, name, **args):
        self.progress.setdefault(name, {})
        self.progress[name].update(args)


if __name__ == '__main__':
    unittest.main()