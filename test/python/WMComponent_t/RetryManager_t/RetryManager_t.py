#!/usr/bin/env python
"""
RetryManager test for module and the harness
"""

__revision__ = "$Id: RetryManager_t.py,v 1.7 2010/02/05 16:52:31 sfoulkes Exp $"
__version__ = "$Revision: 1.7 $"

import os
import threading
import time
import unittest

from WMComponent.RetryManager.RetryManagerPoller import RetryManagerPoller

import WMCore.WMInit
from WMQuality.TestInit   import TestInit
from WMCore.DAOFactory    import DAOFactory
from WMCore.Services.UUID import makeUUID

from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow     import Workflow
from WMCore.WMBS.File         import File
from WMCore.WMBS.Fileset      import Fileset
from WMCore.WMBS.Job          import Job
from WMCore.WMBS.JobGroup     import JobGroup

from WMCore.DataStructs.Run   import Run
from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.Agent.Configuration import Configuration

class RetryManagerTest(unittest.TestCase):
    """
    TestCase for TestRetryManager module 
    """
    def setUp(self):
        """
        setup for test.
        """
        myThread = threading.currentThread()
        
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS",
                                                 "WMCore.MsgService"],
                                useDefault = False)
        
        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        self.getJobs = self.daofactory(classname = "Jobs.GetAllJobs")
        self.setJobTime = self.daofactory(classname = "Jobs.SetStateTime")
        self.testDir = self.testInit.generateWorkDir()
        self.nJobs = 10
        return

    def tearDown(self):
        """
        Database deletion
        """
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()
        return

    def getConfig(self):
        """
        _getConfig_

        """
        config = Configuration()

        # First the general stuff
        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR", self.testDir)
        config.section_("CoreDatabase")
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.socket     = os.getenv("DBSOCK")

        config.component_("RetryManager")
        config.RetryManager.logLevel = 'DEBUG'
        config.RetryManager.namespace = 'WMComponent.RetryManager.RetryManager'
        config.RetryManager.maxRetries = 10
        config.RetryManager.pollInterval = 10
        # These are the cooloff times for the RetryManager, the times it waits
        # Before attempting resubmission
        config.RetryManager.coolOffTime  = {'create': 120, 'submit': 120, 'job': 120}
        # Path to plugin directory
        config.RetryManager.pluginPath = 'WMComponent.RetryManager.PlugIns'
        config.RetryManager.pluginName = ''
        config.RetryManager.WMCoreBase = WMCore.WMInit.getWMBASE()

        # JobStateMachine
        config.component_('JobStateMachine')
        config.JobStateMachine.couchurl        = os.getenv('COUCHURL', None)
        config.JobStateMachine.default_retries = 1
        config.JobStateMachine.couchDBName     = "retry_manager_t"

        return config

    def createTestJobGroup(self):
        """
        _createTestJobGroup_
        
        Creates a group of several jobs
        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001", task="Test")
        testWorkflow.create()
        
        testWMBSFileset = Fileset(name = "TestFileset")
        testWMBSFileset.create()
        
        testSubscription = Subscription(fileset = testWMBSFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        testJobGroup = JobGroup(subscription = testSubscription)
        testJobGroup.create()

        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024, events = 10)
        testFileA.addRun(Run(10, *[12312]))
        testFileA.setLocation('malpaquet')

        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10)
        testFileB.addRun(Run(10, *[12312]))
        testFileA.setLocation('malpaquet')
        testFileA.create()
        testFileB.create()

        for i in range(0,self.nJobs):
            testJob = Job(name = makeUUID())
            testJob.addFile(testFileA)
            testJob.addFile(testFileB)
            testJob['retry_count'] = 1
            testJobGroup.add(testJob)
        
        testJobGroup.commit()
        return testJobGroup

    def testCreate(self):
        """
        WMComponent_t.RetryManager_t.RetryManager_t:testCreate()
        
        Mimics creation of component and test jobs failed in create stage.
        """
        testJobGroup = self.createTestJobGroup()

        config = self.getConfig()
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'createfailed', 'new')
        changer.propagate(testJobGroup.jobs, 'createcooloff', 'createfailed')

        idList = self.getJobs.execute(state = 'CreateCooloff')
        self.assertEqual(len(idList), self.nJobs)

        testRetryManager = RetryManagerPoller(config)
        testRetryManager.setup(None)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID = job["id"],
                                    stateTime = int(time.time()) - 50)

        testRetryManager.algorithm(None)
        idList = self.getJobs.execute(state = 'CreateCooloff')
        self.assertEqual(len(idList), self.nJobs)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID = job["id"],
                                    stateTime = int(time.time()) - 150)

        testRetryManager.algorithm(None)
        
        idList = self.getJobs.execute(state = 'CreateCooloff')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state = 'New')
        self.assertEqual(len(idList), self.nJobs)
        return

    def testSubmit(self):
        """
        WMComponent_t.RetryManager_t.RetryManager_t:testSubmit()
        
        Mimics creation of component and test jobs failed in create stage.
        """
        testJobGroup = self.createTestJobGroup()

        config = self.getConfig()
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'submitfailed', 'created')
        changer.propagate(testJobGroup.jobs, 'submitcooloff', 'submitfailed')

        idList = self.getJobs.execute(state = 'SubmitCooloff')
        self.assertEqual(len(idList), self.nJobs)

        testRetryManager = RetryManagerPoller(config)
        testRetryManager.setup(None)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID = job["id"],
                                    stateTime = int(time.time()) - 50)

        testRetryManager.algorithm(None)

        idList = self.getJobs.execute(state = 'SubmitCooloff')
        self.assertEqual(len(idList), self.nJobs)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID = job["id"],
                                    stateTime = int(time.time()) - 150)        

        testRetryManager.algorithm(None)
        
        idList = self.getJobs.execute(state = 'SubmitCooloff')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state = 'Created')
        self.assertEqual(len(idList), self.nJobs)
        return

    def testJob(self):
        """
        WMComponent_t.RetryManager_t.RetryManager_t:testJob()
        
        Mimics creation of component and test jobs failed in create stage.
        """
        testJobGroup = self.createTestJobGroup()
        
        config = self.getConfig()
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup.jobs, 'jobfailed', 'complete')
        changer.propagate(testJobGroup.jobs, 'jobcooloff', 'jobfailed')

        idList = self.getJobs.execute(state = 'JobCooloff')
        self.assertEqual(len(idList), self.nJobs)

        testRetryManager = RetryManagerPoller(config)
        testRetryManager.setup(None)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID = job["id"],
                                    stateTime = int(time.time()) - 50)

        testRetryManager.algorithm(None)

        idList = self.getJobs.execute(state = 'JobCooloff')
        self.assertEqual(len(idList), self.nJobs)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID = job["id"],
                                    stateTime = int(time.time()) - 150)

        testRetryManager.algorithm(None)

        idList = self.getJobs.execute(state = 'JobCooloff')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state = 'Created')
        self.assertEqual(len(idList), self.nJobs)
        return

if __name__ == '__main__':
    unittest.main()
