#!/usr/bin/env python
# pylint: disable=E1101, C0103
# E1101: Use configuration files
# C0103: Different naming conventions apply for tests
"""
RetryManager test for module and the harness
"""
from __future__ import print_function

from builtins import range
import os
import os.path
import threading
import time
import unittest
from nose.plugins.attrib import attr

import WMCore.WMBase
from WMComponent.RetryManager.RetryManagerPoller import RetryManagerPoller
from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.Run import Run
from WMCore.FwkJobReport.Report import Report
from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.Services.UUIDLib import makeUUID
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMQuality.Emulators import EmulatorSetup
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase


class RetryManagerTest(EmulatedUnitTestCase):
    """
    TestCase for TestRetryManager module
    """

    def setUp(self):
        """
        setup for test.
        """
        super(RetryManagerTest, self).setUp()
        myThread = threading.currentThread()

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules=["WMCore.WMBS"],
                                useDefault=False)
        self.testInit.setupCouch("retry_manager_t/jobs", "JobDump")
        self.testInit.setupCouch("retry_manager_t/fwjrs", "FWJRDump")

        self.daofactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)
        self.getJobs = self.daofactory(classname="Jobs.GetAllJobs")
        self.setJobTime = self.daofactory(classname="Jobs.SetStateTime")
        self.increaseRetry = self.daofactory(classname="Jobs.IncrementRetry")
        self.testDir = self.testInit.generateWorkDir()
        self.configFile = EmulatorSetup.setupWMAgentConfig()
        self.nJobs = 10
        return

    def tearDown(self):
        """
        Database deletion
        """
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()
        self.testInit.tearDownCouch()
        EmulatorSetup.deleteConfig(self.configFile)
        return

    def getConfig(self):
        """
        _getConfig_

        """
        config = self.testInit.getConfiguration()
        self.testInit.generateWorkDir(config)

        # First the general stuff
        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR", self.testDir)
        config.section_("CoreDatabase")
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.socket = os.getenv("DBSOCK")

        config.component_("RetryManager")
        config.RetryManager.logLevel = 'DEBUG'
        config.RetryManager.namespace = 'WMComponent.RetryManager.RetryManager'
        config.RetryManager.pollInterval = 10
        # These are the cooloff times for the RetryManager, the times it waits
        # Before attempting resubmission
        config.RetryManager.section_("DefaultRetryAlgo")
        config.RetryManager.DefaultRetryAlgo.section_("default")
        config.RetryManager.DefaultRetryAlgo.default.coolOffTime = {'create': 120, 'submit': 120, 'job': 120}
        # Path to plugin directory
        config.RetryManager.pluginPath = 'WMComponent.RetryManager.PlugIns'
        config.RetryManager.WMCoreBase = WMCore.WMBase.getWMBASE()
        config.RetryManager.componentDir = os.path.join(os.getcwd(), 'Components')

        # ErrorHandler
        # Not essential, but useful for ProcessingAlgo
        config.component_("ErrorHandler")
        config.ErrorHandler.maxRetries = 5

        # JobStateMachine
        config.component_('JobStateMachine')
        config.JobStateMachine.couchurl = os.getenv('COUCHURL', None)
        config.JobStateMachine.couchDBName = "retry_manager_t"

        return config

    def createTestJobGroup(self, nJobs, subType="Processing", retryOnce=False):
        """
        _createTestJobGroup_

        Creates a group of several jobs
        """
        testWorkflow = Workflow(spec="spec.xml", owner="Simon",
                                name=makeUUID(), task="Test")
        testWorkflow.create()

        testWMBSFileset = Fileset(name="TestFileset")
        testWMBSFileset.create()
        testSubscription = Subscription(fileset=testWMBSFileset,
                                        workflow=testWorkflow,
                                        type=subType)
        testSubscription.create()

        testJobGroup = JobGroup(subscription=testSubscription)
        testJobGroup.create()

        testFileA = File(lfn="/this/is/a/lfnA", size=1024, events=10)
        testFileA.addRun(Run(10, *[12312]))
        testFileA.setLocation('malpaquet')

        testFileB = File(lfn="/this/is/a/lfnB", size=1024, events=10)
        testFileB.addRun(Run(10, *[12312]))
        testFileA.setLocation('malpaquet')
        testFileA.create()
        testFileB.create()

        for _ in range(0, nJobs):
            testJob = Job(name=makeUUID())
            testJob.addFile(testFileA)
            testJob.addFile(testFileB)
            testJob['cache_dir'] = os.path.join(self.testDir, testJob['name'])
            os.mkdir(testJob['cache_dir'])
            testJobGroup.add(testJob)

        testJobGroup.commit()
        if retryOnce:
            self.increaseRetry.execute(testJobGroup.jobs)

        return testJobGroup

    def testA_Create(self):
        """
        WMComponent_t.RetryManager_t.RetryManager_t:testCreate()

        Mimics creation of component and test jobs failed in create stage.
        """
        testJobGroup = self.createTestJobGroup(nJobs=self.nJobs)

        config = self.getConfig()
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'createfailed', 'new')
        changer.propagate(testJobGroup.jobs, 'createcooloff', 'createfailed')

        idList = self.getJobs.execute(state='CreateCooloff')
        self.assertEqual(len(idList), self.nJobs)

        testRetryManager = RetryManagerPoller(config)
        testRetryManager.setup(None)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID=job["id"],
                                    stateTime=int(time.time()) - 50)

        testRetryManager.algorithm(None)
        idList = self.getJobs.execute(state='CreateCooloff')
        self.assertEqual(len(idList), self.nJobs)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID=job["id"],
                                    stateTime=int(time.time()) - 150)

        testRetryManager.algorithm(None)

        idList = self.getJobs.execute(state='CreateCooloff')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state='Created')
        self.assertEqual(len(idList), self.nJobs)
        return

    def testB_Submit(self):
        """
        WMComponent_t.RetryManager_t.RetryManager_t:testSubmit()

        Mimics creation of component and test jobs failed in create stage.
        """
        testJobGroup = self.createTestJobGroup(nJobs=self.nJobs)

        config = self.getConfig()
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'submitfailed', 'created')
        changer.propagate(testJobGroup.jobs, 'submitcooloff', 'submitfailed')

        idList = self.getJobs.execute(state='SubmitCooloff')
        self.assertEqual(len(idList), self.nJobs)

        testRetryManager = RetryManagerPoller(config)
        testRetryManager.setup(None)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID=job["id"],
                                    stateTime=int(time.time()) - 50)

        testRetryManager.algorithm(None)

        idList = self.getJobs.execute(state='SubmitCooloff')
        self.assertEqual(len(idList), self.nJobs)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID=job["id"],
                                    stateTime=int(time.time()) - 150)

        testRetryManager.algorithm(None)

        idList = self.getJobs.execute(state='SubmitCooloff')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state='Created')
        self.assertEqual(len(idList), self.nJobs)
        return

    def testC_Job(self):
        """
        WMComponent_t.RetryManager_t.RetryManager_t:testJob()

        Mimics creation of component and test jobs failed in create stage.
        """
        testJobGroup = self.createTestJobGroup(nJobs=self.nJobs)

        config = self.getConfig()
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup.jobs, 'jobfailed', 'complete')
        changer.propagate(testJobGroup.jobs, 'jobcooloff', 'jobfailed')

        idList = self.getJobs.execute(state='JobCooloff')
        self.assertEqual(len(idList), self.nJobs)

        testRetryManager = RetryManagerPoller(config)
        testRetryManager.setup(None)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID=job["id"],
                                    stateTime=int(time.time()) - 50)

        testRetryManager.algorithm(None)

        idList = self.getJobs.execute(state='JobCooloff')
        self.assertEqual(len(idList), self.nJobs)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID=job["id"],
                                    stateTime=int(time.time()) - 150)

        testRetryManager.algorithm(None)

        idList = self.getJobs.execute(state='JobCooloff')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state='Created')
        self.assertEqual(len(idList), self.nJobs)
        return

    def testD_SquaredAlgo(self):
        """
        _testSquaredAlgo_

        Test the squared algorithm to make sure it loads and works
        """

        testJobGroup = self.createTestJobGroup(nJobs=self.nJobs)

        config = self.getConfig()
        config.RetryManager.plugins = {'Processing': 'SquaredAlgo'}
        config.RetryManager.section_("SquaredAlgo")
        config.RetryManager.SquaredAlgo.section_("Processing")
        config.RetryManager.SquaredAlgo.Processing.coolOffTime = {'create': 10, 'submit': 10, 'job': 10}
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'submitfailed', 'created')
        changer.propagate(testJobGroup.jobs, 'submitcooloff', 'submitfailed')
        changer.propagate(testJobGroup.jobs, 'created', 'submitcooloff')
        changer.propagate(testJobGroup.jobs, 'submitfailed', 'created')
        changer.propagate(testJobGroup.jobs, 'submitcooloff', 'submitfailed')

        idList = self.getJobs.execute(state='SubmitCooloff')
        self.assertEqual(len(idList), self.nJobs)

        testRetryManager = RetryManagerPoller(config)
        testRetryManager.setup(None)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID=job["id"],
                                    stateTime=int(time.time()) - 5)

        testRetryManager.algorithm(None)
        idList = self.getJobs.execute(state='SubmitCooloff')
        self.assertEqual(len(idList), self.nJobs)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID=job["id"],
                                    stateTime=int(time.time()) - 12)

        testRetryManager.algorithm(None)

        idList = self.getJobs.execute(state='SubmitCooloff')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state='Created')
        self.assertEqual(len(idList), self.nJobs)

    def testE_ExponentialAlgo(self):
        """
        _testExponentialAlgo_

        Test the exponential algorithm to make sure it loads and works
        """

        testJobGroup = self.createTestJobGroup(nJobs=self.nJobs)

        config = self.getConfig()
        config.RetryManager.plugins = {'Processing': 'ExponentialAlgo'}
        config.RetryManager.section_("ExponentialAlgo")
        config.RetryManager.ExponentialAlgo.section_("Processing")
        config.RetryManager.ExponentialAlgo.Processing.coolOffTime = {'create': 10, 'submit': 10, 'job': 10}
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'submitfailed', 'created')
        changer.propagate(testJobGroup.jobs, 'submitcooloff', 'submitfailed')
        changer.propagate(testJobGroup.jobs, 'created', 'submitcooloff')
        changer.propagate(testJobGroup.jobs, 'submitfailed', 'created')
        changer.propagate(testJobGroup.jobs, 'submitcooloff', 'submitfailed')

        idList = self.getJobs.execute(state='SubmitCooloff')
        self.assertEqual(len(idList), self.nJobs)

        testRetryManager = RetryManagerPoller(config)
        testRetryManager.setup(None)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID=job["id"],
                                    stateTime=int(time.time()) - 5)

        testRetryManager.algorithm(None)
        idList = self.getJobs.execute(state='SubmitCooloff')
        self.assertEqual(len(idList), self.nJobs)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID=job["id"],
                                    stateTime=int(time.time()) - 12)

        testRetryManager.algorithm(None)

        idList = self.getJobs.execute(state='SubmitCooloff')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state='Created')
        self.assertEqual(len(idList), self.nJobs)

    def testF_LinearAlgo(self):
        """
        _testLinearAlgo_

        Test the linear algorithm to make sure it loads and works
        """

        testJobGroup = self.createTestJobGroup(nJobs=self.nJobs)

        config = self.getConfig()
        config.RetryManager.plugins = {'Processing': 'LinearAlgo'}
        config.RetryManager.section_("LinearAlgo")
        config.RetryManager.LinearAlgo.section_("Processing")
        config.RetryManager.LinearAlgo.Processing.coolOffTime = {'create': 10, 'submit': 10, 'job': 10}
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'submitfailed', 'created')
        changer.propagate(testJobGroup.jobs, 'submitcooloff', 'submitfailed')
        changer.propagate(testJobGroup.jobs, 'created', 'submitcooloff')
        changer.propagate(testJobGroup.jobs, 'submitfailed', 'created')
        changer.propagate(testJobGroup.jobs, 'submitcooloff', 'submitfailed')

        idList = self.getJobs.execute(state='SubmitCooloff')
        self.assertEqual(len(idList), self.nJobs)

        testRetryManager = RetryManagerPoller(config)
        testRetryManager.setup(None)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID=job["id"],
                                    stateTime=int(time.time()) - 5)

        testRetryManager.algorithm(None)
        idList = self.getJobs.execute(state='SubmitCooloff')
        self.assertEqual(len(idList), self.nJobs)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID=job["id"],
                                    stateTime=int(time.time()) - 12)

        testRetryManager.algorithm(None)

        idList = self.getJobs.execute(state='SubmitCooloff')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state='Created')
        self.assertEqual(len(idList), self.nJobs)

        return

    def testG_ProcessingAlgo(self):
        """
        _ProcessingAlgo_

        Test for the ProcessingAlgo Prototype
        """

        testJobGroup = self.createTestJobGroup(nJobs=self.nJobs)

        config = self.getConfig()
        config.RetryManager.plugins = {'Processing': 'ProcessingAlgo'}
        config.RetryManager.section_("ProcessingAlgo")
        config.RetryManager.ProcessingAlgo.section_("default")
        config.RetryManager.ProcessingAlgo.default.coolOffTime = {'create': 10, 'submit': 10, 'job': 10}
        changer = ChangeState(config)
        fwjrPath = os.path.join(WMCore.WMBase.getTestBase(),
                                "WMComponent_t/JobAccountant_t",
                                "fwjrs/badBackfillJobReport.pkl")
        report = Report()
        report.load(fwjrPath)
        for job in testJobGroup.jobs:
            job['fwjr'] = report
            job['retry_count'] = 0
            report.save(os.path.join(job['cache_dir'], "Report.%i.pkl" % job['retry_count']))
        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup.jobs, 'jobfailed', 'complete')
        changer.propagate(testJobGroup.jobs, 'jobcooloff', 'jobfailed')

        testRetryManager = RetryManagerPoller(config)
        testRetryManager.algorithm()

        idList = self.getJobs.execute(state='Created')
        self.assertEqual(len(idList), self.nJobs)

        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup.jobs, 'jobfailed', 'complete')
        changer.propagate(testJobGroup.jobs, 'jobcooloff', 'jobfailed')

        for job in testJobGroup.jobs:
            j = Job(id=job['id'])
            j.load()
            self.assertEqual(j['retry_count'], 1)
            report.save(os.path.join(j['cache_dir'], "Report.%i.pkl" % j['retry_count']))

        config.RetryManager.ProcessingAlgo.default.OneMoreErrorCodes = [8020]
        testRetryManager2 = RetryManagerPoller(config)
        testRetryManager2.algorithm()

        idList = self.getJobs.execute(state='Created')
        self.assertEqual(len(idList), self.nJobs)

        for job in testJobGroup.jobs:
            j = Job(id=job['id'])
            j.load()
            self.assertEqual(j['retry_count'], 5)

        # Now test timeout
        testJobGroup2 = self.createTestJobGroup(nJobs=self.nJobs)

        # Cycle jobs
        for job in testJobGroup2.jobs:
            job['fwjr'] = report
            job['retry_count'] = 0
            report.save(os.path.join(job['cache_dir'], "Report.%i.pkl" % job['retry_count']))
        changer.propagate(testJobGroup2.jobs, 'created', 'new')
        changer.propagate(testJobGroup2.jobs, 'executing', 'created')
        changer.propagate(testJobGroup2.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup2.jobs, 'jobfailed', 'complete')
        changer.propagate(testJobGroup2.jobs, 'jobcooloff', 'jobfailed')

        for job in testJobGroup2.jobs:
            j = Job(id=job['id'])
            j.load()
            self.assertEqual(j['retry_count'], 0)

        config.RetryManager.ProcessingAlgo.default.OneMoreErrorCodes = []
        config.RetryManager.ProcessingAlgo.default.MaxRunTime = 1
        testRetryManager3 = RetryManagerPoller(config)
        testRetryManager3.algorithm()

        idList = self.getJobs.execute(state='Created')
        self.assertEqual(len(idList), self.nJobs * 2)

        for job in testJobGroup2.jobs:
            j = Job(id=job['id'])
            j.load()
            self.assertEqual(j['retry_count'], 5)

        return

    def testH_PauseAlgo(self):
        """
        _testH_PauseAlgo_

        Test the pause algorithm, note that given pauseCount = n, the
        job will run first n + 1 times before being paused.
        After that it will be paused each n times
        """

        testJobGroup = self.createTestJobGroup(nJobs=self.nJobs)

        # adding a 2nd job group
        testJobGroup2 = self.createTestJobGroup(nJobs=self.nJobs)

        config = self.getConfig()
        config.RetryManager.plugins = {'Processing': 'PauseAlgo'}
        config.RetryManager.section_("PauseAlgo")
        config.RetryManager.PauseAlgo.section_("Processing")
        config.RetryManager.PauseAlgo.Processing.coolOffTime = {'create': 20, 'submit': 20, 'job': 20}
        config.RetryManager.PauseAlgo.Processing.pauseCount = 2
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'jobfailed', 'executing')
        changer.propagate(testJobGroup.jobs, 'jobcooloff', 'jobfailed')
        changer.propagate(testJobGroup.jobs, 'created', 'jobcooloff')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'jobfailed', 'executing')
        changer.propagate(testJobGroup.jobs, 'jobcooloff', 'jobfailed')

        testRetryManager = RetryManagerPoller(config)
        testRetryManager.setup(None)

        report = Report()

        # Making sure that jobs are not created ahead of time
        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID=job["id"],
                                    stateTime=int(time.time()) - 15)
        testRetryManager.algorithm(None)
        idList = self.getJobs.execute(state='JobCoolOff')
        self.assertEqual(len(idList), self.nJobs)

        # Giving time so they can be retried
        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID=job["id"],
                                    stateTime=int(time.time()) - 25)

        # Make sure that the plugin allowed them to go back to created state
        testRetryManager.algorithm(None)
        idList = self.getJobs.execute(state='created')
        self.assertEqual(len(idList), self.nJobs)

        # Fail them out again
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'jobfailed', 'executing')
        changer.propagate(testJobGroup.jobs, 'jobcooloff', 'jobfailed')

        # Make sure that no change happens before timeout
        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID=job["id"],
                                    stateTime=int(time.time()) - 75)
        testRetryManager.algorithm(None)
        idList = self.getJobs.execute(state='JobCoolOff')
        self.assertEqual(len(idList), self.nJobs)

        # Giving time so they can be paused
        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID=job["id"],
                                    stateTime=int(time.time()) - 85)

        # Make sure that the plugin pauses them
        testRetryManager.algorithm(None)
        idList = self.getJobs.execute(state='jobpaused')
        self.assertEqual(len(idList), self.nJobs)

        # Emulating ops retrying the job
        changer.propagate(testJobGroup.jobs, 'created', 'jobpaused')

        # Making sure it did the right thing
        idList = self.getJobs.execute(state='created')
        self.assertEqual(len(idList), self.nJobs)

        # Fail them out again
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'jobfailed', 'executing')
        changer.propagate(testJobGroup.jobs, 'jobfailed', 'executing')
        changer.propagate(testJobGroup.jobs, 'jobcooloff', 'jobfailed')

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID=job["id"],
                                    stateTime=int(time.time()) - 175)
        testRetryManager.algorithm(None)
        idList = self.getJobs.execute(state='JobCoolOff')
        self.assertEqual(len(idList), self.nJobs)

        # Giving time so they can be retried
        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID=job["id"],
                                    stateTime=int(time.time()) - 185)

        # Make sure that the plugin allowed them to go back to created state
        testRetryManager.algorithm(None)
        idList = self.getJobs.execute(state='created')
        self.assertEqual(len(idList), self.nJobs)

        # Fail them out again
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'jobfailed', 'executing')
        changer.propagate(testJobGroup.jobs, 'jobfailed', 'executing')
        changer.propagate(testJobGroup.jobs, 'jobcooloff', 'jobfailed')

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID=job["id"],
                                    stateTime=int(time.time()) - 315)
        testRetryManager.algorithm(None)
        idList = self.getJobs.execute(state='jobcooloff')
        self.assertEqual(len(idList), self.nJobs)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID=job["id"],
                                    stateTime=int(time.time()) - 325)

        # Make sure that the plugin allowed them to go back to created state
        testRetryManager.algorithm(None)
        idList = self.getJobs.execute(state='jobpaused')
        self.assertEqual(len(idList), self.nJobs)

        # a configurable retry count per job type {jobExitCodeA: pauseCountB}
        config.RetryManager.PauseAlgo.Processing.retryErrorCodes = {8020: 1, 12345: 1, 5555: 2}

        testRetryManager2 = RetryManagerPoller(config)
        testRetryManager2.algorithm()

        fwjrPath = os.path.join(WMCore.WMBase.getTestBase(),
                                "WMComponent_t/JobAccountant_t",
                                "fwjrs/badBackfillJobReport.pkl")

        report.load(fwjrPath)
        for job in testJobGroup2.jobs:
            job['fwjr'] = report
            job['retry_count'] = 0
            report.save(os.path.join(job['cache_dir'], "Report.%i.pkl" % job['retry_count']))

        # fail the jobs
        changer.propagate(testJobGroup2.jobs, 'created', 'new')
        changer.propagate(testJobGroup2.jobs, 'executing', 'created')
        changer.propagate(testJobGroup2.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup2.jobs, 'jobfailed', 'complete')
        changer.propagate(testJobGroup2.jobs, 'jobcooloff', 'jobfailed')

        # Giving time so they can be paused
        for job in testJobGroup2.jobs:
            self.setJobTime.execute(jobID=job["id"],
                                    stateTime=int(time.time()) - 85)

        # Make sure that the plugin sent those jobs to the next state:
        testRetryManager2.algorithm()
        # job exit code is 8020, so it is supposed to be retried one time.
        # Meaning, that here we should have 10 jobs (from the first part of the test) in jobpaused
        # and 10 jobs in created state

        idList = self.getJobs.execute(state='created')
        self.assertEqual(len(idList), self.nJobs)

        idList2 = self.getJobs.execute(state='jobpaused')
        self.assertEqual(len(idList2), self.nJobs)

        # save a second job report - with a retry count = 1
        for job in testJobGroup2.jobs:
            j = Job(id=job['id'])
            j.load()
            j['retry_count'] = 1
            self.assertEqual(j['retry_count'], 1)
            report.save(os.path.join(j['cache_dir'], "Report.%i.pkl" % j['retry_count']))

        # Fail them out again
        changer.propagate(testJobGroup2.jobs, 'executing', 'created')
        changer.propagate(testJobGroup2.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup2.jobs, 'jobfailed', 'complete')
        changer.propagate(testJobGroup2.jobs, 'jobcooloff', 'jobfailed')

        for job in testJobGroup2.jobs:
            self.setJobTime.execute(jobID=job["id"],
                                    stateTime=int(time.time()) - 175)

        # not sure if this check is needed:
        idList = self.getJobs.execute(state='jobcooloff')
        self.assertEqual(len(idList), self.nJobs)

        # Giving time so they can be paused
        for job in testJobGroup2.jobs:
            self.setJobTime.execute(jobID=job["id"],
                                    stateTime=int(time.time()) - 85)

        # Make sure that the plugin sent those jobs to paused state:
        testRetryManager2.algorithm(None)
        idList = self.getJobs.execute(state='jobpaused')
        # And again, in total, there should be 10+10=20 jobs in jobpaused
        self.assertEqual(len(idList), self.nJobs * 2)

        return

    def testI_MultipleJobTypes(self):
        """
        _testI_MultipleJobTypes_

        Check that we can configure different retry algorithms for different
        job types, including a default for nonspecified types.
        Also check that two job types can share the same retry algorithm
        but with different parameters
        """

        # Let's create 4 job groups
        processingJobGroup = self.createTestJobGroup(nJobs=10,
                                                     retryOnce=True)
        productionJobGroup = self.createTestJobGroup(nJobs=15,
                                                     subType="Production",
                                                     retryOnce=True)
        mergeJobGroup = self.createTestJobGroup(nJobs=20,
                                                subType="Merge",
                                                retryOnce=True)
        skimJobGroup = self.createTestJobGroup(nJobs=5,
                                               subType="Skim",
                                               retryOnce=True)

        # Set an adequate config
        # Processing jobs get the PauseAlgo with pauseCount 4
        # Production jobs get the ExponentialAlgo
        # Merge jobs get the PauseAlgo but with pauseCount 2 which is the default
        # Skim jobs are not configured, so they get the default SquaredAlgo
        config = self.getConfig()
        config.RetryManager.plugins = {'Processing': 'PauseAlgo',
                                       'Production': 'ExponentialAlgo',
                                       'Merge': 'PauseAlgo',
                                       'default': 'SquaredAlgo'}
        config.RetryManager.section_("PauseAlgo")
        config.RetryManager.PauseAlgo.section_("Processing")
        config.RetryManager.PauseAlgo.Processing.coolOffTime = {'create': 30, 'submit': 30, 'job': 30}
        config.RetryManager.PauseAlgo.Processing.pauseCount = 4
        config.RetryManager.PauseAlgo.section_("default")
        config.RetryManager.PauseAlgo.default.coolOffTime = {'create': 60, 'submit': 60, 'job': 60}
        config.RetryManager.PauseAlgo.default.pauseCount = 2
        config.RetryManager.section_("ExponentialAlgo")
        config.RetryManager.ExponentialAlgo.section_("Production")
        config.RetryManager.ExponentialAlgo.Production.coolOffTime = {'create': 30, 'submit': 30, 'job': 30}
        config.RetryManager.ExponentialAlgo.section_("default")
        config.RetryManager.ExponentialAlgo.default.coolOffTime = {'create': 60, 'submit': 60, 'job': 60}
        config.RetryManager.section_("SquaredAlgo")
        config.RetryManager.SquaredAlgo.section_("Skim")
        config.RetryManager.SquaredAlgo.Skim.coolOffTime = {'create': 30, 'submit': 30, 'job': 30}
        config.RetryManager.SquaredAlgo.section_("default")
        config.RetryManager.SquaredAlgo.default.coolOffTime = {'create': 60, 'submit': 60, 'job': 60}

        # Start the state changer and RetryManager
        changer = ChangeState(config)
        testRetryManager = RetryManagerPoller(config)
        testRetryManager.setup(None)

        # Create the jobs for the first time
        changer.propagate(processingJobGroup.jobs, 'created', 'new')

        # Let's start with the processing jobs and the pauseAlgo
        for count in range(1, 5):
            # Fail the jobs
            changer.propagate(processingJobGroup.jobs, 'executing', 'created')
            changer.propagate(processingJobGroup.jobs, 'jobfailed', 'executing')
            changer.propagate(processingJobGroup.jobs, 'jobcooloff', 'jobfailed')

            # Check  that the cooloff time is strictly enforced
            # First a job time just below the cooloff time
            for job in processingJobGroup.jobs:
                self.setJobTime.execute(jobID=job["id"],
                                        stateTime=int(time.time()) - 30 * pow(count, 2) + 5)
            testRetryManager.algorithm(None)
            idList = self.getJobs.execute(state='JobCoolOff')
            self.assertEqual(len(idList), len(processingJobGroup.jobs),
                             "Jobs went into cooloff without the proper timing")

            # Now above the cooloff time
            for job in processingJobGroup.jobs:
                self.setJobTime.execute(jobID=job["id"],
                                        stateTime=int(time.time()) - 30 * pow(count, 2) - 5)
            testRetryManager.algorithm(None)

            # Make sure the jobs get created again or go to paused
            if count < 4:
                idList = self.getJobs.execute(state='created')
            else:
                idList = self.getJobs.execute(state='jobpaused')
            self.assertEqual(len(idList), len(processingJobGroup.jobs),
                             "Jobs didn't change state correctly")

        # Unpause them so they don't interfere with subsequent tests
        changer.propagate(processingJobGroup.jobs, 'created', 'jobpaused')
        changer.propagate(processingJobGroup.jobs, 'executing', 'created')

        # Now the production jobs and the exponential algo
        changer.propagate(productionJobGroup.jobs, 'created', 'new')

        for count in range(1, 3):
            changer.propagate(productionJobGroup.jobs, 'executing', 'created')
            changer.propagate(productionJobGroup.jobs, 'jobfailed', 'executing')
            changer.propagate(productionJobGroup.jobs, 'jobcooloff', 'jobfailed')

            for job in productionJobGroup.jobs:
                self.setJobTime.execute(jobID=job["id"],
                                        stateTime=int(time.time()) - pow(30, count) + 5)
            testRetryManager.algorithm(None)
            idList = self.getJobs.execute(state='JobCoolOff')
            self.assertEqual(len(idList), len(productionJobGroup.jobs),
                             "Jobs went into cooloff without the proper timing")
            for job in productionJobGroup.jobs:
                self.setJobTime.execute(jobID=job["id"],
                                        stateTime=int(time.time()) - pow(30, count) - 5)
            testRetryManager.algorithm(None)

            idList = self.getJobs.execute(state='created')
            self.assertEqual(len(idList), len(productionJobGroup.jobs),
                             "Jobs didn't change state correctly")

        # Send them to executing
        changer.propagate(productionJobGroup.jobs, 'executing', 'created')

        # Now the merge jobs and the paused algo with different parameters
        changer.propagate(mergeJobGroup.jobs, 'created', 'new')

        for count in range(1, 3):
            changer.propagate(mergeJobGroup.jobs, 'executing', 'created')
            changer.propagate(mergeJobGroup.jobs, 'jobfailed', 'executing')
            changer.propagate(mergeJobGroup.jobs, 'jobcooloff', 'jobfailed')

            for job in mergeJobGroup.jobs:
                self.setJobTime.execute(jobID=job["id"],
                                        stateTime=int(time.time()) - 30 * pow(count, 2) - 5)
            testRetryManager.algorithm(None)
            idList = self.getJobs.execute(state='JobCoolOff')
            self.assertEqual(len(idList), len(mergeJobGroup.jobs),
                             "Jobs went into cooloff without the proper timing")

            for job in mergeJobGroup.jobs:
                self.setJobTime.execute(jobID=job["id"],
                                        stateTime=int(time.time()) - 60 * pow(count, 2) - 5)
            testRetryManager.algorithm(None)

            if count < 2:
                idList = self.getJobs.execute(state='created')
            else:
                idList = self.getJobs.execute(state='jobpaused')
            self.assertEqual(len(idList), len(mergeJobGroup.jobs),
                             "Jobs didn't change state correctly")

        # Send them to executing
        changer.propagate(mergeJobGroup.jobs, 'created', 'jobpaused')
        changer.propagate(mergeJobGroup.jobs, 'executing', 'created')

        # Now the skim jobs and the squared algo
        changer.propagate(skimJobGroup.jobs, 'created', 'new')

        for count in range(1, 3):
            changer.propagate(skimJobGroup.jobs, 'executing', 'created')
            changer.propagate(skimJobGroup.jobs, 'jobfailed', 'executing')
            changer.propagate(skimJobGroup.jobs, 'jobcooloff', 'jobfailed')

            for job in skimJobGroup.jobs:
                self.setJobTime.execute(jobID=job["id"],
                                        stateTime=int(time.time()) - 30 * pow(count, 2) + 5)
            testRetryManager.algorithm(None)
            idList = self.getJobs.execute(state='JobCoolOff')
            self.assertEqual(len(idList), len(skimJobGroup.jobs),
                             "Jobs went into cooloff without the proper timing")
            for job in skimJobGroup.jobs:
                self.setJobTime.execute(jobID=job["id"],
                                        stateTime=int(time.time()) - 30 * pow(count, 2) - 5)
            testRetryManager.algorithm(None)

            idList = self.getJobs.execute(state='created')
            self.assertEqual(len(idList), len(skimJobGroup.jobs),
                             "Jobs didn't change state correctly")

    def testY_MultipleIterations(self):
        """
        _MultipleIterations_

        Paranoia based check to see if I'm saving class instances correctly
        """

        testJobGroup = self.createTestJobGroup(nJobs=self.nJobs)

        config = self.getConfig()
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'submitfailed', 'Created')
        changer.propagate(testJobGroup.jobs, 'submitcooloff', 'submitfailed')

        idList = self.getJobs.execute(state='SubmitCooloff')
        self.assertEqual(len(idList), self.nJobs)

        testRetryManager = RetryManagerPoller(config)
        testRetryManager.setup(None)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID=job["id"],
                                    stateTime=int(time.time()) - 50)

        testRetryManager.algorithm(None)
        idList = self.getJobs.execute(state='SubmitCooloff')
        self.assertEqual(len(idList), self.nJobs)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID=job["id"],
                                    stateTime=int(time.time()) - 150)

        testRetryManager.algorithm(None)

        idList = self.getJobs.execute(state='SubmitCooloff')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state='Created')
        self.assertEqual(len(idList), self.nJobs)

        # Make a new jobGroup for a second run
        testJobGroup = self.createTestJobGroup(nJobs=self.nJobs)

        # Set job state
        changer.propagate(testJobGroup.jobs, 'submitfailed', 'created')
        changer.propagate(testJobGroup.jobs, 'submitcooloff', 'submitfailed')

        # Set them to go off
        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID=job["id"],
                                    stateTime=int(time.time()) - 200)

        testRetryManager.algorithm(None)

        idList = self.getJobs.execute(state='SubmitCooloff')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state='Created')
        self.assertEqual(len(idList), self.nJobs * 2)

        return

    @attr('integration')
    def testZ_Profile(self):
        """
        _Profile_

        Do a basic profiling of the algo
        """

        import pstats

        nJobs = 1000

        testJobGroup = self.createTestJobGroup(nJobs=nJobs)

        config = self.getConfig()
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'createfailed', 'new')
        changer.propagate(testJobGroup.jobs, 'createcooloff', 'createfailed')

        idList = self.getJobs.execute(state='CreateCooloff')
        self.assertEqual(len(idList), nJobs)

        testRetryManager = RetryManagerPoller(config)
        testRetryManager.setup(None)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID=job["id"],
                                    stateTime=int(time.time()) - 50)

        testRetryManager.algorithm(None)
        idList = self.getJobs.execute(state='CreateCooloff')
        self.assertEqual(len(idList), nJobs)

        for job in testJobGroup.jobs:
            self.setJobTime.execute(jobID=job["id"],
                                    stateTime=int(time.time()) - 150)

        startTime = time.time()
        # cProfile.runctx("testRetryManager.algorithm()", globals(), locals(), filename = "profStats.stat")
        testRetryManager.algorithm(None)
        stopTime = time.time()

        idList = self.getJobs.execute(state='CreateCooloff')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state='New')
        self.assertEqual(len(idList), nJobs)

        print("Took %f seconds to run polling algo" % (stopTime - startTime))

        p = pstats.Stats('profStats.stat')
        p.sort_stats('cumulative')
        p.print_stats(0.2)

        return


if __name__ == '__main__':
    unittest.main()
