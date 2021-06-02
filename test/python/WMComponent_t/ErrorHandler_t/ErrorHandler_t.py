#!/usr/bin/env python
"""
ErrorHandler test TestErrorHandler module and the harness
"""
from __future__ import print_function

from builtins import range
import cProfile
import os
import os.path
import pstats
import threading
import time
import unittest

from Utils.PythonVersion import PY3

from WMCore_t.WMSpec_t.TestSpec import testWorkload
from nose.plugins.attrib import attr

import WMCore.WMBase
from WMComponent.ErrorHandler.ErrorHandlerPoller import ErrorHandlerPoller
from WMCore.ACDC.DataCollectionService import DataCollectionService
from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.Run import Run
from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.Services.UUIDLib import makeUUID
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMSpec.Makers.TaskMaker import TaskMaker
from WMQuality.Emulators import EmulatorSetup
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMQuality.TestInitCouchApp import TestInitCouchApp


class ErrorHandlerTest(EmulatedUnitTestCase):
    """
    TestCase for TestErrorHandler module
    """

    def setUp(self):
        """
        setup for test.
        """
        super(ErrorHandlerTest, self).setUp()
        myThread = threading.currentThread()

        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules=["WMCore.WMBS"],
                                useDefault=False)
        self.testInit.setupCouch("errorhandler_t", "GroupUser", "ACDC")
        self.testInit.setupCouch("errorhandler_t_jd/jobs", "JobDump")
        self.testInit.setupCouch("errorhandler_t_jd/fwjrs", "FWJRDump")

        self.daofactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)
        self.getJobs = self.daofactory(classname="Jobs.GetAllJobs")
        self.setJobTime = self.daofactory(classname="Jobs.SetStateTime")
        locationAction = self.daofactory(classname="Locations.New")
        locationAction.execute(siteName="malpaquet", pnn="T2_CH_CERN")
        self.testDir = self.testInit.generateWorkDir()
        self.configFile = EmulatorSetup.setupWMAgentConfig()
        self.nJobs = 10

        self.dataCS = DataCollectionService(url=self.testInit.couchUrl,
                                            database="errorhandler_t")

        if PY3:
            self.assertItemsEqual = self.assertCountEqual

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

        config.component_("ErrorHandler")
        # The log level of the component.
        config.ErrorHandler.logLevel = 'INFO'
        # The namespace of the component
        config.ErrorHandler.namespace = 'WMComponent.ErrorHandler.ErrorHandler'
        # maximum number of threads we want to deal
        # config.ErrorHandler.maxThreads = 30
        # with messages per pool.
        config.ErrorHandler.maxProcessSize = 30
        config.ErrorHandler.readFWJR = True
        # maximum number of retries we want for job
        config.ErrorHandler.maxRetries = 5
        # The poll interval at which to look for failed jobs
        config.ErrorHandler.pollInterval = 60
        # this will be overwritten in some unittests
        # JobStateMachine
        config.component_('JobStateMachine')
        config.JobStateMachine.couchurl = os.getenv('COUCHURL', None)
        config.JobStateMachine.couchDBName = "errorhandler_t_jd"

        config.section_('ACDC')
        config.ACDC.couchurl = self.testInit.couchUrl
        config.ACDC.database = "errorhandler_t"

        return config

    def createWorkload(self, workloadName='Test'):
        """
        _createTestWorkload_

        Creates a test workload for us to run on, hold the basic necessities.
        """

        workload = testWorkload(workloadName)

        # Add RequestManager stuff
        workload.data.request.section_('schema')
        workload.data.request.schema.Requestor = 'nobody'
        workload.data.request.schema.Group = 'testers'

        taskMaker = TaskMaker(workload, os.path.join(self.testDir, 'workloadTest'))
        taskMaker.skipSubscription = True
        taskMaker.processWorkload()

        return workload

    def createTestJobGroup(self, nJobs=10, retry_count=1,
                           workloadPath='test', fwjrPath=None,
                           workloadName=makeUUID(),
                           fileModifier=''):
        """
        Creates a group of several jobs
        """

        myThread = threading.currentThread()
        myThread.transaction.begin()
        testWorkflow = Workflow(spec=workloadPath, owner="cmsdataops", group="cmsdataops",
                                name=workloadName, task="/TestWorkload/ReReco")
        testWorkflow.create()

        testWMBSFileset = Fileset(name="TestFileset")
        testWMBSFileset.create()

        testSubscription = Subscription(fileset=testWMBSFileset,
                                        workflow=testWorkflow)
        testSubscription.create()

        testJobGroup = JobGroup(subscription=testSubscription)
        testJobGroup.create()

        testFile0 = File(lfn="/this/is/a/parent%s" % fileModifier, size=1024, events=10)
        testFile0.addRun(Run(10, *[12312]))
        testFile0.setLocation('T2_CH_CERN')

        testFileA = File(lfn="/this/is/a/lfnA%s" % fileModifier, size=1024, events=10,
                         first_event=88, merged=False)
        testFileA.addRun(Run(10, *[12312, 12313]))
        testFileA.setLocation('T2_CH_CERN')

        testFileB = File(lfn="/this/is/a/lfnB%s" % fileModifier, size=1024, events=10,
                         first_event=88, merged=False)
        testFileB.addRun(Run(10, *[12314, 12315, 12316]))
        testFileB.setLocation('T2_CH_CERN')

        testFile0.create()
        testFileA.create()
        testFileB.create()

        testFileA.addParent(lfn="/this/is/a/parent%s" % fileModifier)
        testFileB.addParent(lfn="/this/is/a/parent%s" % fileModifier)

        for i in range(0, nJobs):
            testJob = Job(name=makeUUID())
            testJob['retry_count'] = retry_count
            testJob['retry_max'] = 10
            testJob['mask'].addRunAndLumis(run=10, lumis=[12312])
            testJob['mask'].addRunAndLumis(run=10, lumis=[12314, 12316])
            testJob['cache_dir'] = os.path.join(self.testDir, testJob['name'])
            testJob['fwjr_path'] = fwjrPath
            os.mkdir(testJob['cache_dir'])
            testJobGroup.add(testJob)
            testJob.create(group=testJobGroup)
            testJob.addFile(testFileA)
            testJob.addFile(testFileB)
            testJob.save()

        testJobGroup.commit()

        testSubscription.acquireFiles(files=[testFileA, testFileB])
        testSubscription.save()
        myThread.transaction.commit()

        return testJobGroup

    def testA_Create(self):
        """
        WMComponent_t.ErrorHandler_t.ErrorHandler_t:testCreate()

        Mimics creation of component and test jobs failed in create stage.
        """
        njobs = 4
        workloadName = 'TestWorkload'

        self.createWorkload(workloadName=workloadName)
        workloadPath = os.path.join(self.testDir, 'workloadTest', workloadName,
                                    'WMSandbox', 'WMWorkload.pkl')

        #        testJobGroup = self.createTestJobGroup(nJobs=self.nJobs,
        testJobGroup = self.createTestJobGroup(nJobs=njobs,
                                               workloadPath=workloadPath,
                                               workloadName=workloadName)
        config = self.getConfig()
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'createfailed', 'created')

        idList = self.getJobs.execute(state='CreateFailed')
        self.assertEqual(len(idList), njobs)

        testErrorHandler = ErrorHandlerPoller(config)
        # set reqAuxDB None for the test,
        testErrorHandler.reqAuxDB = None
        testErrorHandler.setup(None)
        testErrorHandler.algorithm(None)

        idList = self.getJobs.execute(state='CreateFailed')
        self.assertEqual(len(idList), 0)

        # These should go directly to exhausted
        idList = self.getJobs.execute(state='Exhausted')
        self.assertEqual(len(idList), njobs)

        # Check that it showed up in ACDC
        collection = self.dataCS.getDataCollection(workloadName)

        # Now look at what's inside
        self.assertTrue(len(collection['filesets']) > 0)
        for fileset in collection["filesets"]:
            counter = 0
            for f in fileset.listFiles():
                counter += 1
                self.assertTrue(f['lfn'] in ["/this/is/a/lfnA", "/this/is/a/lfnB"])
                self.assertEqual(f['events'], 10)
                self.assertEqual(f['size'], 1024)
                self.assertEqual(f['parents'], [u'/this/is/a/parent'])
                self.assertTrue(f['runs'][0]['run_number'] == 10)
                if f['lfn'] == "/this/is/a/lfnA":
                    self.assertItemsEqual(f['runs'][0]['lumis'], [12312])
                elif f['lfn'] == "/this/is/a/lfnB":
                    self.assertItemsEqual(f['runs'][0]['lumis'], [12314, 12315, 12316])
                else:
                    self.assertFail("File name is not known: %s" % f['lfn'])
                self.assertEqual(f['merged'], 0)
                self.assertEqual(f['first_event'], 88)
            self.assertEqual(counter, njobs * 2)  # each job has 2 files (thus 4 times duplicate)
        return

    def testB_Submit(self):
        """
        WMComponent_t.ErrorHandler_t.ErrorHandler_t:testSubmit()

        Mimics creation of component and test jobs failed in submit stage.
        """
        workloadName = 'TestWorkload'

        self.createWorkload(workloadName=workloadName)
        workloadPath = os.path.join(self.testDir, 'workloadTest', workloadName,
                                    'WMSandbox', 'WMWorkload.pkl')

        testJobGroup = self.createTestJobGroup(nJobs=self.nJobs,
                                               workloadPath=workloadPath)

        config = self.getConfig()
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'submitfailed', 'created')

        idList = self.getJobs.execute(state='SubmitFailed')
        self.assertEqual(len(idList), self.nJobs)

        testErrorHandler = ErrorHandlerPoller(config)
        # set reqAuxDB None for the test,
        testErrorHandler.reqAuxDB = None
        testErrorHandler.setup(None)
        testErrorHandler.algorithm(None)

        idList = self.getJobs.execute(state='SubmitFailed')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state='SubmitCooloff')
        self.assertEqual(len(idList), self.nJobs)
        return

    def testC_Jobs(self):
        """
        WMComponent_t.ErrorHandler_t.ErrorHandler_t.testJobs()

        Mimics creation of component and test jobs failed in execute stage.
        """
        workloadName = 'TestWorkload'

        self.createWorkload(workloadName=workloadName)
        workloadPath = os.path.join(self.testDir, 'workloadTest', workloadName,
                                    'WMSandbox', 'WMWorkload.pkl')

        testJobGroup = self.createTestJobGroup(nJobs=self.nJobs,
                                               workloadPath=workloadPath)

        config = self.getConfig()
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup.jobs, 'jobfailed', 'complete')

        idList = self.getJobs.execute(state='JobFailed')
        self.assertEqual(len(idList), self.nJobs)

        testErrorHandler = ErrorHandlerPoller(config)
        # set reqAuxDB None for the test,
        testErrorHandler.reqAuxDB = None
        testErrorHandler.setup(None)
        testErrorHandler.algorithm(None)

        idList = self.getJobs.execute(state='JobFailed')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state='JobCooloff')
        self.assertEqual(len(idList), self.nJobs)
        return

    def testD_Exhausted(self):
        """
        _testExhausted_

        Test that the system can exhaust jobs correctly
        """
        workloadName = 'TestWorkload'

        self.createWorkload(workloadName=workloadName)
        workloadPath = os.path.join(self.testDir, 'workloadTest', workloadName,
                                    'WMSandbox', 'WMWorkload.pkl')

        testJobGroup = self.createTestJobGroup(nJobs=self.nJobs, retry_count=5,
                                               workloadPath=workloadPath)

        config = self.getConfig()
        config.ErrorHandler.maxRetries = 1
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup.jobs, 'jobfailed', 'complete')

        testSubscription = Subscription(id=1)  # You should only have one
        testSubscription.load()
        testSubscription.loadData()

        # Do we have files to start with?
        self.assertEqual(len(testSubscription.filesOfStatus("Acquired")), 2)

        testErrorHandler = ErrorHandlerPoller(config)
        # set reqAuxDB None for the test,
        testErrorHandler.reqAuxDB = None
        testErrorHandler.setup(None)
        testErrorHandler.algorithm(None)

        idList = self.getJobs.execute(state='JobFailed')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state='JobCooloff')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state='Exhausted')
        self.assertEqual(len(idList), self.nJobs)

        # Did we fail the files?
        self.assertEqual(len(testSubscription.filesOfStatus("Acquired")), 0)
        self.assertEqual(len(testSubscription.filesOfStatus("Failed")), 2)

    def testE_FailJobs(self):
        """
        _FailJobs_

        Test our ability to fail jobs based on the information in the FWJR
        """
        workloadName = 'TestWorkload'

        self.createWorkload(workloadName=workloadName)
        workloadPath = os.path.join(self.testDir, 'workloadTest', workloadName,
                                    'WMSandbox', 'WMWorkload.pkl')

        fwjrPath = os.path.join(WMCore.WMBase.getTestBase(),
                                "WMComponent_t/JobAccountant_t",
                                "fwjrs/badBackfillJobReport.pkl")

        testJobGroup = self.createTestJobGroup(nJobs=self.nJobs,
                                               workloadPath=workloadPath,
                                               fwjrPath=fwjrPath)

        badJobGroup = self.createTestJobGroup(nJobs=self.nJobs,
                                              workloadPath=workloadPath,
                                              fwjrPath=None,
                                              fileModifier='bad')

        config = self.getConfig()
        config.ErrorHandler.readFWJR = True
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup.jobs, 'jobfailed', 'complete')
        changer.propagate(badJobGroup.jobs, 'created', 'new')
        changer.propagate(badJobGroup.jobs, 'executing', 'created')
        changer.propagate(badJobGroup.jobs, 'complete', 'executing')
        changer.propagate(badJobGroup.jobs, 'jobfailed', 'complete')

        testErrorHandler = ErrorHandlerPoller(config)
        # set reqAuxDB None for the test,
        testErrorHandler.reqAuxDB = None
        testErrorHandler.setup(None)
        testErrorHandler.exitCodesNoRetry = [8020]
        testErrorHandler.algorithm(None)

        # This should exhaust all jobs due to exit code
        # Except those with no fwjr
        idList = self.getJobs.execute(state='JobFailed')
        self.assertEqual(len(idList), 0)
        idList = self.getJobs.execute(state='JobCooloff')
        self.assertEqual(len(idList), self.nJobs)
        idList = self.getJobs.execute(state='Exhausted')
        self.assertEqual(len(idList), self.nJobs)

        config.ErrorHandler.maxFailTime = -10
        testErrorHandler2 = ErrorHandlerPoller(config)
        # set reqAuxDB None for the test,
        testErrorHandler2.reqAuxDB = None

        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup.jobs, 'jobfailed', 'complete')

        testErrorHandler2.algorithm(None)

        # This should exhaust all jobs due to timeout
        idList = self.getJobs.execute(state='JobFailed')
        self.assertEqual(len(idList), 0)
        idList = self.getJobs.execute(state='JobCooloff')
        self.assertEqual(len(idList), self.nJobs)
        idList = self.getJobs.execute(state='Exhausted')
        self.assertEqual(len(idList), self.nJobs)

        config.ErrorHandler.maxFailTime = 24 * 3600
        config.ErrorHandler.passExitCodes = [8020]
        testErrorHandler3 = ErrorHandlerPoller(config)
        # set reqAuxDB None for the test,
        testErrorHandler3.reqAuxDB = None

        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup.jobs, 'jobfailed', 'complete')

        testErrorHandler3.algorithm(None)

        # This should pass all jobs due to exit code
        idList = self.getJobs.execute(state='Created')
        self.assertEqual(len(idList), self.nJobs)

        return

    @attr('integration')
    def testZ_Profile(self):
        """
        _testProfile_

        Do a full profile of the poller
        """

        nJobs = 100
        workloadName = 'TestWorkload'
        self.createWorkload(workloadName=workloadName)
        workloadPath = os.path.join(self.testDir, 'workloadTest', workloadName,
                                    'WMSandbox', 'WMWorkload.pkl')

        testJobGroup = self.createTestJobGroup(nJobs=nJobs, workloadPath=workloadPath)

        config = self.getConfig()
        changer = ChangeState(config)
        changer.propagate(testJobGroup.jobs, 'created', 'new')
        changer.propagate(testJobGroup.jobs, 'executing', 'created')
        changer.propagate(testJobGroup.jobs, 'complete', 'executing')
        changer.propagate(testJobGroup.jobs, 'jobfailed', 'complete')

        idList = self.getJobs.execute(state='JobFailed')
        self.assertEqual(len(idList), nJobs)

        testErrorHandler = ErrorHandlerPoller(config)
        # set reqAuxDB None for the test,
        testErrorHandler.reqAuxDB = None
        testErrorHandler.setup(None)

        startTime = time.time()
        cProfile.runctx("testErrorHandler.algorithm()", globals(), locals(), filename="profStats.stat")
        stopTime = time.time()

        idList = self.getJobs.execute(state='CreateFailed')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state='JobFailed')
        self.assertEqual(len(idList), 0)

        idList = self.getJobs.execute(state='JobCooloff')
        self.assertEqual(len(idList), nJobs)

        print("Took %f seconds to run polling algo" % (stopTime - startTime))

        p = pstats.Stats('profStats.stat')
        p.sort_stats('cumulative')
        p.print_stats(0.2)

        return


if __name__ == '__main__':
    unittest.main()
