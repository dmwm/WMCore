#!/bin/env python
# pylint: disable=E1101, W0201, E1103
# E1101: reference config file variables
# W0201: Don't much around with __init__
# E1103: Use thread members

from __future__ import print_function
from builtins import range, object

import cProfile
import os
import pickle
import pstats
import random
import threading
import time
import unittest

from Utils.PythonVersion import PY3

from WMCore_t.WMSpec_t.TestSpec import createTestWorkload
from nose.plugins.attrib import attr

from WMComponent.JobCreator.JobCreatorPoller import JobCreatorPoller, capResourceEstimates
from WMCore.Agent.HeartbeatAPI import HeartbeatAPI
from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.Run import Run
from WMCore.ResourceControl.ResourceControl import ResourceControl
from WMCore.Services.UUIDLib import makeUUID
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMSpec.Makers.TaskMaker import TaskMaker
from WMQuality.Emulators import EmulatorSetup
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit


class JobCreatorTest(EmulatedUnitTestCase):
    """
    Test case for the JobCreator

    """

    sites = ['T2_US_Florida', 'T2_US_UCSD', 'T2_TW_Taiwan', 'T1_CH_CERN']

    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        WMBS tables.  Also, create some dummy locations.
        """
        super(JobCreatorTest, self).setUp()

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()

        self.testInit.setSchema(customModules=['WMCore.WMBS', 'WMCore.ResourceControl', 'WMCore.Agent.Database'],
                                useDefault=False)
        self.couchdbname = "jobcreator_t"
        self.testInit.setupCouch("%s/jobs" % self.couchdbname, "JobDump")
        self.testInit.setupCouch("%s/fwjrs" % self.couchdbname, "FWJRDump")
        self.configFile = EmulatorSetup.setupWMAgentConfig()

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)

        locationAction = self.daoFactory(classname="Locations.New")
        for site in self.sites:
            locationAction.execute(siteName=site, pnn=site)

        # Create sites in resourceControl

        resourceControl = ResourceControl()
        for site in self.sites:
            resourceControl.insertSite(siteName=site, pnn=site, ceName=site)
            resourceControl.insertThreshold(siteName=site, taskType='Processing', maxSlots=10000, pendingSlots=10000)

        self.resourceControl = resourceControl

        self._setup = True
        self._teardown = False

        self.testDir = self.testInit.generateWorkDir()
        self.cwd = os.getcwd()

        # Set heartbeat
        self.componentName = 'JobCreator'
        self.heartbeatAPI = HeartbeatAPI(self.componentName)
        self.heartbeatAPI.registerComponent()

        if PY3:
            self.assertItemsEqual = self.assertCountEqual

        return

    def tearDown(self):
        """
        _tearDown_

        Drop all the WMBS tables.
        """

        self.testInit.clearDatabase(modules=['WMCore.WMBS', 'WMCore.ResourceControl', 'WMCore.Agent.Database'])

        self.testInit.delWorkDir()

        self._teardown = True

        self.testInit.tearDownCouch()
        EmulatorSetup.deleteConfig(self.configFile)

        return

    def createJobCollection(self, name, nSubs, nFiles, workflowURL='test'):
        """
        _createJobCollection_

        Create a collection of jobs
        """

        myThread = threading.currentThread()

        testWorkflow = Workflow(spec=workflowURL, owner="mnorman",
                                name=name, task="/TestWorkload/ReReco")
        testWorkflow.create()

        for sub in range(nSubs):

            nameStr = '%s-%i' % (name, sub)

            myThread.transaction.begin()

            testFileset = Fileset(name=nameStr)
            testFileset.create()

            for f in range(nFiles):
                # pick a random site
                site = random.choice(self.sites)
                testFile = File(lfn="/lfn/%s/%i" % (nameStr, f), size=1024, events=10)
                testFile.setLocation(site)
                testFile.create()
                testFileset.addFile(testFile)

            testFileset.commit()
            testSubscription = Subscription(fileset=testFileset,
                                            workflow=testWorkflow,
                                            type="Processing",
                                            split_algo="FileBased")
            testSubscription.create()

            myThread.transaction.commit()

        return

    def createWorkload(self, workloadName='Test'):
        """
        Creates a test workload for us to run on, hold the basic necessities.
        """

        workload = createTestWorkload(workloadName)
        rereco = workload.getTask("ReReco")
        seederDict = {"generator.initialSeed": 1001, "evtgenproducer.initialSeed": 1001}
        rereco.addGenerator("PresetSeeder", **seederDict)

        taskMaker = TaskMaker(workload, os.path.join(self.testDir, 'workloadTest'))
        taskMaker.skipSubscription = True
        taskMaker.processWorkload()

        return workload

    def getConfig(self):
        """
        _getConfig_

        Creates a common config.
        """

        config = self.testInit.getConfiguration()
        self.testInit.generateWorkDir(config)

        # First the general stuff
        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR", os.getcwd())

        config.section_("Agent")
        config.Agent.componentName = self.componentName

        # Now the CoreDatabase information
        # This should be the dialect, dburl, etc
        config.section_("CoreDatabase")
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.socket = os.getenv("DBSOCK")

        config.component_("JobCreator")
        config.JobCreator.namespace = 'WMComponent.JobCreator.JobCreator'
        # The log level of the component.
        # config.JobCreator.logLevel = 'SQLDEBUG'
        config.JobCreator.logLevel = 'INFO'

        # maximum number of threads we want to deal
        # with messages per pool.
        config.JobCreator.maxThreads = 1
        config.JobCreator.UpdateFromResourceControl = True
        config.JobCreator.pollInterval = 10
        # config.JobCreator.jobCacheDir               = self.testDir
        config.JobCreator.defaultJobType = 'processing'  # Type of jobs that we run, used for resource control
        config.JobCreator.workerThreads = 4
        config.JobCreator.componentDir = self.testDir
        config.JobCreator.useWorkQueue = True
        config.JobCreator.WorkQueueParams = {'emulateDBSReader': True}

        # We now call the JobMaker from here
        config.component_('JobMaker')
        config.JobMaker.logLevel = 'INFO'
        config.JobMaker.namespace = 'WMCore.WMSpec.Makers.JobMaker'
        config.JobMaker.maxThreads = 1
        config.JobMaker.makeJobsHandler = 'WMCore.WMSpec.Makers.Handlers.MakeJobs'

        # JobStateMachine
        config.component_('JobStateMachine')
        config.JobStateMachine.couchurl = os.getenv('COUCHURL', 'cmssrv52.fnal.gov:5984')
        config.JobStateMachine.couchDBName = self.couchdbname

        return config

    def testVerySimpleTest(self):
        """
        _VerySimpleTest_

        Just test that everything works...more or less
        """

        # return

        myThread = threading.currentThread()

        config = self.getConfig()

        name = makeUUID()
        nSubs = 5
        nFiles = 10
        workloadName = 'TestWorkload'

        dummyWorkload = self.createWorkload(workloadName=workloadName)
        workloadPath = os.path.join(self.testDir, 'workloadTest', 'TestWorkload', 'WMSandbox', 'WMWorkload.pkl')

        self.createJobCollection(name=name, nSubs=nSubs, nFiles=nFiles, workflowURL=workloadPath)

        testJobCreator = JobCreatorPoller(config=config)

        # First, can we run once without everything crashing?
        testJobCreator.algorithm()

        getJobsAction = self.daoFactory(classname="Jobs.GetAllJobs")
        result = getJobsAction.execute(state='Created', jobType="Processing")

        self.assertEqual(len(result), nSubs * nFiles)

        # Count database objects
        result = myThread.dbi.processData('SELECT * FROM wmbs_sub_files_acquired')[0].fetchall()
        self.assertEqual(len(result), nSubs * nFiles)

        # Find the test directory
        testDirectory = os.path.join(self.testDir, 'jobCacheDir', 'TestWorkload', 'ReReco')
        # It should have at least one jobGroup
        self.assertTrue('JobCollection_1_0' in os.listdir(testDirectory))
        # But no more then twenty
        self.assertTrue(len(os.listdir(testDirectory)) <= 20)

        groupDirectory = os.path.join(testDirectory, 'JobCollection_1_0')

        # First job should be in here
        listOfDirs = []
        for tmpDirectory in os.listdir(testDirectory):
            listOfDirs.extend(os.listdir(os.path.join(testDirectory, tmpDirectory)))
        self.assertTrue('job_1' in listOfDirs)
        self.assertTrue('job_2' in listOfDirs)
        self.assertTrue('job_3' in listOfDirs)
        jobDir = os.listdir(groupDirectory)[0]
        jobFile = os.path.join(groupDirectory, jobDir, 'job.pkl')
        self.assertTrue(os.path.isfile(jobFile))
        f = open(jobFile, 'rb')
        job = pickle.load(f)
        f.close()

        self.assertEqual(job.baggage.PresetSeeder.generator.initialSeed, 1001)
        self.assertEqual(job.baggage.PresetSeeder.evtgenproducer.initialSeed, 1001)

        self.assertEqual(job['workflow'], name)
        self.assertEqual(len(job['input_files']), 1)
        self.assertEqual(os.path.basename(job['sandbox']), 'TestWorkload-Sandbox.tar.bz2')

        return

    def testCampaignName(self):
        """
        Test campaign name is written into job pickle file
        """
        myThread = threading.currentThread()

        config = self.getConfig()

        name = makeUUID()
        nSubs = 1
        nFiles = 1
        workloadName = 'TestWorkload'

        dummyWorkload = self.createWorkload(workloadName=workloadName)
        workloadPath = os.path.join(self.testDir, 'workloadTest', 'TestWorkload', 'WMSandbox', 'WMWorkload.pkl')

        self.createJobCollection(name=name, nSubs=nSubs, nFiles=nFiles, workflowURL=workloadPath)

        testJobCreator = JobCreatorPoller(config=config)

        # First, can we run once without everything crashing?
        testJobCreator.algorithm()

        getJobsAction = self.daoFactory(classname="Jobs.GetAllJobs")
        result = getJobsAction.execute(state='Created', jobType="Processing")

        # Find the test directory
        testDirectory = os.path.join(self.testDir, 'jobCacheDir', 'TestWorkload', 'ReReco')
        groupDirectory = os.path.join(testDirectory, 'JobCollection_1_0')

        # Get job pickle file
        jobDir = os.listdir(groupDirectory)[0]
        jobFile = os.path.join(groupDirectory, jobDir, 'job.pkl')
        self.assertTrue(os.path.isfile(jobFile))
        with open(jobFile, 'rb') as f:
            job = pickle.load(f)

        # Attribute campaign name should exist
        # but be set to the default value: None
        self.assertEqual(job['campaignName'], None)

        return

    def testPhysicsType(self):
        """
        Test physics type is written into job pickle file
        """
        myThread = threading.currentThread()

        config = self.getConfig()

        name = makeUUID()
        nSubs = 1
        nFiles = 1
        workloadName = 'TestWorkload'

        dummyWorkload = self.createWorkload(workloadName=workloadName)
        workloadPath = os.path.join(self.testDir, 'workloadTest', 'TestWorkload', 'WMSandbox', 'WMWorkload.pkl')

        self.createJobCollection(name=name, nSubs=nSubs, nFiles=nFiles, workflowURL=workloadPath)

        testJobCreator = JobCreatorPoller(config=config)

        # First, can we run once without everything crashing?
        testJobCreator.algorithm()

        getJobsAction = self.daoFactory(classname="Jobs.GetAllJobs")
        result = getJobsAction.execute(state='Created', jobType="Processing")

        # Find the test directory
        testDirectory = os.path.join(self.testDir, 'jobCacheDir', 'TestWorkload', 'ReReco')
        groupDirectory = os.path.join(testDirectory, 'JobCollection_1_0')

        # Get job pickle file
        jobDir = os.listdir(groupDirectory)[0]
        jobFile = os.path.join(groupDirectory, jobDir, 'job.pkl')
        self.assertTrue(os.path.isfile(jobFile))
        with open(jobFile, 'rb') as f:
            job = pickle.load(f)

        # Attribute campaign name should exist
        # but be set to the default value: None
        self.assertEqual(job['physicsTaskType'], None)

        return
    @attr('performance', 'integration')
    def testProfilePoller(self):
        """
        Profile your performance
        You shouldn't be running this normally because it doesn't do anything
        """

        name = makeUUID()
        nSubs = 5
        nFiles = 1500
        workloadName = 'TestWorkload'

        workload = self.createWorkload(workloadName=workloadName)
        workloadPath = os.path.join(self.testDir, 'workloadTest', 'TestWorkload', 'WMSandbox', 'WMWorkload.pkl')

        self.createJobCollection(name=name, nSubs=nSubs, nFiles=nFiles, workflowURL=workloadPath)

        config = self.getConfig()

        testJobCreator = JobCreatorPoller(config=config)
        cProfile.runctx("testJobCreator.algorithm()", globals(), locals(), filename="testStats.stat")

        getJobsAction = self.daoFactory(classname="Jobs.GetAllJobs")
        result = getJobsAction.execute(state='Created', jobType="Processing")

        time.sleep(10)

        self.assertEqual(len(result), nSubs * nFiles)

        p = pstats.Stats('testStats.stat')
        p.sort_stats('cumulative')
        p.print_stats(.2)

        return

    @attr('integration')
    def testProfileWorker(self):
        """
        Profile where the work actually gets done
        You shouldn't be running this one either, since it doesn't test anything.
        """

        name = makeUUID()
        nSubs = 5
        nFiles = 500
        workloadName = 'TestWorkload'

        workload = self.createWorkload(workloadName=workloadName)
        workloadPath = os.path.join(self.testDir, 'workloadTest', 'TestWorkload', 'WMSandbox', 'WMWorkload.pkl')

        self.createJobCollection(name=name, nSubs=nSubs, nFiles=nFiles, workflowURL=workloadPath)

        config = self.getConfig()

        configDict = {"couchURL": config.JobStateMachine.couchurl,
                      "couchDBName": config.JobStateMachine.couchDBName,
                      'jobCacheDir': config.JobCreator.jobCacheDir,
                      'defaultJobType': config.JobCreator.defaultJobType}

        subs = [{"subscription": 1}, {"subscription": 2}, {"subscription": 3}, {"subscription": 4},
                {"subscription": 5}]

        testJobCreator = JobCreatorPoller(**configDict)
        cProfile.runctx("testJobCreator.algorithm(parameters = input)", globals(), locals(), filename="workStats.stat")

        p = pstats.Stats('workStats.stat')
        p.sort_stats('cumulative')
        p.print_stats(.2)

        return

    @attr('integration')
    def testHugeTest(self):
        """
        Don't run this one either

        """

        myThread = threading.currentThread()

        config = self.getConfig()

        name = makeUUID()
        nSubs = 10
        nFiles = 5000
        workloadName = 'Tier1ReReco'

        dummyWorkload = self.createWorkload(workloadName=workloadName)
        workloadPath = os.path.join(self.testDir, 'workloadTest', 'TestWorkload', 'WMSandbox', 'WMWorkload.pkl')

        self.createJobCollection(name=name, nSubs=nSubs, nFiles=nFiles, workflowURL=workloadPath)

        testJobCreator = JobCreatorPoller(config=config)

        # First, can we run once without everything crashing?
        startTime = time.time()
        testJobCreator.algorithm()
        stopTime = time.time()

        getJobsAction = self.daoFactory(classname="Jobs.GetAllJobs")
        result = getJobsAction.execute(state='Created', jobType="Processing")

        self.assertEqual(len(result), nSubs * nFiles)

        print("Job took %f seconds to run" % (stopTime - startTime))

        # Count database objects
        result = myThread.dbi.processData('SELECT * FROM wmbs_sub_files_acquired')[0].fetchall()
        self.assertEqual(len(result), nSubs * nFiles)

        return

    def stuffWMBS(self, workflowURL, name):
        """
        _stuffWMBS_

        Insert some dummy jobs, jobgroups, filesets, files and subscriptions
        into WMBS to test job creation.  Three completed job groups each
        containing several files are injected.  Another incomplete job group is
        also injected.  Also files are added to the "Mergeable" subscription as
        well as to the output fileset for their jobgroups.
        """
        locationAction = self.daoFactory(classname="Locations.New")
        locationAction.execute(siteName="s1", pnn="somese.cern.ch")

        mergeFileset = Fileset(name="mergeFileset")
        mergeFileset.create()
        bogusFileset = Fileset(name="bogusFileset")
        bogusFileset.create()

        mergeWorkflow = Workflow(spec=workflowURL, owner="mnorman",
                                 name=name, task="/TestWorkload/ReReco")
        mergeWorkflow.create()

        mergeSubscription = Subscription(fileset=mergeFileset,
                                         workflow=mergeWorkflow,
                                         split_algo="ParentlessMergeBySize")
        mergeSubscription.create()
        dummySubscription = Subscription(fileset=bogusFileset,
                                         workflow=mergeWorkflow,
                                         split_algo="ParentlessMergeBySize")

        file1 = File(lfn="file1", size=1024, events=1024, first_event=0,
                     locations={"somese.cern.ch"})
        file1.addRun(Run(1, *[45]))
        file1.create()
        file2 = File(lfn="file2", size=1024, events=1024, first_event=1024, locations={"somese.cern.ch"})
        file2.addRun(Run(1, *[45]))
        file2.create()
        file3 = File(lfn="file3", size=1024, events=1024, first_event=2048, locations={"somese.cern.ch"})
        file3.addRun(Run(1, *[45]))
        file3.create()
        file4 = File(lfn="file4", size=1024, events=1024, first_event=3072, locations={"somese.cern.ch"})
        file4.addRun(Run(1, *[45]))
        file4.create()

        fileA = File(lfn="fileA", size=1024, events=1024, first_event=0, locations={"somese.cern.ch"})
        fileA.addRun(Run(1, *[46]))
        fileA.create()
        fileB = File(lfn="fileB", size=1024, events=1024, first_event=1024, locations={"somese.cern.ch"})
        fileB.addRun(Run(1, *[46]))
        fileB.create()
        fileC = File(lfn="fileC", size=1024, events=1024, first_event=2048, locations={"somese.cern.ch"})
        fileC.addRun(Run(1, *[46]))
        fileC.create()

        fileI = File(lfn="fileI", size=1024, events=1024, first_event=0, locations={"somese.cern.ch"})
        fileI.addRun(Run(2, *[46]))
        fileI.create()
        fileII = File(lfn="fileII", size=1024, events=1024, first_event=1024, locations={"somese.cern.ch"})
        fileII.addRun(Run(2, *[46]))
        fileII.create()
        fileIII = File(lfn="fileIII", size=1024, events=1024, first_event=2048, locations={"somese.cern.ch"})
        fileIII.addRun(Run(2, *[46]))
        fileIII.create()
        fileIV = File(lfn="fileIV", size=1024 * 1000000, events=1024, first_event=3072, locations={"somese.cern.ch"})
        fileIV.addRun(Run(2, *[46]))
        fileIV.create()

        for fileObj in [file1, file2, file3, file4, fileA, fileB, fileC, fileI, fileII, fileIII, fileIV]:
            mergeFileset.addFile(fileObj)
            bogusFileset.addFile(fileObj)

        mergeFileset.commit()
        bogusFileset.commit()

        return

    def testTestNonProxySplitting(self):
        """
        _TestNonProxySplitting_

        Test and see if we can split things without a proxy.
        """

        config = self.getConfig()
        config.JobCreator.workerThreads = 1

        name = makeUUID()
        workloadName = 'TestWorkload'

        workload = self.createWorkload(workloadName=workloadName)

        workloadPath = os.path.join(self.testDir, 'workloadTest', 'TestWorkload', 'WMSandbox', 'WMWorkload.pkl')

        self.stuffWMBS(workflowURL=workloadPath, name=name)

        testJobCreator = JobCreatorPoller(config=config)

        testJobCreator.algorithm()

        getJobsAction = self.daoFactory(classname="Jobs.GetAllJobs")
        result = getJobsAction.execute(state='Created', jobType="Processing")

        self.assertEqual(len(result), 1)

        result = getJobsAction.execute(state='Created', jobType="Merge")
        self.assertEqual(len(result), 0)

        return

    def testCapResourceEstimates(self):
        """
        _testCapResourceEstimates_

        Test capResourceEstimates function to make sure the glideinwms
        constraints are being properly considered.
        """

        class JobGroup(object):
            """Dummy object holding a jobs attr full of jobs"""

            def __init__(self):
                self.jobs = []

        constraints = {'MaxRequestDiskKB': 20971520, 'MinRequestDiskKB': 1048576,
                       'MaxWallTimeSecs': 162000, 'MinWallTimeSecs': 3600}

        jobGroups = []
        jobGroup = JobGroup()
        jobGroup.jobs.append({'estimatedJobTime': None, 'estimatedDiskUsage': None})
        jobGroup.jobs.append({'estimatedJobTime': 0, 'estimatedDiskUsage': 0})
        jobGroup.jobs.append({'estimatedJobTime': 10000, 'estimatedDiskUsage': 10 * 1000 * 1000})
        jobGroup.jobs.append({'estimatedJobTime': 200000, 'estimatedDiskUsage': 100 * 1000 * 1000})
        jobGroups.append(jobGroup)

        capResourceEstimates(jobGroups, constraints)

        self.assertItemsEqual(jobGroup.jobs[0], {'estimatedJobTime': 3600, 'estimatedDiskUsage': 1048576})
        self.assertItemsEqual(jobGroup.jobs[1], {'estimatedJobTime': 3600, 'estimatedDiskUsage': 1048576})
        self.assertItemsEqual(jobGroup.jobs[2], {'estimatedJobTime': 10000, 'estimatedDiskUsage': 10 * 1000 * 1000})
        self.assertItemsEqual(jobGroup.jobs[3], {'estimatedJobTime': 162000, 'estimatedDiskUsage': 20971520})

        return


if __name__ == "__main__":
    unittest.main()
