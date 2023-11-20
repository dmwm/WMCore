#!/usr/bin/env python

from __future__ import division

import os.path

import getpass
import os
import pickle
import random
import shutil
import threading
import time
import unittest
from WMCore_t.WMSpec_t.TestSpec import createTestWorkload
from subprocess import Popen, PIPE

from WMComponent.JobAccountant.JobAccountantPoller import JobAccountantPoller
from WMComponent.JobArchiver.JobArchiverPoller import JobArchiverPoller
# Component imports
from WMComponent.JobCreator.JobCreatorPoller import JobCreatorPoller
from WMComponent.JobSubmitter.JobSubmitterPoller import JobSubmitterPoller
from WMComponent.JobTracker.JobTrackerPoller import JobTrackerPoller
from WMComponent.TaskArchiver.TaskArchiverPoller import TaskArchiverPoller
# Agent imports
from WMCore.Agent.HeartbeatAPI import HeartbeatAPI
from WMCore.DAOFactory import DAOFactory
from WMCore.FwkJobReport.Report import Report
# WMCore library imports
from WMCore.ResourceControl.ResourceControl import ResourceControl
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
# WMBS Objects
from WMCore.WMBS.Job import Job
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMInit import getWMBASE
# WMSpec stuff
from WMCore.WMSpec.Makers.TaskMaker import TaskMaker
from WMQuality.Emulators import EmulatorSetup
# Imports for testing
from WMQuality.TestInit import TestInit


def getCondorRunningJobs(user=None):
    """
    _getCondorRunningJobs_

    Return the number of jobs currently running for a user
    """
    if not user:
        user = getpass.getuser()

    command = ['condor_q', user]
    pipe = Popen(command, stdout=PIPE, stderr=PIPE, shell=False)
    stdout, error = pipe.communicate()

    output = stdout.split('\n')[-2]

    nJobs = int(output.split(';')[0].split()[0])

    return nJobs


def condorRM(user=None):
    # Now clean-up
    if not user:
        user = getpass.getuser()
    command = ['condor_rm', user]
    pipe = Popen(command, stdout=PIPE, stderr=PIPE, shell=False)
    pipe.communicate()
    return


class WMAgentTest(unittest.TestCase):
    """
    _WMAgentTest_

    Global unittest for all WMAgent components
    """

    # This is an integration test
    __integration__ = "Any old bollocks"

    sites = ['T2_US_Florida', 'T2_US_UCSD', 'T2_TW_Taiwan', 'T1_CH_CERN']
    components = ['JobCreator', 'JobSubmitter', 'JobTracker',
                  'JobAccountant', 'JobArchiver', 'TaskArchiver',
                  'RetryManager', 'ErrorHandler']

    def setUp(self):
        """
        _setUp_

        Set up vital components
        """

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules=["WMCore.WMBS", 'WMCore.MsgService',
                                               'WMCore.ResourceControl', 'WMCore.ThreadPool',
                                               'WMCore.Agent.Database'],
                                useDefault=False)

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)

        locationAction = self.daoFactory(classname="Locations.New")
        pendingSlots = self.daoFactory(classname="Locations.SetPendingSlots")

        for site in self.sites:
            locationAction.execute(siteName=site, pnn='se.%s' % (site), ceName=site)
            pendingSlots.execute(siteName=site, pendingSlots=1000)

        # Create sites in resourceControl
        resourceControl = ResourceControl()
        for site in self.sites:
            resourceControl.insertSite(siteName=site, pnn='se.%s' % (site), ceName=site)
            resourceControl.insertThreshold(siteName=site, taskType='Processing', \
                                            maxSlots=10000, pendingSlots=10000)

        self.testDir = self.testInit.generateWorkDir()

        # Set heartbeat
        for component in self.components:
            heartbeatAPI = HeartbeatAPI(component)
            heartbeatAPI.registerComponent()

        self.configFile = EmulatorSetup.setupWMAgentConfig()

        return

    def tearDown(self):
        """
        _tearDown_

        Tear down everything and go home.
        """

        self.testInit.clearDatabase()

        self.testInit.delWorkDir()

        EmulatorSetup.deleteConfig(self.configFile)

        return

    def setupTestWorkload(self, workloadName='Test', emulator=True):
        """

        Creates a test workload for us to run on, hold the basic necessities.
        """

        workload = createTestWorkload("TestWorkload")
        rereco = workload.getTask("ReReco")

        taskMaker = TaskMaker(workload, os.path.join(self.testDir, 'workloadTest'))
        taskMaker.skipSubscription = True
        taskMaker.processWorkload()

        workload.save(workloadName)

        return workload

    def getConfig(self):
        """
        _getConfig_

        This is the global test configuration object
        """

        config = self.testInit.getConfiguration()

        config.component_("Agent")
        config.Agent.WMSpecDirectory = self.testDir
        config.Agent.agentName = 'testAgent'
        config.Agent.componentName = 'test'

        # First the general stuff
        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR", self.testDir)

        # Now the CoreDatabase information
        # This should be the dialect, dburl, etc

        config.section_("CoreDatabase")
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.socket = os.getenv("DBSOCK")

        # JobCreator
        config.component_("JobCreator")
        config.JobCreator.namespace = 'WMComponent.JobCreator.JobCreator'
        config.JobCreator.logLevel = 'DEBUG'
        config.JobCreator.maxThreads = 1
        config.JobCreator.UpdateFromResourceControl = True
        config.JobCreator.pollInterval = 10
        config.JobCreator.jobCacheDir = self.testDir
        config.JobCreator.defaultJobType = 'processing'  # Type of jobs that we run, used for resource control
        config.JobCreator.workerThreads = 2
        config.JobCreator.componentDir = os.path.join(os.getcwd(), 'Components')

        # JobSubmitter
        config.component_("JobSubmitter")
        config.JobSubmitter.namespace = 'WMComponent.JobSubmitter.JobSubmitter'
        config.JobSubmitter.logLevel = 'INFO'
        config.JobSubmitter.maxThreads = 1
        config.JobSubmitter.pollInterval = 10
        config.JobSubmitter.pluginName = 'CondorGlobusPlugin'
        config.JobSubmitter.pluginDir = 'JobSubmitter.Plugins'
        config.JobSubmitter.submitDir = os.path.join(self.testDir, 'submit')
        config.JobSubmitter.submitNode = os.getenv("HOSTNAME", 'badtest.fnal.gov')
        config.JobSubmitter.submitScript = os.path.join(getWMBASE(),
                                                        'test/python/WMComponent_t/JobSubmitter_t',
                                                        'submit.sh')
        config.JobSubmitter.componentDir = os.path.join(os.getcwd(), 'Components')
        config.JobSubmitter.workerThreads = 2
        config.JobSubmitter.jobsPerWorker = 200

        # JobTracker
        config.component_("JobTracker")
        config.JobTracker.logLevel = 'DEBUG'
        config.JobTracker.pollInterval = 10
        config.JobTracker.trackerName = 'CondorTracker'
        config.JobTracker.pluginDir = 'WMComponent.JobTracker.Plugins'
        config.JobTracker.componentDir = os.path.join(os.getcwd(), 'Components')
        config.JobTracker.runTimeLimit = 7776000  # Jobs expire after 90 days
        config.JobTracker.idleTimeLimit = 7776000
        config.JobTracker.heldTimeLimit = 7776000
        config.JobTracker.unknTimeLimit = 7776000

        # JobAccountant
        config.component_("JobAccountant")
        config.JobAccountant.pollInterval = 60
        config.JobAccountant.componentDir = os.path.join(os.getcwd(), 'Components')
        config.JobAccountant.logLevel = 'INFO'

        # JobArchiver
        config.component_("JobArchiver")
        config.JobArchiver.pollInterval = 60
        config.JobArchiver.logLevel = 'INFO'
        config.JobArchiver.logDir = os.path.join(self.testDir, 'logs')
        config.JobArchiver.componentDir = os.path.join(os.getcwd(), 'Components')
        config.JobArchiver.numberOfJobsToCluster = 1000

        # Task Archiver
        config.component_("TaskArchiver")
        config.TaskArchiver.componentDir = self.testInit.generateWorkDir()
        config.TaskArchiver.WorkQueueParams = {}
        config.TaskArchiver.pollInterval = 60
        config.TaskArchiver.logLevel = 'INFO'
        config.TaskArchiver.timeOut = 0

        # JobStateMachine
        config.component_('JobStateMachine')
        config.JobStateMachine.couchurl = os.getenv('COUCHURL',
                                                    'mnorman:theworst@cmssrv52.fnal.gov:5984')
        config.JobStateMachine.couchDBName = "mnorman_test"

        # Needed, because this is a test
        os.makedirs(config.JobSubmitter.submitDir)

        return config

    def createFileCollection(self, name, nSubs, nFiles, workflowURL='test', site=None):
        """
        _createFileCollection_

        Create a collection of files for splitting into jobs
        """

        myThread = threading.currentThread()

        testWorkflow = Workflow(spec=workflowURL, owner="mnorman",
                                name=name, task="/TestWorkload/ReReco")
        testWorkflow.create()

        for sub in range(nSubs):

            nameStr = '%s-%i' % (name, sub)

            testFileset = Fileset(name=nameStr)
            testFileset.create()

            for f in range(nFiles):
                # pick a random site
                if not site:
                    tmpSite = 'se.%s' % (random.choice(self.sites))
                else:
                    tmpSite = 'se.%s' % (site)
                testFile = File(lfn="/lfn/%s/%i" % (nameStr, f), size=1024, events=10)
                testFile.setLocation(tmpSite)
                testFile.create()
                testFileset.addFile(testFile)

            testFileset.commit()
            testFileset.markOpen(isOpen=0)
            testSubscription = Subscription(fileset=testFileset,
                                            workflow=testWorkflow,
                                            type="Processing",
                                            split_algo="FileBased")
            testSubscription.create()

        return

    def createReports(self, jobs, retryCount=0):
        """
        _createReports_

        Create some dummy job reports for each job
        """

        report = Report()
        report.addStep('testStep', 0)

        for job in jobs:
            # reportPath = os.path.join(job['cache_dir'], 'Report.%i.pkl' % (retryCount))
            reportPath = job['fwjr_path']
            if os.path.exists(reportPath):
                os.remove(reportPath)
            report.save(reportPath)

        return

    def testA_StraightThrough(self):
        """
        _StraightThrough_

        Just run everything straight through without any variations
        """
        # Do pre-submit job check
        nRunning = getCondorRunningJobs()
        self.assertEqual(nRunning, 0, "User currently has %i running jobs.  Test will not continue" % (nRunning))

        myThread = threading.currentThread()
        workload = self.setupTestWorkload()
        config = self.getConfig()

        name = 'WMAgent_Test1'
        site = self.sites[0]
        nSubs = 5
        nFiles = 10
        workloadPath = os.path.join(self.testDir, 'workloadTest',
                                    'TestWorkload', 'WMSandbox',
                                    'WMWorkload.pkl')

        # Create a collection of files
        self.createFileCollection(name=name, nSubs=nSubs,
                                  nFiles=nFiles,
                                  workflowURL=workloadPath,
                                  site=site)

        ############################################################
        # Test the JobCreator

        config.Agent.componentName = 'JobCreator'
        testJobCreator = JobCreatorPoller(config=config)

        testJobCreator.algorithm()
        time.sleep(5)

        # Did all jobs get created?
        getJobsAction = self.daoFactory(classname="Jobs.GetAllJobs")
        result = getJobsAction.execute(state='Created', jobType="Processing")
        self.assertEqual(len(result), nSubs * nFiles)

        # Count database objects
        result = myThread.dbi.processData('SELECT * FROM wmbs_sub_files_acquired')[0].fetchall()
        self.assertEqual(len(result), nSubs * nFiles)

        # Find the test directory
        testDirectory = os.path.join(self.testDir, 'TestWorkload', 'ReReco')
        self.assertTrue('JobCollection_1_0' in os.listdir(testDirectory))
        self.assertTrue(len(os.listdir(testDirectory)) <= 20)

        groupDirectory = os.path.join(testDirectory, 'JobCollection_1_0')

        # First job should be in here
        self.assertTrue('job_1' in os.listdir(groupDirectory))
        jobFile = os.path.join(groupDirectory, 'job_1', 'job.pkl')
        self.assertTrue(os.path.isfile(jobFile))
        with open(jobFile, 'rb') as f:
            job = pickle.load(f)

        self.assertEqual(job['workflow'], name)
        self.assertEqual(len(job['input_files']), 1)
        self.assertEqual(os.path.basename(job['sandbox']), 'TestWorkload-Sandbox.tar.bz2')

        ###############################################################
        # Now test the JobSubmitter

        config.Agent.componentName = 'JobSubmitter'
        testJobSubmitter = JobSubmitterPoller(config=config)

        testJobSubmitter.algorithm()

        # Check that jobs are in the right state
        result = getJobsAction.execute(state='Created', jobType="Processing")
        self.assertEqual(len(result), 0)
        result = getJobsAction.execute(state='Executing', jobType="Processing")
        self.assertEqual(len(result), nSubs * nFiles)

        # Check assigned locations
        getLocationAction = self.daoFactory(classname="Jobs.GetLocation")
        for id in result:
            loc = getLocationAction.execute(jobid=id)
            self.assertEqual(loc, [[site]])

        # Check to make sure we have running jobs
        nRunning = getCondorRunningJobs()
        self.assertEqual(nRunning, nFiles * nSubs)

        #################################################################
        # Now the JobTracker

        config.Agent.componentName = 'JobTracker'
        testJobTracker = JobTrackerPoller(config=config)
        testJobTracker.setup()

        testJobTracker.algorithm()

        # Running the algo without removing the jobs should do nothing
        result = getJobsAction.execute(state='Executing', jobType="Processing")
        self.assertEqual(len(result), nSubs * nFiles)

        condorRM()
        time.sleep(1)

        # All jobs gone?
        nRunning = getCondorRunningJobs()
        self.assertEqual(nRunning, 0)

        testJobTracker.algorithm()
        time.sleep(5)

        # Running the algo without removing the jobs should do nothing
        result = getJobsAction.execute(state='Executing', jobType="Processing")
        self.assertEqual(len(result), 0)
        result = getJobsAction.execute(state='Complete', jobType="Processing")
        self.assertEqual(len(result), nSubs * nFiles)

        #################################################################
        # Now the JobAccountant

        # First you need to load all jobs

        self.getFWJRAction = self.daoFactory(classname="Jobs.GetFWJRByState")
        completeJobs = self.getFWJRAction.execute(state="complete")

        # Create reports for all jobs
        self.createReports(jobs=completeJobs, retryCount=0)

        config.Agent.componentName = 'JobAccountant'
        testJobAccountant = JobAccountantPoller(config=config)
        testJobAccountant.setup()

        # It should do something with the jobs
        testJobAccountant.algorithm()

        # All the jobs should be done now
        result = getJobsAction.execute(state='Complete', jobType="Processing")
        self.assertEqual(len(result), 0)
        result = getJobsAction.execute(state='Success', jobType="Processing")
        self.assertEqual(len(result), nSubs * nFiles)

        #######################################################################
        # Now the JobArchiver

        config.Agent.componentName = 'JobArchiver'
        testJobArchiver = JobArchiverPoller(config=config)

        testJobArchiver.algorithm()

        # All the jobs should be cleaned up
        result = getJobsAction.execute(state='Success', jobType="Processing")
        self.assertEqual(len(result), 0)
        result = getJobsAction.execute(state='Cleanout', jobType="Processing")
        self.assertEqual(len(result), nSubs * nFiles)

        logDir = os.path.join(self.testDir, 'logs')

        for job in completeJobs:
            self.assertFalse(os.path.exists(job['fwjr_path']))
            jobFolder = 'JobCluster_%i' \
                        % (int(job['id'] / config.JobArchiver.numberOfJobsToCluster))
            jobPath = os.path.join(logDir, jobFolder, 'Job_%i.tar' % (job['id']))
            self.assertTrue(os.path.isfile(jobPath))
            self.assertTrue(os.path.getsize(jobPath) > 0)

        ###########################################################################
        # Now the TaskAchiver

        config.Agent.componentName = 'TaskArchiver'
        testTaskArchiver = TaskArchiverPoller(config=config)

        testTaskArchiver.algorithm()

        result = getJobsAction.execute(state='Cleanout', jobType="Processing")
        self.assertEqual(len(result), 0)

        for jdict in completeJobs:
            job = Job(id=jdict['id'])
            self.assertFalse(job.exists())

        if os.path.isdir('testDir'):
            shutil.rmtree('testDir')
        shutil.copytree('%s' % self.testDir, os.path.join(os.getcwd(), 'testDir'))

        return


if __name__ == "__main__":
    unittest.main()
