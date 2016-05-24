#!/bin/env python
"""
_JobSubmitter_t_

JobSubmitter unit-test, uses the MockPlugin to submit and tests
the different dynamics that occur inside the JobSubmitter.
"""
from __future__ import print_function

import cProfile
import os
import pickle
import pstats
import threading
import time
import unittest

from nose.plugins.attrib import attr
from WMComponent.JobSubmitter.JobSubmitterPoller import JobSubmitterPoller
from WMCore.Agent.HeartbeatAPI import HeartbeatAPI
from WMCore.DAOFactory import DAOFactory
from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.ResourceControl.ResourceControl import ResourceControl
from WMCore.Services.UUID import makeUUID
from WMCore.WMBase import getTestBase
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMSpec.Makers.TaskMaker import TaskMaker
from WMCore_t.WMSpec_t.TestSpec import testWorkload
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit
from WMQuality.Emulators import EmulatorSetup

class JobSubmitterTest(unittest.TestCase):
    """
    _JobSubmitterTest_

    Test class for the JobSubmitterPoller
    """

    def setUp(self):
        """
        _setUp_

        Standard setup: Now with 100% more couch
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS", "WMCore.BossAir", "WMCore.ResourceControl", "WMCore.Agent.Database"])
        self.testInit.setupCouch("jobsubmitter_t/jobs", "JobDump")
        self.testInit.setupCouch("jobsubmitter_t/fwjrs", "FWJRDump")
        self.testInit.setupCouch("wmagent_summary_t", "WMStats")

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        self.baDaoFactory = DAOFactory(package = "WMCore.BossAir",
                                       logger = myThread.logger,
                                       dbinterface = myThread.dbi)

        self.testDir = self.testInit.generateWorkDir()

        # Set heartbeat
        self.componentName = 'JobSubmitter'
        self.heartbeatAPI = HeartbeatAPI(self.componentName)
        self.heartbeatAPI.registerComponent()
        self.configFile = EmulatorSetup.setupWMAgentConfig()

        return

    def tearDown(self):
        """
        _tearDown_

        Standard tearDown
        """
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()
        self.testInit.tearDownCouch()
        EmulatorSetup.deleteConfig(self.configFile)
        return

    def setResourceThresholds(self, site, **options):
        """
        _setResourceThresholds_

        Utility to set resource thresholds
        """
        if not options:
            options = {'state'        : 'Normal',
                       'runningSlots' : 10,
                       'pendingSlots' : 5,
                       'tasks' : ['Processing', 'Merge'],
                       'Processing' : {'pendingSlots' : 5,
                                       'runningSlots' : 10},
                       'Merge' : {'pendingSlots' : 2,
                                  'runningSlots' : 5}}

        resourceControl = ResourceControl()
        resourceControl.insertSite(siteName = site, pnn = 'se.%s' % (site),
                                   ceName = site, plugin = "MockPlugin", pendingSlots = options['pendingSlots'],
                                   runningSlots = options['runningSlots'], cmsName = site)
        for task in options['tasks']:
            resourceControl.insertThreshold(siteName = site, taskType = task,
                                            maxSlots = options[task]['runningSlots'],
                                            pendingSlots = options[task]['pendingSlots'])
        if options.get('state'):
            resourceControl.changeSiteState(site, options.get('state'))

        return

    def createJobGroups(self, nSubs, nJobs, task, workloadSpec, site,
                        taskType = 'Processing', name = None):
        """
        _createJobGroups_

        Creates a series of jobGroups for submissions
        """

        jobGroupList = []

        if name is None:
            name = makeUUID()

        testWorkflow = Workflow(spec = workloadSpec, owner = "tapas",
                                name = name, task = "basicWorkload/Production")
        testWorkflow.create()

        # Create subscriptions
        for _ in range(nSubs):

            name = makeUUID()

            # Create Fileset, Subscription, jobGroup
            testFileset = Fileset(name = name)
            testFileset.create()
            testSubscription = Subscription(fileset = testFileset,
                                            workflow = testWorkflow,
                                            type = taskType,
                                            split_algo = "FileBased")
            testSubscription.create()

            testJobGroup = JobGroup(subscription = testSubscription)
            testJobGroup.create()

            # Create jobs
            self.makeNJobs(name = name, task = task,
                           nJobs = nJobs,
                           jobGroup = testJobGroup,
                           fileset = testFileset,
                           sub = testSubscription.exists(),
                           site = site)

            testFileset.commit()
            testJobGroup.commit()
            jobGroupList.append(testJobGroup)

        return jobGroupList

    def makeNJobs(self, name, task, nJobs, jobGroup, fileset, sub, site):
        """
        _makeNJobs_

        Make and return a WMBS Job and File
        This handles all those damn add-ons

        """
        # Set the CacheDir
        cacheDir = os.path.join(self.testDir, 'CacheDir')

        for n in range(nJobs):
            # First make a file
            #site = self.sites[0]
            testFile = File(lfn = "/singleLfn/%s/%s" % (name, n),
                            size = 1024, events = 10)
            fileset.addFile(testFile)

        fileset.commit()

        location = None
        if isinstance(site, list):
            if len(site) > 0:
                location = site[0]
        else:
            location = site

        index = 0
        for f in fileset.files:
            index += 1
            testJob = Job(name = '%s-%i' % (name, index))
            testJob.addFile(f)
            testJob["location"] = location
            testJob["possiblePSN"] = set(site) if isinstance(site, list) else set([site])
            testJob['task'] = task.getPathName()
            testJob['sandbox'] = task.data.input.sandbox
            testJob['spec'] = os.path.join(self.testDir, 'basicWorkload.pcl')
            testJob['mask']['FirstEvent'] = 101
            testJob['priority'] = 101
            testJob['numberOfCores'] = 1
            jobCache = os.path.join(cacheDir, 'Sub_%i' % (sub), 'Job_%i' % (index))
            os.makedirs(jobCache)
            testJob.create(jobGroup)
            testJob['cache_dir'] = jobCache
            testJob.save()
            jobGroup.add(testJob)
            output = open(os.path.join(jobCache, 'job.pkl'), 'w')
            pickle.dump(testJob, output)
            output.close()

        return testJob, testFile

    def getConfig(self):
        """
        _getConfig_

        Gets a basic config from default location
        """

        config = self.testInit.getConfiguration()
        self.testInit.generateWorkDir(config)

        config.component_("Agent")
        config.Agent.WMSpecDirectory = self.testDir
        config.Agent.agentName       = 'testAgent'
        config.Agent.componentName   = self.componentName
        config.Agent.useHeartbeat    = False


        #First the general stuff
        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR", self.testDir)

        #Now the CoreDatabase information
        config.section_("CoreDatabase")
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.socket     = os.getenv("DBSOCK")

        # BossAir and MockPlugin configuration
        config.section_("BossAir")
        config.BossAir.pluginNames = ['MockPlugin']
        #Here Test the CondorPlugin instead of MockPlugin
        #config.BossAir.pluginNames = ['CondorPlugin']
        config.BossAir.pluginDir   = 'WMCore.BossAir.Plugins'
        config.BossAir.nCondorProcesses = 1
        config.BossAir.section_("MockPlugin")
        config.BossAir.MockPlugin.fakeReport = os.path.join(getTestBase(),
                                                         'WMComponent_t/JobSubmitter_t',
                                                         "submit.sh")


        # JobSubmitter configuration
        config.component_("JobSubmitter")
        config.JobSubmitter.logLevel      = 'DEBUG'
        config.JobSubmitter.maxThreads    = 1
        config.JobSubmitter.pollInterval  = 10
        config.JobSubmitter.submitScript  = os.path.join(getTestBase(),
                                                         'WMComponent_t/JobSubmitter_t',
                                                         'submit.sh')
        config.JobSubmitter.componentDir  = os.path.join(self.testDir, 'Components')
        config.JobSubmitter.workerThreads = 2
        config.JobSubmitter.jobsPerWorker = 200

        #JobStateMachine
        config.component_('JobStateMachine')
        config.JobStateMachine.couchurl        = os.getenv('COUCHURL')
        config.JobStateMachine.couchDBName     = "jobsubmitter_t"
        config.JobStateMachine.jobSummaryDBName = 'wmagent_summary_t'

        # Needed, because this is a test
        os.makedirs(config.JobSubmitter.componentDir)

        return config

    def createTestWorkload(self, workloadName = 'Tier1ReReco'):
        """
        _createTestWorkload_

        Creates a test workload for us to run on, hold the basic necessities.
        """

        workload = testWorkload(workloadName)

        taskMaker = TaskMaker(workload, os.path.join(self.testDir, 'workloadTest'))
        taskMaker.skipSubscription = True
        taskMaker.processWorkload()

        return workload

    def testA_BasicTest(self):
        """
        Use the MockPlugin to create a simple test
        Check to see that all the jobs were "submitted",
        don't care about thresholds
        """
        workloadName = "basicWorkload"
        workload = self.createTestWorkload()
        config = self.getConfig()
        changeState = ChangeState(config)

        nSubs = 2
        nJobs = 20
        site = "T2_US_UCSD"

        self.setResourceThresholds(site, pendingSlots = 50, runningSlots = 100, tasks = ['Processing', 'Merge'],
                                   Processing = {'pendingSlots' : 50, 'runningSlots' : 100},
                                   Merge = {'pendingSlots' : 50, 'runningSlots' : 100})

        jobGroupList = self.createJobGroups(nSubs = nSubs, nJobs = nJobs,
                                            task = workload.getTask("ReReco"),
                                            workloadSpec = os.path.join(self.testDir, 'workloadTest',
                                                                        workloadName),
                                            site = site)
        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        # Do pre-submit check
        getJobsAction = self.daoFactory(classname = "Jobs.GetAllJobs")
        result = getJobsAction.execute(state = 'Created', jobType = "Processing")
        self.assertEqual(len(result), nSubs * nJobs)

        jobSubmitter = JobSubmitterPoller(config = config)
        jobSubmitter.algorithm()

        # Check that jobs are in the right state
        result = getJobsAction.execute(state = 'Created', jobType = "Processing")
        self.assertEqual(len(result), 0)
        result = getJobsAction.execute(state = 'Executing', jobType = "Processing")
        self.assertEqual(len(result), nSubs * nJobs)

        # Check assigned locations
        getLocationAction = self.daoFactory(classname = "Jobs.GetLocation")
        for jobId in result:
            loc = getLocationAction.execute(jobid = jobId)
            self.assertEqual(loc, [['T2_US_UCSD']])

        # Run another cycle, it shouldn't submit anything. There isn't anything to submit
        jobSubmitter.algorithm()
        result = getJobsAction.execute(state = 'Created', jobType = "Processing")
        self.assertEqual(len(result), 0)
        result = getJobsAction.execute(state = 'Executing', jobType = "Processing")
        self.assertEqual(len(result), nSubs * nJobs)

        nSubs = 1
        nJobs = 10

        # Submit another 10 jobs
        jobGroupList = self.createJobGroups(nSubs = nSubs, nJobs = nJobs,
                                            task = workload.getTask("ReReco"),
                                            workloadSpec = os.path.join(self.testDir, 'workloadTest',
                                                                        workloadName),
                                            site = site,
                                            taskType = "Merge")
        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        # Check that the jobs are available for submission and run another cycle
        result = getJobsAction.execute(state = 'Created', jobType = "Merge")
        self.assertEqual(len(result), nSubs * nJobs)
        jobSubmitter.algorithm()

        #Check that the last 10 jobs were submitted as well.
        result = getJobsAction.execute(state = 'Created', jobType = "Merge")
        self.assertEqual(len(result), 0)
        result = getJobsAction.execute(state = 'Executing', jobType = "Merge")
        self.assertEqual(len(result), nSubs * nJobs)

        return

    def testB_thresholdTest(self):
        """
        _testB_thresholdTest_

        Check that the threshold management is working,
        this requires checks on pending/running jobs globally
        at a site and per task/site
        """
        workloadName = "basicWorkload"
        workload = self.createTestWorkload()
        config = self.getConfig()
        changeState = ChangeState(config)

        nSubs = 5
        nJobs = 10
        site = "T1_US_FNAL"

        self.setResourceThresholds(site, pendingSlots = 50, runningSlots = 200, tasks = ['Processing', 'Merge'],
                                   Processing = {'pendingSlots' : 45, 'runningSlots' :-1},
                                   Merge = {'pendingSlots' : 10, 'runningSlots' : 20, 'priority' : 5})

        # Always initialize the submitter after setting the sites, flaky!
        jobSubmitter = JobSubmitterPoller(config = config)

        jobGroupList = self.createJobGroups(nSubs = nSubs, nJobs = nJobs,
                                            task = workload.getTask("ReReco"),
                                            workloadSpec = os.path.join(self.testDir, 'workloadTest',
                                                                        workloadName),
                                            site = site)
        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        # Do pre-submit check
        getJobsAction = self.daoFactory(classname = "Jobs.GetAllJobs")
        result = getJobsAction.execute(state = 'Created', jobType = "Processing")
        self.assertEqual(len(result), nSubs * nJobs)

        jobSubmitter.algorithm()

        # Check that jobs are in the right state,
        # here we are limited by the pending threshold for the Processing task (45)
        result = getJobsAction.execute(state = 'Created', jobType = "Processing")
        self.assertEqual(len(result), 5)
        result = getJobsAction.execute(state = 'Executing', jobType = "Processing")
        self.assertEqual(len(result), 45)

        # Check assigned locations
        getLocationAction = self.daoFactory(classname = "Jobs.GetLocation")
        for jobId in result:
            loc = getLocationAction.execute(jobid = jobId)
            self.assertEqual(loc, [['T1_US_FNAL']])

        # Run another cycle, it shouldn't submit anything. Jobs are still in pending
        jobSubmitter.algorithm()
        result = getJobsAction.execute(state = 'Created', jobType = "Processing")
        self.assertEqual(len(result), 5)
        result = getJobsAction.execute(state = 'Executing', jobType = "Processing")
        self.assertEqual(len(result), 45)

        # Now put 10 Merge jobs, only 5 can be submitted, there we hit the global pending threshold for the site
        nSubs = 1
        nJobs = 10
        jobGroupList = self.createJobGroups(nSubs = nSubs, nJobs = nJobs,
                                            task = workload.getTask("ReReco"),
                                            workloadSpec = os.path.join(self.testDir, 'workloadTest',
                                                                        workloadName),
                                            site = site,
                                            taskType = 'Merge')
        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        jobSubmitter.algorithm()
        result = getJobsAction.execute(state = 'Created', jobType = "Merge")
        self.assertEqual(len(result), 5)
        result = getJobsAction.execute(state = 'Executing', jobType = "Merge")
        self.assertEqual(len(result), 5)
        result = getJobsAction.execute(state = 'Created', jobType = "Processing")
        self.assertEqual(len(result), 5)
        result = getJobsAction.execute(state = 'Executing', jobType = "Processing")
        self.assertEqual(len(result), 45)

        # Now let's test running thresholds
        # The scenario will be setup as follows: Move all current jobs as running
        # Create 300 Processing jobs and 300 merge jobs
        # Run 5 polling cycles, moving all pending jobs to running in between
        # Result is, merge is left at 25 running 0 pending and processing is left at 215 running 0 pending
        # Processing has 135 jobs in queue and Merge 285
        # This tests all threshold dynamics including the prioritization of merge over processing
        nSubs = 1
        nJobs = 300
        jobGroupList = self.createJobGroups(nSubs = nSubs, nJobs = nJobs,
                                            task = workload.getTask("ReReco"),
                                            workloadSpec = os.path.join(self.testDir, 'workloadTest',
                                                                        workloadName),
                                            site = site)
        jobGroupList.extend(self.createJobGroups(nSubs = nSubs, nJobs = nJobs,
                                            task = workload.getTask("ReReco"),
                                            workloadSpec = os.path.join(self.testDir, 'workloadTest',
                                                                        workloadName),
                                            site = site,
                                            taskType = 'Merge'))
        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        getRunJobID = self.baDaoFactory(classname = "LoadByWMBSID")
        setRunJobStatus = self.baDaoFactory(classname = "SetStatus")

        for _ in range(5):
            result = getJobsAction.execute(state = 'Executing')
            binds = []
            for jobId in result:
                binds.append({'id' : jobId, 'retry_count' : 0})
            runJobIds = getRunJobID.execute(binds)
            setRunJobStatus.execute([x['id'] for x in runJobIds], 'Running')
            jobSubmitter.algorithm()

        result = getJobsAction.execute(state = 'Executing', jobType = 'Processing')
        self.assertEqual(len(result), 215)
        result = getJobsAction.execute(state = 'Created', jobType = 'Processing')
        self.assertEqual(len(result), 135)
        result = getJobsAction.execute(state = 'Executing', jobType = 'Merge')
        self.assertEqual(len(result), 25)
        result = getJobsAction.execute(state = 'Created', jobType = 'Merge')
        self.assertEqual(len(result), 285)

        return

    def testC_prioritization(self):
        """
        _testC_prioritization_

        Check that jobs are prioritized by job type and by oldest workflow
        """
        workloadName = "basicWorkload"
        workload = self.createTestWorkload()
        config = self.getConfig()
        changeState = ChangeState(config)

        nSubs = 1
        nJobs = 10
        site = "T1_US_FNAL"

        self.setResourceThresholds(site, pendingSlots = 10, runningSlots = -1, tasks = ['Processing', 'Merge'],
                                   Processing = {'pendingSlots' : 50, 'runningSlots' :-1},
                                   Merge = {'pendingSlots' : 10, 'runningSlots' :-1, 'priority' : 5})

        # Always initialize the submitter after setting the sites, flaky!
        jobSubmitter = JobSubmitterPoller(config = config)

        jobGroupList = self.createJobGroups(nSubs = nSubs, nJobs = nJobs,
                                            task = workload.getTask("ReReco"),
                                            workloadSpec = os.path.join(self.testDir, 'workloadTest',
                                                                        workloadName),
                                            site = site,
                                            name = 'OldestWorkflow')
        jobGroupList.extend(self.createJobGroups(nSubs = nSubs, nJobs = nJobs,
                                            task = workload.getTask("ReReco"),
                                            workloadSpec = os.path.join(self.testDir, 'workloadTest',
                                                                        workloadName),
                                            site = site,
                                            taskType = 'Merge'))
        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        jobSubmitter.algorithm()

        # Merge goes first
        getJobsAction = self.daoFactory(classname = "Jobs.GetAllJobs")
        result = getJobsAction.execute(state = 'Created', jobType = "Merge")
        self.assertEqual(len(result), 0)
        result = getJobsAction.execute(state = 'Executing', jobType = "Merge")
        self.assertEqual(len(result), 10)
        result = getJobsAction.execute(state = 'Created', jobType = "Processing")
        self.assertEqual(len(result), 10)
        result = getJobsAction.execute(state = 'Executing', jobType = "Processing")
        self.assertEqual(len(result), 0)

        # Create a newer workflow processing, and after some new jobs for an old workflow

        jobGroupList = self.createJobGroups(nSubs = nSubs, nJobs = nJobs,
                                            task = workload.getTask("ReReco"),
                                            workloadSpec = os.path.join(self.testDir, 'workloadTest',
                                                                        workloadName),
                                            site = site,
                                            name = 'NewestWorkflow')

        jobGroupList.extend(self.createJobGroups(nSubs = nSubs, nJobs = nJobs,
                                    task = workload.getTask("ReReco"),
                                    workloadSpec = os.path.join(self.testDir, 'workloadTest',
                                                                workloadName),
                                    site = site,
                                    name = 'OldestWorkflow'))

        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        # Move pending jobs to running

        getRunJobID = self.baDaoFactory(classname = "LoadByWMBSID")
        setRunJobStatus = self.baDaoFactory(classname = "SetStatus")

        for idx in range(2):
            result = getJobsAction.execute(state = 'Executing')
            binds = []
            for jobId in result:
                binds.append({'id' : jobId, 'retry_count' : 0})
            runJobIds = getRunJobID.execute(binds)
            setRunJobStatus.execute([x['id'] for x in runJobIds], 'Running')

            # Run again on created workflows
            jobSubmitter.algorithm()

            result = getJobsAction.execute(state = 'Created', jobType = "Merge")
            self.assertEqual(len(result), 0)
            result = getJobsAction.execute(state = 'Executing', jobType = "Merge")
            self.assertEqual(len(result), 10)
            result = getJobsAction.execute(state = 'Created', jobType = "Processing")
            self.assertEqual(len(result), 30 - (idx + 1) * 10)
            result = getJobsAction.execute(state = 'Executing', jobType = "Processing")
            self.assertEqual(len(result), (idx + 1) * 10)

            # Check that older workflow goes first even with newer jobs
            getWorkflowAction = self.daoFactory(classname = "Jobs.GetWorkflowTask")
            workflows = getWorkflowAction.execute(result)
            for workflow in workflows:
                self.assertEqual(workflow['name'], 'OldestWorkflow')

        return

    def testD_SubmitFailed(self):
        """
        _testD_SubmitFailed_

        Check if jobs without a possible site to run at go to SubmitFailed
        """
        workloadName = "basicWorkload"
        workload = self.createTestWorkload()
        config = self.getConfig()
        changeState = ChangeState(config)

        nSubs = 2
        nJobs = 10

        jobGroupList = self.createJobGroups(nSubs = nSubs, nJobs = nJobs,
                                            task = workload.getTask("ReReco"),
                                            site = [],
                                            workloadSpec = os.path.join(self.testDir,
                                                                        'workloadTest',
                                                                        workloadName))

        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        jobSubmitter = JobSubmitterPoller(config = config)
        jobSubmitter.algorithm()

        # Jobs should go to submit failed
        getJobsAction = self.daoFactory(classname = "Jobs.GetAllJobs")
        result = getJobsAction.execute(state = 'SubmitFailed', jobType = "Processing")
        self.assertEqual(len(result), nSubs * nJobs)

        return

    def testE_SiteModesTest(self):
        """
        _testE_SiteModesTest_

        Test the behavior of the submitter in response to the different
        states of the sites
        """
        workloadName = "basicWorkload"
        workload = self.createTestWorkload()
        config = self.getConfig()
        changeState = ChangeState(config)
        nSubs = 1
        nJobs = 20

        sites = ['T2_US_Florida', 'T2_TW_Taiwan', 'T3_CO_Uniandes', 'T1_US_FNAL']
        for site in sites:
            self.setResourceThresholds(site, pendingSlots = 10, runningSlots = -1, tasks = ['Processing', 'Merge'],
                                       Processing = {'pendingSlots' : 10, 'runningSlots' :-1},
                                       Merge = {'pendingSlots' : 10, 'runningSlots' :-1, 'priority' : 5})

        myResourceControl = ResourceControl(config)
        myResourceControl.changeSiteState('T2_US_Florida', 'Draining')
        # First test that we prefer Normal over drain, and T1 over T2/T3
        jobGroupList = self.createJobGroups(nSubs = nSubs, nJobs = nJobs,
                                            site = [x for x in sites],
                                            task = workload.getTask("ReReco"),
                                            workloadSpec = os.path.join(self.testDir,
                                                                        'workloadTest',
                                                                        workloadName))
        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')
        jobSubmitter = JobSubmitterPoller(config = config)
        # Actually run it
        jobSubmitter.algorithm()

        getJobsAction = self.daoFactory(classname = "Jobs.GetAllJobs")
        result = getJobsAction.execute(state = 'Executing', jobType = "Processing")
        self.assertEqual(len(result), nSubs * nJobs)

        # All jobs should be at either FNAL, Taiwan or Uniandes. It's a random selection
        # Check assigned locations
        getLocationAction = self.daoFactory(classname = "Jobs.GetLocation")
        locationDict = getLocationAction.execute([{'jobid' : x} for x in result])
        for entry in locationDict:
            loc = entry['site_name']
            self.assertNotEqual(loc, 'T2_US_Florida')

        # Now set everything to down, check we don't submit anything
        for site in sites:
            myResourceControl.changeSiteState(site, 'Down')
        jobGroupList = self.createJobGroups(nSubs = nSubs, nJobs = nJobs,
                                            site = [x for x in sites],
                                            task = workload.getTask("ReReco"),
                                            workloadSpec = os.path.join(self.testDir,
                                                                        'workloadTest',
                                                                        workloadName))
        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')
        jobSubmitter.algorithm()
        # Nothing is submitted despite the empty slots at Uniandes and Florida
        result = getJobsAction.execute(state = 'Executing', jobType = "Processing")
        self.assertEqual(len(result), nSubs * nJobs)

        # Now set everything to Drain and create Merge jobs. Those should be submitted
        for site in sites:
            myResourceControl.changeSiteState(site, 'Draining')

        nSubsMerge = 1
        nJobsMerge = 5
        jobGroupList = self.createJobGroups(nSubs = nSubsMerge, nJobs = nJobsMerge,
                                            site = [x for x in sites],
                                            task = workload.getTask("ReReco"),
                                            workloadSpec = os.path.join(self.testDir,
                                                                        'workloadTest',
                                                                        workloadName),
                                            taskType = 'Merge')

        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        jobSubmitter.algorithm()

        result = getJobsAction.execute(state = 'Executing', jobType = 'Merge')
        self.assertEqual(len(result), nSubsMerge * nJobsMerge)

        # Now set everything to Aborted, and create Merge jobs. Those should fail
        # since the can only run at one place
        for site in sites:
            myResourceControl.changeSiteState(site, 'Aborted')

        nSubsMerge = 1
        nJobsMerge = 5
        jobGroupList = self.createJobGroups(nSubs = nSubsMerge, nJobs = nJobsMerge,
                                            site = [x for x in sites],
                                            task = workload.getTask("ReReco"),
                                            workloadSpec = os.path.join(self.testDir,
                                                                        'workloadTest',
                                                                        workloadName),
                                            taskType = 'Merge')

        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        jobSubmitter.algorithm()

        result = getJobsAction.execute(state = 'SubmitFailed', jobType = 'Merge')
        self.assertEqual(len(result), nSubsMerge * nJobsMerge)
        result = getJobsAction.execute(state = 'Executing', jobType = 'Processing')
        self.assertEqual(len(result), nSubs * nJobs)

        return

    @attr('integration')
    def testF_PollerProfileTest(self):
        """
        _testF_PollerProfileTest_

        Submit a lot of jobs and test how long it takes for
        them to actually be submitted
        """

        workloadName = "basicWorkload"
        workload = self.createTestWorkload()
        config = self.getConfig()
        changeState = ChangeState(config)

        nSubs = 100
        nJobs = 100
        site = "T1_US_FNAL"

        self.setResourceThresholds(site, pendingSlots = 20000, runningSlots = -1, tasks = ['Processing', 'Merge'],
                                   Processing = {'pendingSlots' : 10000, 'runningSlots' :-1},
                                   Merge = {'pendingSlots' : 10000, 'runningSlots' :-1, 'priority' : 5})

        # Always initialize the submitter after setting the sites, flaky!
        JobSubmitterPoller(config = config)

        jobGroupList = self.createJobGroups(nSubs = nSubs, nJobs = nJobs,
                                            task = workload.getTask("ReReco"),
                                            workloadSpec = os.path.join(self.testDir, 'workloadTest',
                                                                        workloadName),
                                            site = site)

        jobGroupList.extend(self.createJobGroups(nSubs = nSubs, nJobs = nJobs,
                                            task = workload.getTask("ReReco"),
                                            workloadSpec = os.path.join(self.testDir, 'workloadTest',
                                                                        workloadName),
                                            site = site,
                                            taskType = 'Merge'))

        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        # Actually run it
        startTime = time.time()
        cProfile.runctx("JobSubmitterPoller(config=config).algorithm()", globals(), locals(), filename="testStats.stat")
        stopTime = time.time()

        print("Job took %f seconds to complete" % (stopTime - startTime))

        p = pstats.Stats('testStats.stat')
        p.sort_stats('cumulative')
        p.print_stats()

        return

if __name__ == "__main__":
    unittest.main()
