#!/bin/env python
"""
_JobSubmitter_t_

JobSubmitter unit-test, uses the MockPlugin to submit and tests
the different dynamics that occur inside the JobSubmitter.
"""
from __future__ import print_function

from builtins import range
import cProfile
import os
import pickle
import pstats
import threading
import time
import unittest

from Utils.PythonVersion import PY3

from WMCore_t.WMSpec_t.TestSpec import testWorkload
from nose.plugins.attrib import attr

from WMComponent.JobSubmitter.JobSubmitterPoller import JobSubmitterPoller
from WMCore.Agent.HeartbeatAPI import HeartbeatAPI
from WMCore.DAOFactory import DAOFactory
from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.ResourceControl.ResourceControl import ResourceControl
from WMCore.Services.CRIC.CRIC import CRIC
from WMCore.Services.UUIDLib import makeUUID
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBase import getTestBase
from WMCore.WMSpec.Makers.TaskMaker import TaskMaker
from WMQuality.Emulators import EmulatorSetup
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMQuality.Emulators.LogDB.MockLogDB import MockLogDB
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit


class JobSubmitterTest(EmulatedUnitTestCase):
    """
    _JobSubmitterTest_

    Test class for the JobSubmitterPoller
    """

    def setUp(self):
        """
        _setUp_

        Standard setup: Now with 100% more couch
        """
        super(JobSubmitterTest, self).setUp()

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(
            customModules=["WMCore.WMBS", "WMCore.BossAir", "WMCore.ResourceControl", "WMCore.Agent.Database"])
        self.testInit.setupCouch("jobsubmitter_t/jobs", "JobDump")
        self.testInit.setupCouch("jobsubmitter_t/fwjrs", "FWJRDump")
        self.testInit.setupCouch("wmagent_summary_t", "WMStats")

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)
        self.baDaoFactory = DAOFactory(package="WMCore.BossAir",
                                       logger=myThread.logger,
                                       dbinterface=myThread.dbi)

        self.testDir = self.testInit.generateWorkDir()

        # Set heartbeat
        self.componentName = 'JobSubmitter'
        self.heartbeatAPI = HeartbeatAPI(self.componentName)
        self.heartbeatAPI.registerComponent()
        self.configFile = EmulatorSetup.setupWMAgentConfig()
        config = self.getConfig()
        myThread.logdbClient = MockLogDB(config.General.central_logdb_url,
                                         config.Agent.hostName, logger=None)

        if PY3:
            self.assertItemsEqual = self.assertCountEqual

        return

    def tearDown(self):
        """
        _tearDown_

        Standard tearDown
        """
        myThread = threading.currentThread()
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()
        self.testInit.tearDownCouch()
        EmulatorSetup.deleteConfig(self.configFile)
        myThread.logdbClient = None
        return

    def setResourceThresholds(self, site, **options):
        """
        _setResourceThresholds_

        Utility to set resource thresholds
        """
        if not options:
            options = {'state': 'Normal',
                       'runningSlots': 10,
                       'pendingSlots': 5,
                       'tasks': ['Processing', 'Merge'],
                       'Processing': {'pendingSlots': 5,
                                      'runningSlots': 10},
                       'Merge': {'pendingSlots': 2,
                                 'runningSlots': 5}}

        resourceControl = ResourceControl()
        resourceControl.insertSite(siteName=site, pnn='se.%s' % (site),
                                   ceName=site, plugin="MockPlugin", pendingSlots=options['pendingSlots'],
                                   runningSlots=options['runningSlots'], cmsName=site)
        for task in options['tasks']:
            resourceControl.insertThreshold(siteName=site, taskType=task,
                                            maxSlots=options[task]['runningSlots'],
                                            pendingSlots=options[task]['pendingSlots'])
        if options.get('state'):
            resourceControl.changeSiteState(site, options.get('state'))

        return

    def createJobGroups(self, nSubs, nJobs, task, workloadSpec, site,
                        taskType='Processing', name=None, wfPrio=1, changeState=None):
        """
        _createJobGroups_

        Creates a series of jobGroups for submissions
        changeState is an instance of the ChangeState class to make job status changes
        """

        jobGroupList = []

        if name is None:
            name = makeUUID()

        testWorkflow = Workflow(spec=workloadSpec, owner="tapas",
                                name=name, task="basicWorkload/Production",
                                priority=wfPrio)
        testWorkflow.create()

        # Create subscriptions
        for _ in range(nSubs):
            name = makeUUID()

            # Create Fileset, Subscription, jobGroup
            testFileset = Fileset(name=name)
            testFileset.create()
            testSubscription = Subscription(fileset=testFileset,
                                            workflow=testWorkflow,
                                            type=taskType,
                                            split_algo="FileBased")
            testSubscription.create()

            testJobGroup = JobGroup(subscription=testSubscription)
            testJobGroup.create()

            # Create jobs
            self.makeNJobs(name=name, task=task,
                           nJobs=nJobs,
                           jobGroup=testJobGroup,
                           fileset=testFileset,
                           sub=testSubscription.exists(),
                           site=site)

            testFileset.commit()
            testJobGroup.commit()
            jobGroupList.append(testJobGroup)

        if changeState:
            for group in jobGroupList:
                changeState.propagate(group.jobs, 'created', 'new')

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
            # site = self.sites[0]
            testFile = File(lfn="/singleLfn/%s/%s" % (name, n),
                            size=1024, events=10)
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
            testJob = Job(name='%s-%i' % (name, index))
            testJob.addFile(f)
            testJob["location"] = location
            testJob["possiblePSN"] = set(site) if isinstance(site, list) else set([site])
            testJob['task'] = task.getPathName()
            testJob['sandbox'] = task.data.input.sandbox
            testJob['spec'] = os.path.join(self.testDir, 'basicWorkload.pcl')
            testJob['mask']['FirstEvent'] = 101
            testJob['priority'] = 101
            testJob['numberOfCores'] = 1
            testJob['requestType'] = 'ReReco'
            jobCache = os.path.join(cacheDir, 'Sub_%i' % (sub), 'Job_%i' % (index))
            os.makedirs(jobCache)
            testJob.create(jobGroup)
            testJob['cache_dir'] = jobCache
            testJob.save()
            jobGroup.add(testJob)
            output = open(os.path.join(jobCache, 'job.pkl'), 'wb')
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
        config.Agent.agentName = 'testAgent'
        config.Agent.hostName = 'testAgent'
        config.Agent.componentName = self.componentName
        config.Agent.useHeartbeat = False
        config.Agent.isDocker = False

        # First the general stuff
        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR", self.testDir)
        config.General.central_logdb_url = "http://localhost/testlogdb"
        config.General.ReqMgr2ServiceURL = "http://localhost/reqmgr2"

        # Now the CoreDatabase information
        config.section_("CoreDatabase")
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.socket = os.getenv("DBSOCK")

        # BossAir and MockPlugin configuration
        config.section_("BossAir")
        config.BossAir.pluginNames = ['MockPlugin']
        # Here Test the CondorPlugin instead of MockPlugin
        # config.BossAir.pluginNames = ['CondorPlugin']
        config.BossAir.pluginDir = 'WMCore.BossAir.Plugins'
        config.BossAir.nCondorProcesses = 1
        config.BossAir.section_("MockPlugin")
        config.BossAir.MockPlugin.fakeReport = os.path.join(getTestBase(),
                                                            'WMComponent_t/JobSubmitter_t',
                                                            "submit.sh")

        # JobSubmitter configuration
        config.component_("JobSubmitter")
        config.JobSubmitter.logLevel = 'DEBUG'
        config.JobSubmitter.maxThreads = 1
        config.JobSubmitter.pollInterval = 10
        config.JobSubmitter.submitScript = os.path.join(getTestBase(),
                                                        'WMComponent_t/JobSubmitter_t',
                                                        'submit.sh')
        config.JobSubmitter.componentDir = os.path.join(self.testDir, 'Components')
        config.JobSubmitter.workerThreads = 2
        config.JobSubmitter.jobsPerWorker = 200
        config.JobSubmitter.drainGraceTime = 2  # in seconds

        # JobStateMachine
        config.component_('JobStateMachine')
        config.JobStateMachine.couchurl = os.getenv('COUCHURL')
        config.JobStateMachine.couchDBName = "jobsubmitter_t"
        config.JobStateMachine.jobSummaryDBName = 'wmagent_summary_t'

        # Needed, because this is a test
        try:
            os.makedirs(config.JobSubmitter.componentDir)
        except:
            pass

        return config

    def createTestWorkload(self, name='workloadTest'):
        """
        _createTestWorkload_

        Creates a test workload for us to run on, hold the basic necessities.
        """

        workload = testWorkload()

        taskMaker = TaskMaker(workload, os.path.join(self.testDir, name))
        taskMaker.skipSubscription = True
        taskMaker.processWorkload()
        self.workloadSpecPath = os.path.join(self.testDir, name,
                                             "%s/WMSandbox/WMWorkload.pkl" % name)

        return workload

    def testA_BasicTest(self):
        """
        Use the MockPlugin to create a simple test
        Check to see that all the jobs were "submitted",
        don't care about thresholds
        """
        workload = self.createTestWorkload()
        config = self.getConfig()
        changeState = ChangeState(config)

        nSubs = 2
        nJobs = 20
        site = "T2_US_UCSD"

        self.setResourceThresholds(site, pendingSlots=50, runningSlots=100, tasks=['Processing', 'Merge'],
                                   Processing={'pendingSlots': 50, 'runningSlots': 100},
                                   Merge={'pendingSlots': 50, 'runningSlots': 100})

        jobGroupList = self.createJobGroups(nSubs=nSubs, nJobs=nJobs,
                                            task=workload.getTask("ReReco"),
                                            workloadSpec=self.workloadSpecPath,
                                            site=site)
        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        # Do pre-submit check
        getJobsAction = self.daoFactory(classname="Jobs.GetAllJobs")
        result = getJobsAction.execute(state='Created', jobType="Processing")
        self.assertEqual(len(result), nSubs * nJobs)

        jobSubmitter = JobSubmitterPoller(config=config)
        jobSubmitter.algorithm()

        # Check that jobs are in the right state
        result = getJobsAction.execute(state='Created', jobType="Processing")
        self.assertEqual(len(result), 0)
        result = getJobsAction.execute(state='Executing', jobType="Processing")
        self.assertEqual(len(result), nSubs * nJobs)

        # Check assigned locations
        getLocationAction = self.daoFactory(classname="Jobs.GetLocation")
        for jobId in result:
            loc = getLocationAction.execute(jobid=jobId)
            self.assertEqual(loc, [['T2_US_UCSD']])

        # Run another cycle, it shouldn't submit anything. There isn't anything to submit
        jobSubmitter.algorithm()
        result = getJobsAction.execute(state='Created', jobType="Processing")
        self.assertEqual(len(result), 0)
        result = getJobsAction.execute(state='Executing', jobType="Processing")
        self.assertEqual(len(result), nSubs * nJobs)

        nSubs = 1
        nJobs = 10

        # Submit another 10 jobs
        jobGroupList = self.createJobGroups(nSubs=nSubs, nJobs=nJobs,
                                            task=workload.getTask("ReReco"),
                                            workloadSpec=self.workloadSpecPath,
                                            site=site,
                                            taskType="Merge")
        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        # Check that the jobs are available for submission and run another cycle
        result = getJobsAction.execute(state='Created', jobType="Merge")
        self.assertEqual(len(result), nSubs * nJobs)
        jobSubmitter.algorithm()

        # Check that the last 10 jobs were submitted as well.
        result = getJobsAction.execute(state='Created', jobType="Merge")
        self.assertEqual(len(result), 0)
        result = getJobsAction.execute(state='Executing', jobType="Merge")
        self.assertEqual(len(result), nSubs * nJobs)

        return

    def testB_thresholdTest(self):
        """
        _testB_thresholdTest_

        Check that the threshold management is working,
        this requires checks on pending/running jobs globally
        at a site and per task/site
        """
        workload = self.createTestWorkload()
        config = self.getConfig()
        changeState = ChangeState(config)

        nSubs = 5
        nJobs = 10
        site = "T1_US_FNAL"

        self.setResourceThresholds(site, pendingSlots=50, runningSlots=220, tasks=['Processing', 'Merge'],
                                   Processing={'pendingSlots': 45, 'runningSlots': 200},
                                   Merge={'pendingSlots': 10, 'runningSlots': 20, 'priority': 5})

        # Always initialize the submitter after setting the sites, flaky!
        jobSubmitter = JobSubmitterPoller(config=config)

        jobGroupList = self.createJobGroups(nSubs=nSubs, nJobs=nJobs,
                                            task=workload.getTask("ReReco"),
                                            workloadSpec=self.workloadSpecPath,
                                            site=site)
        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        # Do pre-submit check
        getJobsAction = self.daoFactory(classname="Jobs.GetAllJobs")
        result = getJobsAction.execute(state='Created', jobType="Processing")
        self.assertEqual(len(result), nSubs * nJobs)

        jobSubmitter.algorithm()

        # Check that jobs are in the right state,
        # here we are limited by the pending threshold for the Processing task (45)
        result = getJobsAction.execute(state='Created', jobType="Processing")
        self.assertEqual(len(result), 5)
        result = getJobsAction.execute(state='Executing', jobType="Processing")
        self.assertEqual(len(result), 45)

        # Check assigned locations
        getLocationAction = self.daoFactory(classname="Jobs.GetLocation")
        for jobId in result:
            loc = getLocationAction.execute(jobid=jobId)
            self.assertEqual(loc, [['T1_US_FNAL']])

        # Run another cycle, it shouldn't submit anything. Jobs are still in pending
        jobSubmitter.algorithm()
        result = getJobsAction.execute(state='Created', jobType="Processing")
        self.assertEqual(len(result), 5)
        result = getJobsAction.execute(state='Executing', jobType="Processing")
        self.assertEqual(len(result), 45)

        # Now put 10 Merge jobs, only 5 can be submitted, there we hit the global pending threshold for the site
        nSubs = 1
        nJobs = 10
        jobGroupList = self.createJobGroups(nSubs=nSubs, nJobs=nJobs,
                                            task=workload.getTask("ReReco"),
                                            workloadSpec=self.workloadSpecPath,
                                            site=site,
                                            taskType='Merge')
        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        jobSubmitter.algorithm()
        result = getJobsAction.execute(state='Created', jobType="Merge")
        self.assertEqual(len(result), 5)
        result = getJobsAction.execute(state='Executing', jobType="Merge")
        self.assertEqual(len(result), 5)
        result = getJobsAction.execute(state='Created', jobType="Processing")
        self.assertEqual(len(result), 5)
        result = getJobsAction.execute(state='Executing', jobType="Processing")
        self.assertEqual(len(result), 45)

        # Now let's test running thresholds
        # The scenario will be setup as follows: Move all current jobs as running
        # Create 300 Processing jobs and 300 merge jobs
        # Run 5 polling cycles, moving all pending jobs to running in between
        # Result is, merge is left at 30 running 0 pending and processing is left at 240 running 0 pending
        # Processing has 110 jobs in queue and Merge 280
        # This tests all threshold dynamics including the prioritization of merge over processing
        nSubs = 1
        nJobs = 300
        jobGroupList = self.createJobGroups(nSubs=nSubs, nJobs=nJobs,
                                            task=workload.getTask("ReReco"),
                                            workloadSpec=self.workloadSpecPath,
                                            site=site)
        jobGroupList.extend(self.createJobGroups(nSubs=nSubs, nJobs=nJobs,
                                                 task=workload.getTask("ReReco"),
                                                 workloadSpec=self.workloadSpecPath,
                                                 site=site,
                                                 taskType='Merge'))
        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        getRunJobID = self.baDaoFactory(classname="LoadByWMBSID")
        setRunJobStatus = self.baDaoFactory(classname="SetStatus")

        for i in range(5):
            result = getJobsAction.execute(state='Executing')
            binds = []
            for jobId in result:
                binds.append({'id': jobId, 'retry_count': 0})
            runJobIds = getRunJobID.execute(binds)
            setRunJobStatus.execute([x['id'] for x in runJobIds], 'Running')
            jobSubmitter.algorithm()

        result = getJobsAction.execute(state='Executing', jobType='Processing')
        self.assertEqual(len(result), 240)
        result = getJobsAction.execute(state='Created', jobType='Processing')
        self.assertEqual(len(result), 110)
        result = getJobsAction.execute(state='Executing', jobType='Merge')
        self.assertEqual(len(result), 30)
        result = getJobsAction.execute(state='Created', jobType='Merge')
        self.assertEqual(len(result), 280)

        return

    def testC_prioTest(self):
        """
        _testC_prioTest_

        Test whether the correct job type, workflow and task id priorities
        are respected in the DAO
        """
        workload1 = self.createTestWorkload(name='testWorkload1')
        workload2 = self.createTestWorkload(name='testWorkload2')
        workload3 = self.createTestWorkload(name='testWorkload3')
        workload4 = self.createTestWorkload(name='testWorkload4')

        config = self.getConfig()
        changeState = ChangeState(config)
        getJobsAction = self.daoFactory(classname="Jobs.ListForSubmitter")

        site = "T1_US_FNAL"
        self.setResourceThresholds(site, pendingSlots=1000, runningSlots=1000,
                                   tasks=['Processing', 'Merge', 'Production', 'Harvesting', 'LogCollect'],
                                   Processing={'pendingSlots': 1000, 'runningSlots': 1000},
                                   Merge={'pendingSlots': 1000, 'runningSlots': 10000},
                                   Production={'pendingSlots': 1000, 'runningSlots': 1000},
                                   Harvesting={'pendingSlots': 1000, 'runningSlots': 1000},
                                   LogCollect={'pendingSlots': 1000, 'runningSlots': 1000})

        nSubs = 1
        nJobs = 5
        jobGroupList = []
        jobGroup = self.createJobGroups(nSubs=nSubs, nJobs=nJobs,
                                        task=workload1.getTask("ReReco"),
                                        workloadSpec=self.workloadSpecPath,
                                        site=site,
                                        name='OldestWorkflow')  # task_id = 1
        jobGroupList.extend(jobGroup)
        jobGroup = self.createJobGroups(nSubs=nSubs, nJobs=nJobs,
                                        task=workload1.getTask("ReReco"),
                                        workloadSpec=self.workloadSpecPath,
                                        site=site,
                                        taskType='Merge')  # task_id = 2
        jobGroupList.extend(jobGroup)
        jobGroup = self.createJobGroups(nSubs=nSubs, nJobs=nJobs,
                                        task=workload1.getTask("ReReco"),
                                        workloadSpec=self.workloadSpecPath,
                                        site=site,
                                        taskType='LogCollect')  # task_id = 3
        jobGroupList.extend(jobGroup)

        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        # retrieve all 15 jobs created so far
        result = getJobsAction.execute(limitRows=100)
        self.assertItemsEqual([int(j['task_prio']) for j in result],
                              [4] * 5 + [2] * 5 + [0] * 5)
        self.assertItemsEqual([int(j['wf_priority']) for j in result],
                              [1] * 15)
        self.assertItemsEqual([int(j['task_id']) for j in result],
                              [2] * 5 + [3] * 5 + [1] * 5)

        # now retrieve only 6 jobs (5 Merge and 1 LogCollect), wf prio=1
        result = getJobsAction.execute(limitRows=6)
        self.assertItemsEqual([int(j['task_prio']) for j in result], [4] * 5 + [2] * 1)

        jobGroupList = []
        jobGroup = self.createJobGroups(nSubs=nSubs, nJobs=nJobs, wfPrio=2,
                                        task=workload2.getTask("ReReco"),
                                        workloadSpec=self.workloadSpecPath,
                                        site=site, taskType='Merge')  # task_id = 4
        jobGroupList.extend(jobGroup)
        jobGroup = self.createJobGroups(nSubs=nSubs, nJobs=nJobs, wfPrio=3,
                                        task=workload3.getTask("ReReco"),
                                        workloadSpec=self.workloadSpecPath,
                                        site=site, taskType='Processing')  # task_id = 5
        jobGroupList.extend(jobGroup)
        jobGroup = self.createJobGroups(nSubs=nSubs, nJobs=nJobs, wfPrio=3,
                                        task=workload3.getTask("ReReco"),
                                        workloadSpec=self.workloadSpecPath,
                                        site=site, taskType='LogCollect')  # task_id = 6
        jobGroupList.extend(jobGroup)

        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        # retrieve all 30 jobs created so far
        result = getJobsAction.execute(limitRows=100)
        self.assertItemsEqual([int(j['task_prio']) for j in result],
                              [4] * 10 + [2] * 10 + [0] * 10)
        # merge prio 2, merge prio 1, logCol prio 3, logCol prio 1, proc prio 3, proc prio 1
        self.assertItemsEqual([int(j['wf_priority']) for j in result],
                              [2] * 5 + [1] * 5 + [3] * 5 + [1] * 5 + [3] * 5 + [1] * 5)
        # merge id 4, merge id 2, logCol id 6, logCol id 3, proc id 5, proc id 1
        self.assertItemsEqual([int(j['task_id']) for j in result],
                              [4] * 5 + [2] * 5 + [6] * 5 + [3] * 5 + [5] * 5 + [1] * 5)

        jobGroupList = []
        jobGroup = self.createJobGroups(nSubs=nSubs, nJobs=nJobs, wfPrio=2,
                                        task=workload4.getTask("ReReco"),
                                        workloadSpec=self.workloadSpecPath,
                                        site=site, taskType='Merge')  # task_id = 7
        jobGroupList.extend(jobGroup)

        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        # retrieve all 15 Merge jobs created so far
        result = getJobsAction.execute(limitRows=15)
        self.assertItemsEqual([int(j['task_prio']) for j in result], [4] * 15)
        # merge prio 2, merge prio 2, merge prio 1
        self.assertItemsEqual([int(j['wf_priority']) for j in result], [2] * 10 + [1] * 5)
        # merge id 7, merge id 4, merge id 2
        self.assertItemsEqual([int(j['task_id']) for j in result],
                              [7] * 5 + [4] * 5 + [2] * 5)

    def testC_prioritization(self):
        """
        _testC_prioritization_

        Check that jobs are prioritized by job type and by oldest workflow
        """
        workload = self.createTestWorkload()
        config = self.getConfig()
        changeState = ChangeState(config)

        nSubs = 1
        nJobs = 10
        site = "T1_US_FNAL"

        self.setResourceThresholds(site, pendingSlots=10, runningSlots=10000, tasks=['Processing', 'Merge'],
                                   Processing={'pendingSlots': 50, 'runningSlots': 10000},
                                   Merge={'pendingSlots': 10, 'runningSlots': 10000, 'priority': 5})

        # Always initialize the submitter after setting the sites, flaky!
        jobSubmitter = JobSubmitterPoller(config=config)

        jobGroupList = self.createJobGroups(nSubs=nSubs, nJobs=nJobs,
                                            task=workload.getTask("ReReco"),
                                            workloadSpec=self.workloadSpecPath,
                                            site=site,
                                            name='OldestWorkflow')
        jobGroupList.extend(self.createJobGroups(nSubs=nSubs, nJobs=nJobs,
                                                 task=workload.getTask("ReReco"),
                                                 workloadSpec=self.workloadSpecPath,
                                                 site=site,
                                                 taskType='Merge'))
        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        jobSubmitter.algorithm()

        # Merge goes first
        getJobsAction = self.daoFactory(classname="Jobs.GetAllJobs")
        result = getJobsAction.execute(state='Created', jobType="Merge")
        self.assertEqual(len(result), 0)
        result = getJobsAction.execute(state='Executing', jobType="Merge")
        self.assertEqual(len(result), 10)
        result = getJobsAction.execute(state='Created', jobType="Processing")
        self.assertEqual(len(result), 10)
        result = getJobsAction.execute(state='Executing', jobType="Processing")
        self.assertEqual(len(result), 0)

        # Create a newer workflow processing, and after some new jobs for an old workflow

        jobGroupList = self.createJobGroups(nSubs=nSubs, nJobs=nJobs,
                                            task=workload.getTask("ReReco"),
                                            workloadSpec=self.workloadSpecPath,
                                            site=site,
                                            name='OldestWorkflow')
        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        jobGroupList = self.createJobGroups(nSubs=nSubs, nJobs=nJobs,
                                            task=workload.getTask("ReReco"),
                                            workloadSpec=self.workloadSpecPath,
                                            site=site,
                                            name='NewestWorkflow')
        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        # Move pending jobs to running

        getRunJobID = self.baDaoFactory(classname="LoadByWMBSID")
        setRunJobStatus = self.baDaoFactory(classname="SetStatus")

        for idx in range(2):
            result = getJobsAction.execute(state='Executing')
            binds = []
            for jobId in result:
                binds.append({'id': jobId, 'retry_count': 0})
            runJobIds = getRunJobID.execute(binds)
            setRunJobStatus.execute([x['id'] for x in runJobIds], 'Running')

            # Run again on created workflows
            jobSubmitter.algorithm()

            result = getJobsAction.execute(state='Created', jobType="Merge")
            self.assertEqual(len(result), 0)
            result = getJobsAction.execute(state='Executing', jobType="Merge")
            self.assertEqual(len(result), 10)
            result = getJobsAction.execute(state='Created', jobType="Processing")
            self.assertEqual(len(result), 30 - (idx + 1) * 10)
            result = getJobsAction.execute(state='Executing', jobType="Processing")
            self.assertEqual(len(result), (idx + 1) * 10)

            # Check that older workflow goes first even with newer jobs
            getWorkflowAction = self.daoFactory(classname="Jobs.GetWorkflowTask")
            workflows = getWorkflowAction.execute(result)
            for workflow in workflows:
                self.assertEqual(workflow['name'], 'OldestWorkflow')

        return

    def testD_SubmitFailed(self):
        """
        _testD_SubmitFailed_

        Check if jobs without a possible site to run at go to SubmitFailed
        """
        workload = self.createTestWorkload()
        config = self.getConfig()
        changeState = ChangeState(config)

        nSubs = 2
        nJobs = 10

        jobGroupList = self.createJobGroups(nSubs=nSubs, nJobs=nJobs,
                                            task=workload.getTask("ReReco"),
                                            site=[],
                                            workloadSpec=self.workloadSpecPath)

        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        jobSubmitter = JobSubmitterPoller(config=config)
        jobSubmitter.algorithm()

        # Jobs should go to submit failed
        getJobsAction = self.daoFactory(classname="Jobs.GetAllJobs")
        result = getJobsAction.execute(state='SubmitFailed', jobType="Processing")
        self.assertEqual(len(result), nSubs * nJobs)

        return

    def testE_SiteModesTest(self):
        """
        _testE_SiteModesTest_

        Test the behavior of the submitter in response to the different
        states of the sites
        """
        workload = self.createTestWorkload()
        config = self.getConfig()
        changeState = ChangeState(config)
        nSubs = 1
        nJobs = 20

        sites = ['T2_US_Florida', 'T2_RU_INR', 'T3_CO_Uniandes', 'T1_US_FNAL']
        for site in sites:
            self.setResourceThresholds(site, pendingSlots=10, runningSlots=999999, tasks=['Processing', 'Merge'],
                                       Processing={'pendingSlots': 10, 'runningSlots': 999999},
                                       Merge={'pendingSlots': 10, 'runningSlots': 999999, 'priority': 5})

        myResourceControl = ResourceControl(config)
        myResourceControl.changeSiteState('T2_US_Florida', 'Draining')
        # First test that we prefer Normal over drain, and T1 over T2/T3
        jobGroupList = self.createJobGroups(nSubs=nSubs, nJobs=nJobs,
                                            site=[x for x in sites],
                                            task=workload.getTask("ReReco"),
                                            workloadSpec=self.workloadSpecPath)
        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')
        jobSubmitter = JobSubmitterPoller(config=config)
        # Actually run it
        jobSubmitter.algorithm()

        getJobsAction = self.daoFactory(classname="Jobs.GetAllJobs")
        result = getJobsAction.execute(state='Executing', jobType="Processing")
        self.assertEqual(len(result), nSubs * nJobs)

        # All jobs should be at either FNAL, Taiwan or Uniandes. It's a random selection
        # Check assigned locations
        getLocationAction = self.daoFactory(classname="Jobs.GetLocation")
        locationDict = getLocationAction.execute([{'jobid': x} for x in result])
        for entry in locationDict:
            loc = entry['site_name']
            self.assertNotEqual(loc, 'T2_US_Florida')

        # Now set everything to down, check we don't submit anything
        for site in sites:
            myResourceControl.changeSiteState(site, 'Down')
        jobGroupList = self.createJobGroups(nSubs=nSubs, nJobs=nJobs,
                                            site=[x for x in sites],
                                            task=workload.getTask("ReReco"),
                                            workloadSpec=self.workloadSpecPath)
        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')
        jobSubmitter.algorithm()
        # Nothing is submitted despite the empty slots at Uniandes and Florida
        result = getJobsAction.execute(state='Executing', jobType="Processing")
        self.assertEqual(len(result), nSubs * nJobs)

        # Now set everything to Drain and create Merge jobs. Those should be submitted
        for site in sites:
            myResourceControl.changeSiteState(site, 'Draining')

        nSubsMerge = 1
        nJobsMerge = 5
        jobGroupList = self.createJobGroups(nSubs=nSubsMerge, nJobs=nJobsMerge,
                                            site=[x for x in sites],
                                            task=workload.getTask("ReReco"),
                                            workloadSpec=self.workloadSpecPath,
                                            taskType='Merge')

        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        jobSubmitter.algorithm()

        result = getJobsAction.execute(state='Executing', jobType='Merge')
        self.assertEqual(len(result), nSubsMerge * nJobsMerge)

        # Now set everything to Aborted, and create Merge jobs. Those should fail
        # since the can only run at one place
        for site in sites:
            myResourceControl.changeSiteState(site, 'Aborted')

        nSubsMerge = 1
        nJobsMerge = 5
        jobGroupList = self.createJobGroups(nSubs=nSubsMerge, nJobs=nJobsMerge,
                                            site=[x for x in sites],
                                            task=workload.getTask("ReReco"),
                                            workloadSpec=self.workloadSpecPath,
                                            taskType='Merge')

        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        jobSubmitter.algorithm()

        result = getJobsAction.execute(state='SubmitFailed', jobType='Merge')
        self.assertEqual(len(result), nSubsMerge * nJobsMerge)
        result = getJobsAction.execute(state='Executing', jobType='Processing')
        self.assertEqual(len(result), nSubs * nJobs)

        return

    def testJobSiteDrain(self):
        """
        _testJobSiteDrain_

        Test the behavior of jobs pending to a single site that is in drain mode
        """
        workload = self.createTestWorkload()
        config = self.getConfig()
        jobSubmitter = JobSubmitterPoller(config=config)
        myResourceControl = ResourceControl(config)
        changeState = ChangeState(config)
        getJobsAction = self.daoFactory(classname="Jobs.GetAllJobs")

        nSubs = 1
        nJobs = 30

        site = 'T2_US_Nebraska'
        self.setResourceThresholds(site, pendingSlots=100, runningSlots=100,
                                   tasks=['Processing', 'Merge'],
                                   Processing={'pendingSlots': 10, 'runningSlots': 10},
                                   Merge={'pendingSlots': 10, 'runningSlots': 10, 'priority': 5})

        jobGroupList = self.createJobGroups(nSubs=nSubs, nJobs=nJobs,
                                            site=[site],
                                            task=workload.getTask("ReReco"),
                                            workloadSpec=self.workloadSpecPath)
        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        # submit first 10 jobs
        jobSubmitter.algorithm()

        result = getJobsAction.execute(state='Executing', jobType="Processing")
        self.assertEqual(len(result), 10)

        myResourceControl.changeSiteState(site, 'Draining')

        # site is now in drain, so don't submit anything
        jobSubmitter.algorithm()

        # jobs were supposed to get killed, but I guess the MockPlugin doesnt do anything
        result = getJobsAction.execute(state='Executing', jobType="Processing")
        self.assertEqual(len(result), 10)
        result = getJobsAction.execute(state='created', jobType="Processing")
        self.assertEqual(len(result), 20)
        result = getJobsAction.execute(state='submitfailed', jobType="Processing")
        self.assertEqual(len(result), 0)

        # make sure the drain grace period expires...
        time.sleep(3)
        jobSubmitter.algorithm()

        result = getJobsAction.execute(state='Executing', jobType="Processing")
        self.assertEqual(len(result), 10)
        # the remaining jobs should have gone to submitfailed by now
        result = getJobsAction.execute(state='submitfailed', jobType="Processing")
        self.assertEqual(len(result), 20)
        result = getJobsAction.execute(state='created', jobType="Processing")
        self.assertEqual(len(result), 0)

    @attr('integration')
    def testF_PollerProfileTest(self):
        """
        _testF_PollerProfileTest_

        Submit a lot of jobs and test how long it takes for
        them to actually be submitted
        """

        workload = self.createTestWorkload()
        config = self.getConfig()
        changeState = ChangeState(config)

        nSubs = 100
        nJobs = 100
        site = "T1_US_FNAL"

        self.setResourceThresholds(site, pendingSlots=20000, runningSlots=999999, tasks=['Processing', 'Merge'],
                                   Processing={'pendingSlots': 10000, 'runningSlots': 999999},
                                   Merge={'pendingSlots': 10000, 'runningSlots': 999999, 'priority': 5})

        # Always initialize the submitter after setting the sites, flaky!
        JobSubmitterPoller(config=config)

        jobGroupList = self.createJobGroups(nSubs=nSubs, nJobs=nJobs,
                                            task=workload.getTask("ReReco"),
                                            workloadSpec=self.workloadSpecPath,
                                            site=site)

        jobGroupList.extend(self.createJobGroups(nSubs=nSubs, nJobs=nJobs,
                                                 task=workload.getTask("ReReco"),
                                                 workloadSpec=self.workloadSpecPath,
                                                 site=site,
                                                 taskType='Merge'))

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

    @attr('integration')
    def testMemoryProfile(self):
        """
        _testMemoryProfile_

        Creates 20k jobs and keep refreshing the cache and submitting
        them between the components cycle

        Example using memory_profiler library, unfortunately the source
        code has to be updated with decorators.
        NOTE: Never run it on jenkins
        """
        workload = self.createTestWorkload()
        config = self.getConfig()
        changeState = ChangeState(config)
        # myResourceControl = ResourceControl(config)

        nSubs = 20
        nJobs = 100

        sites = ['T2_US_Florida', 'T2_RU_INR', 'T3_CO_Uniandes', 'T1_US_FNAL']
        allSites = CRIC().PSNtoPNNMap('*')

        for site in allSites:
            self.setResourceThresholds(site, pendingSlots=20000, runningSlots=999999, tasks=['Processing', 'Merge'],
                                       Processing={'pendingSlots': 10000, 'runningSlots': 999999},
                                       Merge={'pendingSlots': 10000, 'runningSlots': 999999, 'priority': 5})

        # Always initialize the submitter after setting the sites, flaky!
        jobSubmitter = JobSubmitterPoller(config=config)

        self.createJobGroups(nSubs=nSubs, nJobs=nJobs, wfPrio=10,
                             task=workload.getTask("ReReco"),
                             workloadSpec=self.workloadSpecPath,
                             site=[x for x in sites], changeState=changeState)

        # Actually run it
        jobSubmitter.algorithm()  # cycle 1

        self.createJobGroups(nSubs=nSubs, nJobs=nJobs, wfPrio=10,
                             task=workload.getTask("ReReco"),
                             workloadSpec=self.workloadSpecPath,
                             site=[x for x in sites], changeState=changeState)
        # myResourceControl.changeSiteState('T2_US_Florida', 'Draining')
        jobSubmitter.algorithm()  # cycle 2

        self.createJobGroups(nSubs=nSubs, nJobs=nJobs, wfPrio=10,
                             task=workload.getTask("ReReco"),
                             workloadSpec=self.workloadSpecPath,
                             site=[x for x in sites], changeState=changeState)
        # myResourceControl.changeSiteState('T2_RU_INR', 'Draining')
        jobSubmitter.algorithm()  # cycle 3

        self.createJobGroups(nSubs=nSubs, nJobs=nJobs, wfPrio=10,
                             task=workload.getTask("ReReco"),
                             workloadSpec=self.workloadSpecPath,
                             site=[x for x in sites], changeState=changeState)
        # myResourceControl.changeSiteState('T3_CO_Uniandes', 'Draining')
        jobSubmitter.algorithm()  # cycle 4

        # myResourceControl.changeSiteState('T2_RU_INR', 'Normal')
        jobSubmitter.algorithm()  # cycle 5

        # myResourceControl.changeSiteState('T2_US_Florida', 'Normal')
        jobSubmitter.algorithm()  # cycle 6

        # myResourceControl.changeSiteState('T2_RU_INR', 'Normal')
        jobSubmitter.algorithm()  # cycle 7

        # myResourceControl.changeSiteState('T3_CO_Uniandes', 'Normal')
        jobSubmitter.algorithm()  # cycle 8
        jobSubmitter.algorithm()  # cycle 9, nothing to submit

        return


if __name__ == "__main__":
    unittest.main()
