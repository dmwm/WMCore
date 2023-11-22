#!/usr/bin/env python

"""
BossAir preliminary test
"""
from __future__ import print_function

import getpass
import os.path
import subprocess
import threading
import unittest
import pickle

from nose.plugins.attrib import attr

import WMCore.WMInit
from WMCore.DAOFactory import DAOFactory
from WMCore.Services.UUIDLib import makeUUID
from WMCore.WMSpec.Makers.TaskMaker import TaskMaker
from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.ResourceControl.ResourceControl import ResourceControl
from WMCore.Agent.HeartbeatAPI import HeartbeatAPI
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Job import Job
from WMCore.BossAir.BossAirAPI import BossAirAPI, BossAirException

from WMCore_t.WMSpec_t.TestSpec import createTestWorkload
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit


def getNArcJobs():
    """
    Return the number of active ARC jobs.

    """

    cmd = "wc -l ~/.ngjobs"
    pipe = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, shell=True)
    stdout, _ = pipe.communicate()
    nJobs = int(stdout.split()[0])
    return nJobs


def getCondorRunningJobs(user):
    """
    _getCondorRunningJobs_

    Return the number of jobs currently running for a user
    """

    command = ['condor_q', user]
    pipe = subprocess.Popen(command, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, shell=False)
    stdout, _ = pipe.communicate()

    output = stdout.split('\n')[-2]

    nJobs = int(output.split(';')[0].split()[0])

    return nJobs


class BossAirTest(unittest.TestCase):
    """
    Tests for the BossAir prototype

    """

    sites = ['T2_US_UCSD', 'T2_TW_Taiwan', 'T1_CH_CERN', 'T2_US_Florida']

    def setUp(self):
        """
        setup for test.
        """

        myThread = threading.currentThread()

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection(destroyAllDatabase=True)
        self.tearDown()
        self.testInit.setSchema(
            customModules=["WMCore.WMBS", "WMCore.BossAir", "WMCore.ResourceControl", "WMCore.Agent.Database"],
            useDefault=False)
        self.testInit.setupCouch("bossair_t/jobs", "JobDump")
        self.testInit.setupCouch("bossair_t/fwjrs", "FWJRDump")

        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)
        self.getJobs = self.daoFactory(classname="Jobs.GetAllJobs")

        # Create sites in resourceControl
        resourceControl = ResourceControl()
        for site in self.sites:
            resourceControl.insertSite(siteName=site, pnn='%s_PNN' % site, cmsName=site,
                                       ceName=site, plugin="SimpleCondorPlugin", pendingSlots=1000,
                                       runningSlots=2000)
            resourceControl.insertThreshold(siteName=site, taskType='Processing',
                                            maxSlots=1000, pendingSlots=1000)

        site = 'T3_US_Xanadu'
        resourceControl.insertSite(siteName=site, pnn='%s_PNN' % site, cmsName=site,
                                   ceName=site, plugin="TestPlugin")
        resourceControl.insertThreshold(siteName=site, taskType='Processing',
                                        maxSlots=10000, pendingSlots=10000)

        # Create user
        newuser = self.daoFactory(classname="Users.New")
        newuser.execute(dn="tapas", group_name="phgroup", role_name="cmsrole")

        # We actually need the user name
        self.user = getpass.getuser()

        # Change this to the working dir to keep track of error and log files from condor
        self.testDir = self.testInit.generateWorkDir()

        # Set heartbeat
        componentName = 'test'
        self.heartbeatAPI = HeartbeatAPI(componentName)
        self.heartbeatAPI.registerComponent()
        componentName = 'JobTracker'
        self.heartbeatAPI2 = HeartbeatAPI(componentName)
        self.heartbeatAPI2.registerComponent()

        return

    def tearDown(self):
        """
        Database deletion
        """
        # self.testInit.clearDatabase(modules = ["WMCore.WMBS", "WMCore.BossAir", "WMCore.ResourceControl", "WMCore.Agent.Database"])

        self.testInit.delWorkDir()

        self.testInit.tearDownCouch()

        return

    def getConfig(self):
        """
        _getConfig_

        Build a basic BossAir config
        """

        config = self.testInit.getConfiguration()

        config.section_("Agent")
        config.Agent.agentName = 'testAgent'
        config.Agent.componentName = 'test'
        config.Agent.useHeartbeat = False
        config.Agent.isDocker = False

        config.section_("CoreDatabase")
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.socket = os.getenv("DBSOCK")

        config.section_("BossAir")
        config.BossAir.pluginNames = ['TestPlugin', 'SimpleCondorPlugin']
        config.BossAir.pluginDir = 'WMCore.BossAir.Plugins'
        config.BossAir.UISetupScript = '/afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.sh'

        config.component_("JobSubmitter")
        config.JobSubmitter.logLevel = 'INFO'
        config.JobSubmitter.pollInterval = 1
        config.JobSubmitter.pluginName = 'SimpleCondorPlugin'
        config.JobSubmitter.pluginDir = 'JobSubmitter.Plugins'
        config.JobSubmitter.submitDir = os.path.join(self.testDir, 'submit')
        config.JobSubmitter.submitNode = os.getenv("HOSTNAME", 'stevia.hep.wisc.edu')
        config.JobSubmitter.submitScript = os.path.join(WMCore.WMInit.getWMBASE(),
                                                        'test/python/WMComponent_t/JobSubmitter_t',
                                                        'submit.sh')
        config.JobSubmitter.componentDir = os.path.join(os.getcwd(), 'Components')
        config.JobSubmitter.workerThreads = 2
        config.JobSubmitter.jobsPerWorker = 200
        config.JobSubmitter.gLiteConf = os.path.join(os.getcwd(), 'config.cfg')

        # JobTracker
        config.component_("JobTracker")
        config.JobTracker.logLevel = 'INFO'
        config.JobTracker.pollInterval = 1

        # JobStateMachine
        config.component_('JobStateMachine')
        config.JobStateMachine.couchurl = os.getenv('COUCHURL')
        config.JobStateMachine.couchDBName = "bossair_t"

        # JobStatusLite
        config.component_('JobStatusLite')
        config.JobStatusLite.componentDir = os.path.join(os.getcwd(), 'Components')
        config.JobStatusLite.stateTimeouts = {'Pending': 10, 'Running': 86400}
        config.JobStatusLite.pollInterval = 1

        return config

    def createTestWorkload(self, workloadName='Test', emulator=True):
        """
        Creates a test workload for us to run on, hold the basic necessities.
        """

        workload = createTestWorkload("Tier1ReReco")
        rereco = workload.getTask("ReReco")

        taskMaker = TaskMaker(workload, os.path.join(self.testDir, 'workloadTest'))
        taskMaker.skipSubscription = True
        taskMaker.processWorkload()

        workload.save(workloadName)

        return workload

    def createJobGroups(self, nSubs, nJobs, task, workloadSpec, site=None, bl=[], wl=[]):
        """
        Creates a series of jobGroups for submissions

        """

        jobGroupList = []

        testWorkflow = Workflow(spec=workloadSpec, owner="tapas",
                                name=makeUUID(), task="basicWorkload/Production",
                                owner_vogroup='phgroup', owner_vorole='cmsrole')
        testWorkflow.create()

        # Create subscriptions
        for i in range(nSubs):
            name = makeUUID()

            # Create Fileset, Subscription, jobGroup
            testFileset = Fileset(name=name)
            testFileset.create()
            testSubscription = Subscription(fileset=testFileset,
                                            workflow=testWorkflow,
                                            type="Processing",
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
                           site=site, bl=bl, wl=wl)

            testFileset.commit()
            testJobGroup.commit()
            jobGroupList.append(testJobGroup)

        return jobGroupList

    def makeNJobs(self, name, task, nJobs, jobGroup, fileset, sub, site=None, bl=[], wl=[]):
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
            if site:
                testFile.setLocation(site)
            else:
                for tmpSite in self.sites:
                    testFile.setLocation('se.%s' % (tmpSite))
            testFile.create()
            fileset.addFile(testFile)

        fileset.commit()

        index = 0
        for f in fileset.files:
            index += 1
            testJob = Job(name='%s-%i' % (name, index))
            testJob.addFile(f)
            testJob["location"] = f.getLocations()[0]
            testJob['custom']['location'] = f.getLocations()[0]
            testJob['task'] = task.getPathName()
            testJob['sandbox'] = task.data.input.sandbox
            testJob['spec'] = os.path.join(self.testDir, 'basicWorkload.pcl')
            testJob['mask']['FirstEvent'] = 101
            testJob['owner'] = 'tapas'
            testJob["siteBlacklist"] = bl
            testJob["siteWhitelist"] = wl
            testJob['ownerDN'] = 'tapas'
            testJob['ownerRole'] = 'cmsrole'
            testJob['ownerGroup'] = 'phgroup'

            jobCache = os.path.join(cacheDir, 'Sub_%i' % (sub), 'Job_%i' % (index))
            os.makedirs(jobCache)
            testJob.create(jobGroup)
            testJob['cache_dir'] = jobCache
            testJob.save()
            jobGroup.add(testJob)
            with open(os.path.join(jobCache, 'job.pkl'), 'wb') as output:
                pickle.dump(testJob, output)

        return testJob, testFile

    def createDummyJobs(self, nJobs, location=None):
        """
        _createDummyJobs_

        Create some dummy jobs
        """

        if not location:
            location = self.sites[0]

        nameStr = makeUUID()

        testWorkflow = Workflow(spec=nameStr, owner="tapas",
                                name=nameStr, task="basicWorkload/Production",
                                owner_vogroup='phgroup', owner_vorole='cmsrole')
        testWorkflow.create()

        testFileset = Fileset(name=nameStr)
        testFileset.create()

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=testWorkflow,
                                        type="Processing",
                                        split_algo="FileBased")
        testSubscription.create()

        testJobGroup = JobGroup(subscription=testSubscription)
        testJobGroup.create()

        jobList = []

        for i in range(nJobs):
            testJob = Job(name='%s-%i' % (nameStr, i))
            testJob['location'] = location
            testJob['custom']['location'] = location
            testJob['userdn'] = 'tapas'
            testJob['owner'] = 'tapas'
            testJob['userrole'] = 'cmsrole'
            testJob['usergroup'] = 'phgroup'

            testJob.create(testJobGroup)
            jobList.append(testJob)

        return jobList

    @attr('integration')
    def testA_APITest(self):
        """
        _APITest_

        This is a commissioning test that has very little to do
        with anything except loading the code.
        """
        myThread = threading.currentThread()

        config = self.getConfig()

        baAPI = BossAirAPI(config=config, insertStates=True)

        # We should have loaded a plugin
        self.assertTrue('TestPlugin' in baAPI.plugins.keys())

        result = myThread.dbi.processData("SELECT name FROM bl_status")[0].fetchall()
        statusList = []
        for i in result:
            statusList.append(i.values()[0])

        # We should have the plugin states in the database
        self.assertEqual(statusList.sort(), ['New', 'Dead', 'Gone'].sort())

        # Create some jobs
        nJobs = 10

        jobDummies = self.createDummyJobs(nJobs=nJobs)
        print(jobDummies)

        baAPI.createNewJobs(wmbsJobs=jobDummies)

        runningJobs = baAPI._listRunJobs()

        self.assertEqual(len(runningJobs), nJobs)

        newJobs = baAPI._loadByStatus(status='New')
        self.assertEqual(len(newJobs), nJobs)
        deadJobs = baAPI._loadByStatus(status='Dead')
        self.assertEqual(len(deadJobs), 0)
        raisesException = False

        self.assertRaises(BossAirException,
                          baAPI._loadByStatus, status='FalseStatus')

        # Change the job status and update it
        for job in newJobs:
            job['status'] = 'Dead'

        baAPI._updateJobs(jobs=newJobs)

        # Test whether we see the job status as updated
        newJobs = baAPI._loadByStatus(status='New')
        self.assertEqual(len(newJobs), 0)
        deadJobs = baAPI._loadByStatus(status='Dead')
        self.assertEqual(len(deadJobs), nJobs)

        # Can we load by BossAir ID?
        loadedJobs = baAPI._loadByID(jobs=deadJobs)
        self.assertEqual(len(loadedJobs), nJobs)

        # Can we load via WMBS?
        loadedJobs = baAPI.loadByWMBS(wmbsJobs=jobDummies)
        self.assertEqual(len(loadedJobs), nJobs)

        # See if we can delete jobs
        baAPI._deleteJobs(jobs=deadJobs)

        # Confirm that they're gone
        deadJobs = baAPI._loadByStatus(status='Dead')
        self.assertEqual(len(deadJobs), 0)

        self.assertEqual(len(baAPI.jobs), 0)

        return

    @attr('integration')
    def testB_PluginTest(self):
        """
        _PluginTest_


        Now check that these functions worked if called through plugins
        Instead of directly.

        There are only three plugin
        """
        myThread = threading.currentThread()

        config = self.getConfig()

        baAPI = BossAirAPI(config=config, insertStates=True)

        # Create some jobs
        nJobs = 10

        jobDummies = self.createDummyJobs(nJobs=nJobs, location='T3_US_Xanadu')
        changeState = ChangeState(config)
        changeState.propagate(jobDummies, 'created', 'new')
        changeState.propagate(jobDummies, 'executing', 'created')

        # Prior to building the job, each job must have a plugin
        # and user assigned
        for job in jobDummies:
            job['plugin'] = 'TestPlugin'
            job['owner'] = 'tapas'

        baAPI.submit(jobs=jobDummies)

        newJobs = baAPI._loadByStatus(status='New')
        self.assertEqual(len(newJobs), nJobs)

        # Should be no more running jobs
        runningJobs = baAPI._listRunJobs()
        self.assertEqual(len(runningJobs), nJobs)

        # Test Plugin should complete all jobs
        baAPI.track()

        # Should be no more running jobs
        runningJobs = baAPI._listRunJobs()
        self.assertEqual(len(runningJobs), 0)

        # Check if they're complete
        completeJobs = baAPI.getComplete()
        self.assertEqual(len(completeJobs), nJobs)

        # Do this test because BossAir is specifically built
        # to keep it from finding completed jobs
        result = myThread.dbi.processData("SELECT id FROM bl_runjob")[0].fetchall()
        self.assertEqual(len(result), nJobs)

        jobsToRemove = baAPI._buildRunningJobs(wmbsJobs=jobDummies)
        baAPI._deleteJobs(jobs=jobsToRemove)

        result = myThread.dbi.processData("SELECT id FROM bl_runjob")[0].fetchall()
        self.assertEqual(len(result), 0)

        return

    def testG_monitoringDAO(self):
        """
        _monitoringDAO_

        Because I need a test for the monitoring DAO
        """
        myThread = threading.currentThread()

        config = self.getConfig()

        baAPI = BossAirAPI(config=config, insertStates=True)

        # Create some jobs
        nJobs = 10

        jobDummies = self.createDummyJobs(nJobs=nJobs)

        # Prior to building the job, each job must have a plugin
        # and user assigned
        for job in jobDummies:
            job['plugin'] = 'TestPlugin'
            job['owner'] = 'tapas'
            job['location'] = 'T2_US_UCSD'
            job.save()

        baAPI.submit(jobs=jobDummies)

        results = baAPI.monitor()
        self.assertEqual(results[0]['Pending'], nJobs)

        return


if __name__ == '__main__':
    unittest.main()
