#!/usr/bin/env python

"""
BossAir preliminary test
"""


import os
import time
import shutil
import os.path
import logging
import threading
import unittest
import getpass
import subprocess
import traceback
import cPickle as pickle


import WMCore.WMInit
from WMQuality.TestInit             import TestInit
from WMCore.DAOFactory              import DAOFactory
from WMCore.Services.UUID           import makeUUID
from WMCore.Agent.Configuration     import Configuration
from WMCore.WMSpec.Makers.TaskMaker import TaskMaker
from WMCore.WMSpec.StdSpecs.ReReco  import rerecoWorkload, getTestArguments



from WMCore.JobStateMachine.ChangeState     import ChangeState
from WMCore.ResourceControl.ResourceControl import ResourceControl

from WMCore.WMBS.File         import File
from WMCore.WMBS.Fileset      import Fileset
from WMCore.WMBS.Workflow     import Workflow
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.JobGroup     import JobGroup
from WMCore.WMBS.Job          import Job


from WMCore.WMBS.Job import Job

from WMCore.BossAir.BossAirAPI   import BossAirAPI, BossAirException
from WMCore.BossAir.StatusPoller import StatusPoller

from WMCore.Agent.HeartbeatAPI              import HeartbeatAPI


from WMCore_t.WMSpec_t.TestSpec import testWorkload



from WMComponent.JobSubmitter.JobSubmitterPoller import JobSubmitterPoller
from WMComponent.JobTracker.JobTrackerPoller import JobTrackerPoller


from nose.plugins.attrib import attr




def getCondorRunningJobs(user):
    """
    _getCondorRunningJobs_

    Return the number of jobs currently running for a user
    """


    command = ['condor_q', user]
    pipe = subprocess.Popen(command, stdout = subprocess.PIPE,
                            stderr = subprocess.PIPE, shell = False)
    stdout, error = pipe.communicate()

    output = stdout.split('\n')[-2]

    nJobs = int(output.split(';')[0].split()[0])

    return nJobs





class BossAirTest(unittest.TestCase):
    """
    Tests for the BossAir prototype    

    """

    sites = ['T2_US_Florida', 'T2_US_UCSD', 'T2_TW_Taiwan', 'T1_CH_CERN', 'malpaquet']

    

    def setUp(self):
        """
        setup for test.
        """

        myThread = threading.currentThread()
        
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        #self.tearDown()
        self.testInit.setSchema(customModules = ["WMCore.WMBS", "WMCore.BossAir", "WMCore.ResourceControl", "WMCore.Agent.Database", "WMCore.MsgService"],
                                useDefault = False)

        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        self.getJobs = self.daoFactory(classname = "Jobs.GetAllJobs")


        locationAction = self.daoFactory(classname = "Locations.New")
        locationSlots  = self.daoFactory(classname = "Locations.SetJobSlots")



        #Create sites in resourceControl
        resourceControl = ResourceControl()
        for site in self.sites:
            resourceControl.insertSite(siteName = site, seName = 'se.%s' % (site),
                                       ceName = site, plugin = "CondorPlugin")
            resourceControl.insertThreshold(siteName = site, taskType = 'Processing', \
                                            maxSlots = 10000)
        resourceControl.insertSite(siteName = 'Xanadu', seName = 'se.Xanadu',
                                   ceName = 'Xanadu', plugin = "TestPlugin")
        resourceControl.insertThreshold(siteName = 'Xanadu', taskType = 'Processing', \
                                        maxSlots = 10000)


        # We actually need the user name
        self.user = getpass.getuser()

        self.testDir = self.testInit.generateWorkDir()


        # Set heartbeat
        componentName = 'test'
        self.heartbeatAPI  = HeartbeatAPI(componentName)
        self.heartbeatAPI.registerComponent()
        componentName = 'JobTracker'
        self.heartbeatAPI2  = HeartbeatAPI(componentName)
        self.heartbeatAPI2.registerComponent()

        return

    def tearDown(self):
        """
        Database deletion
        """
        self.testInit.clearDatabase(modules = ["WMCore.WMBS", "WMCore.BossAir", "WMCore.ResourceControl", "WMCore.Agent.Database", "WMCore.MsgService"])

        self.testInit.delWorkDir()

        return



    def getConfig(self):
        """
        _getConfig_

        Build a basic BossAir config
        """
                       
        config = Configuration()

        config.section_("Agent")
        config.Agent.agentName  = 'testAgent'
        config.Agent.componentName = 'test'
        config.Agent.useHeartbeat = False

        config.section_("CoreDatabase")
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.socket     = os.getenv("DBSOCK")


        config.section_("BossAir")
        config.BossAir.pluginNames = ['TestPlugin', 'CondorPlugin']
        config.BossAir.pluginDir   = 'WMCore.BossAir.Plugins'

        config.component_("JobSubmitter")
        config.JobSubmitter.logLevel      = 'INFO'
        #config.JobSubmitter.maxThreads    = 1
        config.JobSubmitter.pollInterval  = 1
        config.JobSubmitter.pluginName    = 'AirPlugin'
        config.JobSubmitter.pluginDir     = 'JobSubmitter.Plugins'
        config.JobSubmitter.submitDir     = os.path.join(self.testDir, 'submit')
        config.JobSubmitter.submitNode    = os.getenv("HOSTNAME", 'badtest.fnal.gov')
        #config.JobSubmitter.submitScript  = os.path.join(os.getcwd(), 'submit.sh')
        config.JobSubmitter.submitScript  = os.path.join(WMCore.WMInit.getWMBASE(),
                                                         'test/python/WMComponent_t/JobSubmitter_t',
                                                         'submit.sh')
        config.JobSubmitter.componentDir  = os.path.join(os.getcwd(), 'Components')
        config.JobSubmitter.workerThreads = 2
        config.JobSubmitter.jobsPerWorker = 200
        config.JobSubmitter.gLiteConf     = os.path.join(os.getcwd(), 'config.cfg')



        # JobTracker
        config.component_("JobTracker")
        config.JobTracker.logLevel      = 'INFO'
        config.JobTracker.pollInterval  = 1
        #config.JobTracker.trackerName   = 'AirTracker'
        #config.JobTracker.pluginDir     = 'WMComponent.JobTracker.Plugins'
        #config.JobTracker.componentDir  = os.path.join(os.getcwd(), 'Components')
        #config.JobTracker.runTimeLimit  = 7776000 #Jobs expire after 90 days
        #config.JobTracker.idleTimeLimit = 7776000
        #config.JobTracker.heldTimeLimit = 7776000
        #config.JobTracker.unknTimeLimit = 7776000


        # JobStateMachine
        config.component_('JobStateMachine')
        config.JobStateMachine.couchurl        = os.getenv('COUCHURL')
        config.JobStateMachine.default_retries = 1
        config.JobStateMachine.couchDBName     = "mnorman_test"


        # JobStatus
        config.component_('JobStatus')
        config.JobStatus.stateTimeouts  = {'Pending': 2, 'Running': 86400}
        config.JobStatus.pollInterval   = 1

        # JobStatusLite (LEGACY)
        config.component_('JobStatusLite')
        config.JobStatusLite.componentDir = os.path.join(os.getcwd(), 'Components')



        return config


    def createTestWorkload(self, workloadName = 'Test', emulator = True):
        """
        _createTestWorkload_

        Creates a test workload for us to run on, hold the basic necessities.
        """


        workload = testWorkload("Tier1ReReco")
        rereco = workload.getTask("ReReco")

        
        taskMaker = TaskMaker(workload, os.path.join(self.testDir, 'workloadTest'))
        taskMaker.skipSubscription = True
        taskMaker.processWorkload()

        workload.save(workloadName)

        return workload



    def createJobGroups(self, nSubs, nJobs, task, workloadSpec, site = None, bl = [], wl = []):
        """
        Creates a series of jobGroups for submissions

        """

        jobGroupList = []

        testWorkflow = Workflow(spec = workloadSpec, owner = "mnorman",
                                name = makeUUID(), task="basicWorkload/Production")
        testWorkflow.create()

        # Create subscriptions
        for i in range(nSubs):

            name = makeUUID()

            # Create Fileset, Subscription, jobGroup
            testFileset = Fileset(name = name)
            testFileset.create()
            testSubscription = Subscription(fileset = testFileset,
                                            workflow = testWorkflow,
                                            type = "Processing",
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
                           site = site, bl = bl, wl = wl)
                                         


            testFileset.commit()
            testJobGroup.commit()
            jobGroupList.append(testJobGroup)

        return jobGroupList
            


    def makeNJobs(self, name, task, nJobs, jobGroup, fileset, sub, site = None, bl = [], wl = []):
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
            testFile = File(lfn = "/singleLfn/%s/%s" %(name, n),
                            size = 1024, events = 10)
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
            testJob = Job(name = '%s-%i' %(name, index))
            testJob.addFile(f)
            testJob["location"]  = f.getLocations()[0]
            testJob['task']    = task.getPathName()
            testJob['sandbox'] = task.data.input.sandbox
            testJob['spec']    = os.path.join(self.testDir, 'basicWorkload.pcl')
            testJob['mask']['FirstEvent'] = 101
            testJob['owner']   = 'mnorman'
            testJob["siteBlacklist"] = bl
            testJob["siteWhitelist"] = wl
            jobCache = os.path.join(cacheDir, 'Sub_%i' % (sub), 'Job_%i' % (index))
            os.makedirs(jobCache)
            testJob.create(jobGroup)
            testJob['cache_dir'] = jobCache
            testJob.save()
            jobGroup.add(testJob)
            output = open(os.path.join(jobCache, 'job.pkl'),'w')
            pickle.dump(testJob, output)
            output.close()

        return testJob, testFile



    def createDummyJobs(self, nJobs, location = None):
        """
        _createDummyJobs_
        
        Create some dummy jobs
        """

        if not location:
            location = self.sites[0]

        nameStr = makeUUID()

        testWorkflow = Workflow(spec = nameStr, owner = "mnorman",
                                name = nameStr, task="basicWorkload/Production")
        testWorkflow.create()

        testFileset = Fileset(name = nameStr)
        testFileset.create()

        testSubscription = Subscription(fileset = testFileset,
                                            workflow = testWorkflow,
                                            type = "Processing",
                                            split_algo = "FileBased")
        testSubscription.create()

        testJobGroup = JobGroup(subscription = testSubscription)
        testJobGroup.create()

        jobList = []

        for i in range(nJobs):
            testJob = Job(name = '%s-%i' % (nameStr, i))
            testJob['location'] = location
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
        #return
        
        myThread = threading.currentThread()

        config = self.getConfig()

        baAPI  = BossAirAPI(config = config)

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

        jobDummies = self.createDummyJobs(nJobs = nJobs)

        baAPI.createNewJobs(wmbsJobs = jobDummies)

        runningJobs = baAPI._listRunning()

        self.assertEqual(len(runningJobs), nJobs)


        newJobs = baAPI._loadByStatus(status = 'New')
        self.assertEqual(len(newJobs), nJobs)
        deadJobs = baAPI._loadByStatus(status = 'Dead')
        self.assertEqual(len(deadJobs), 0)
        raisesException = False
        
        try:
            baAPI._loadByStatus(status = 'FalseStatus')
        except BossAirException:
            # It should raise an error if we try loading a
            # non-existant status
            raisesException = True
        self.assertTrue(raisesException)


        # Change the job status and update it
        for job in newJobs:
            job['status'] = 'Dead'

        baAPI._updateJobs(jobs = newJobs)


        # Test whether we see the job status as updated
        newJobs = baAPI._loadByStatus(status = 'New')
        self.assertEqual(len(newJobs), 0)
        deadJobs = baAPI._loadByStatus(status = 'Dead')
        self.assertEqual(len(deadJobs), nJobs)

        # Can we load by BossAir ID?
        loadedJobs = baAPI._loadByID(jobs = deadJobs)
        self.assertEqual(len(loadedJobs), nJobs)

        # Can we load via WMBS?
        loadedJobs = baAPI.loadByWMBS(wmbsJobs = jobDummies)
        self.assertEqual(len(loadedJobs), nJobs)


        # See if we can delete jobs
        baAPI._deleteJobs(jobs = deadJobs)
        
        # Confirm that they're gone
        deadJobs = baAPI._loadByStatus(status = 'Dead')
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

        baAPI  = BossAirAPI(config = config)


        # Create some jobs
        nJobs = 10

        jobDummies = self.createDummyJobs(nJobs = nJobs, location = 'Xanadu')

        # Prior to building the job, each job must have a plugin
        # and user assigned
        for job in jobDummies:
            job['plugin']   = 'TestPlugin'
            job['owner']    = 'mnorman'

        baAPI.submit(jobs = jobDummies)


        newJobs = baAPI._loadByStatus(status = 'New')
        self.assertEqual(len(newJobs), nJobs)

        # Should be no more running jobs
        runningJobs = baAPI._listRunning()
        self.assertEqual(len(runningJobs), nJobs)


        # Test Plugin should complete all jobs
        baAPI.track()

        # Should be no more running jobs
        runningJobs = baAPI._listRunning()
        self.assertEqual(len(runningJobs), 0)


        # Check if they're complete
        completeJobs = baAPI.getComplete()
        self.assertEqual(len(completeJobs), nJobs)


        # Do this test because BossAir is specifically built
        # to keep it from finding completed jobs
        result = myThread.dbi.processData("SELECT id FROM bl_runjob")[0].fetchall()
        self.assertEqual(len(result), nJobs)


        baAPI.removeComplete(jobs = jobDummies)


        result = myThread.dbi.processData("SELECT id FROM bl_runjob")[0].fetchall()
        self.assertEqual(len(result), 0)
        

        return


    @attr('integration')
    def testC_CondorTest(self):
        """
        _CondorTest_

        This test works on the CondorPlugin, checking all of
        its functions with a single set of jobs
        """

        #return

        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, 0, "User currently has %i running jobs.  Test will not continue" % (nRunning))

        config = self.getConfig()

        nJobs = 10

        jobDummies = self.createDummyJobs(nJobs = nJobs)

        baAPI  = BossAirAPI(config = config)


        jobPackage = os.path.join(self.testDir, 'JobPackage.pkl')
        f = open(jobPackage, 'w')
        f.write(' ')
        f.close()

        sandbox = os.path.join(self.testDir, 'sandbox.box')
        f = open(sandbox, 'w')
        f.write(' ')
        f.close()

        jobList = []
        for j in jobDummies:
            tmpJob = {'id': j['id']}
            tmpJob['custom']      = {'location': 'malpaquet'}
            tmpJob['name']        = j['name']
            tmpJob['cache_dir']   = self.testDir
            tmpJob['retry_count'] = 0
            tmpJob['plugin']      = 'CondorPlugin'
            tmpJob['owner']       = 'mnorman'
            tmpJob['packageDir']  = self.testDir
            tmpJob['sandbox']     = sandbox
            jobList.append(tmpJob)


        info = {}
        #info['packageDir'] = self.testDir
        info['index']      = 0
        info['sandbox']    = sandbox



        baAPI.submit(jobs = jobList, info = info)

        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, nJobs)

        newJobs = baAPI._loadByStatus(status = 'New')
        self.assertEqual(len(newJobs), nJobs)


        baAPI.track()

        newJobs = baAPI._loadByStatus(status = 'New')
        self.assertEqual(len(newJobs), 0)

        newJobs = baAPI._loadByStatus(status = 'Idle')
        self.assertEqual(len(newJobs), nJobs)


        baAPI.kill(jobs = jobList)

        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, 0)


        return



    @attr('integration')
    def testD_PrototypeChain(self):
        """
        _PrototypeChain_

        Prototype the BossAir workflow
        """
        
        #return

        myThread = threading.currentThread()

        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, 0, "User currently has %i running jobs.  Test will not continue" % (nRunning))


        config = self.getConfig()
        config.BossAir.pluginName = 'CondorPlugin'

        baAPI  = BossAirAPI(config = config)

        workload = self.createTestWorkload()

        workloadName = "basicWorkload"

        changeState = ChangeState(config)

        nSubs = 1
        nJobs = 2
        cacheDir = os.path.join(self.testDir, 'CacheDir')

        jobGroupList = self.createJobGroups(nSubs = nSubs, nJobs = nJobs,
                                            task = workload.getTask("ReReco"),
                                            workloadSpec = os.path.join(self.testDir,
                                                                        'workloadTest',
                                                                        workloadName),
                                            site = 'se.T2_US_UCSD')
        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')


        jobSubmitter = JobSubmitterPoller(config = config)
        jobTracker   = JobTrackerPoller(config = config)

        jobSubmitter.algorithm()

        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, nSubs * nJobs)

        newJobs = baAPI._loadByStatus(status = 'New')
        self.assertEqual(len(newJobs), nSubs * nJobs)

        # Check WMBS
        getJobsAction = self.daoFactory(classname = "Jobs.GetAllJobs")
        result = getJobsAction.execute(state = 'Executing', jobType = "Processing")
        self.assertEqual(len(result), nSubs * nJobs)
        

        statusPoller = StatusPoller(config = config)

        statusPoller.algorithm()


        newJobs = baAPI._loadByStatus(status = 'New')
        self.assertEqual(len(newJobs), 0)

        newJobs = baAPI._loadByStatus(status = 'Idle')
        self.assertEqual(len(newJobs), nSubs * nJobs)
        

        # Tracker should do nothing
        jobTracker.algorithm()

        result = getJobsAction.execute(state = 'Executing', jobType = "Processing")
        self.assertEqual(len(result), nSubs * nJobs)


        # Wait for jobs to timeout due to short Pending wait period
        time.sleep(5)


        statusPoller.algorithm()

        newJobs = baAPI._loadByStatus(status = 'Idle')
        self.assertEqual(len(newJobs), 0)

        newJobs = baAPI._loadByStatus(status = 'Timeout', complete = '0')
        self.assertEqual(len(newJobs), nSubs * nJobs)

        # Jobs should be gone
        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, 0)


        # Check if they're complete
        completeJobs = baAPI.getComplete()
        self.assertEqual(len(completeJobs), nSubs * nJobs)


        # Because they timed out, they all should have failed
        jobTracker.algorithm()

        result = getJobsAction.execute(state = 'Executing', jobType = "Processing")
        self.assertEqual(len(result), 0)

        result = getJobsAction.execute(state = 'JobFailed', jobType = "Processing")
        self.assertEqual(len(result), nSubs * nJobs)

        

        return


    @attr('integration')
    def testE_FullChain(self):
        """
        _FullChain_

        Full test going through the chain; using polling cycles and everything
        """

        #return

        from WMComponent.JobSubmitter.JobSubmitter   import JobSubmitter
        from WMComponent.JobStatusLite.JobStatusLite import JobStatusLite
        from WMComponent.JobTracker.JobTracker       import JobTracker


        myThread = threading.currentThread()

        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, 0, "User currently has %i running jobs.  Test will not continue" % (nRunning))


        config = self.getConfig()
        config.BossAir.pluginName = 'CondorPlugin'

        baAPI  = BossAirAPI(config = config)

        workload = self.createTestWorkload()

        workloadName = "basicWorkload"

        changeState = ChangeState(config)

        nSubs = 1
        nJobs = 2
        cacheDir = os.path.join(self.testDir, 'CacheDir')

        jobGroupList = self.createJobGroups(nSubs = nSubs, nJobs = nJobs,
                                            task = workload.getTask("ReReco"),
                                            workloadSpec = os.path.join(self.testDir,
                                                                        'workloadTest',
                                                                        workloadName),
                                            site = 'se.T2_US_UCSD')
        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        jobSubmitter = JobSubmitter(config = config)
        jobTracker   = JobTracker(config = config)
        jobStatus    = JobStatusLite(config = config)


        jobSubmitter.prepareToStart()
        jobTracker.prepareToStart()
        jobStatus.prepareToStart()

        # What should happen here:
        # 1) The JobSubmitter should submit the jobs
        # 2) Because of the ridiculously short time on pending jobs
        #     the JobStatus poller should mark the jobs as done
        #     and kill them.
        # 3) The JobTracker should realize there are finished jobs
        #
        # So at the end of several polling cycles, the jobs should all
        # be done, but be in the failed status (they timed out)

        time.sleep(10)


        myThread.workerThreadManager.terminateWorkers()


        getJobsAction = self.daoFactory(classname = "Jobs.GetAllJobs")
        result = getJobsAction.execute(state = 'Executing', jobType = "Processing")
        self.assertEqual(len(result), 0)


        result = getJobsAction.execute(state = 'JobFailed', jobType = "Processing")
        self.assertEqual(len(result), nJobs * nSubs)

        


        return




    def testF_gLitePluginParallelization(self):
        """
        _gLitePluginParallelization_
        
        A simple test to see if I can successfully run commands
        in parallel using multiprocessing in the gLite plugin
        """

        config = self.getConfig()
        config.BossAir.pluginNames.append('gLitePlugin')

        baAPI  = BossAirAPI(config = config)
        
        jobs = []

        for x in range(10):
            jobs.append({'gridid': 'job_%i' % (x)})

        results = baAPI.plugins['gLitePlugin'].complete(jobs = jobs)
        for result in results:
            adjResult = result.rstrip('\n')
            self.assertTrue(adjResult in [x['gridid'] for x in jobs])

        
        return
        


    def testG_monitoringDAO(self):
        """
        _monitoringDAO_

        Because I need a test for the monitoring DAO
        """

        #return

        myThread = threading.currentThread()

        config = self.getConfig()

        baAPI  = BossAirAPI(config = config)


        # Create some jobs
        nJobs = 10

        jobDummies = self.createDummyJobs(nJobs = nJobs)

        # Prior to building the job, each job must have a plugin
        # and user assigned
        for job in jobDummies:
            job['plugin']   = 'TestPlugin'
            job['owner']    = 'mnorman'
            job['location'] = 'T2_US_UCSD'
            job.save()

        baAPI.submit(jobs = jobDummies)


        results = baAPI.monitor()

        self.assertEqual(len(results), nJobs)
        for job in results:
            self.assertEqual(job['plugin'], 'CondorPlugin')


        return










        



if __name__ == '__main__':
    unittest.main()



                                    
