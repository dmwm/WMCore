#!/usr/bin/python

"""
_CondorPlugin_t_

CondorPlugin unittests
"""
import time
import os.path
import threading
import unittest

from nose.plugins.attrib import attr
from subprocess import Popen, PIPE, STDOUT

from WMCore.BossAir.BossAirAPI   import BossAirAPI, BossAirException
from WMCore.BossAir.StatusPoller import StatusPoller
from WMCore.JobStateMachine.ChangeState          import ChangeState
from WMComponent.JobSubmitter.JobSubmitterPoller import JobSubmitterPoller
from WMComponent.JobTracker.JobTrackerPoller     import JobTrackerPoller

from WMCore_t.BossAir_t.BossAir_t import BossAirTest, getNArcJobs, getCondorRunningJobs

class CondorPluginTest(BossAirTest):
    """
    _CondorPluginTest_

    Inherit everything from BossAir
    """


    @attr('integration')
    def testC_CondorTest(self):
        """
        _CondorTest_

        This test works on the CondorPlugin, checking all of
        its functions with a single set of jobs
        """
        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, 0, "User currently has %i running jobs.  Test will not continue" % (nRunning))

        # Get the config and set the removal time to -10 for testing
        config = self.getConfig()
        config.BossAir.removeTime = -10.0

        nJobs = 10

        jobDummies = self.createDummyJobs(nJobs = nJobs)

        baAPI  = BossAirAPI(config = config)

        print self.testDir

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
            tmpJob['owner']       = 'tapas'
            tmpJob['packageDir']  = self.testDir
            tmpJob['sandbox']     = sandbox
            tmpJob['priority']    = None
            tmpJob['usergroup']   = "wheel"
            tmpJob['userrole']    = 'cmsuser'
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

        # Do a second time to make sure that the cache
        # doesn't die on us
        baAPI.track()

        newJobs = baAPI._loadByStatus(status = 'New')
        self.assertEqual(len(newJobs), 0)

        newJobs = baAPI._loadByStatus(status = 'Idle')
        self.assertEqual(len(newJobs), nJobs)

        baAPI.kill(jobs = jobList)

        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, 0)

        # Try resubmission
        for j in jobList:
            j['retry_count'] = 1

        baAPI.submit(jobs = jobList, info = info)

        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, nJobs)

        newJobs = baAPI._loadByStatus(status = 'New')
        self.assertEqual(len(newJobs), nJobs)


        # See where they are
        baAPI.track()

        newJobs = baAPI._loadByStatus(status = 'New')
        self.assertEqual(len(newJobs), 0)

        newJobs = baAPI._loadByStatus(status = 'Idle')
        self.assertEqual(len(newJobs), nJobs)

        # Now kill 'em manually
        command = ['condor_rm', self.user]
        pipe = Popen(command, stdout = PIPE, stderr = PIPE, shell = False)
        pipe.communicate()

        # See what happened
        baAPI.track()

        newJobs = baAPI._loadByStatus(status = 'Idle')
        self.assertEqual(len(newJobs), 0)

        #newJobs = baAPI._loadByStatus(status = 'Removed')
        #self.assertEqual(len(newJobs), nJobs)

        # Because removal time is -10.0, jobs should remove immediately
        baAPI.track()

        # Assert that jobs were listed as completed
        myThread = threading.currentThread()
        newJobs = baAPI._loadByStatus(status = 'Removed', complete = '0')
        self.assertEqual(len(newJobs), nJobs)

        return


    @attr('integration')
    def testD_PrototypeChain(self):
        """
        _PrototypeChain_

        Prototype the BossAir workflow
        """
        myThread = threading.currentThread()

        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, 0, "User currently has %i running jobs.  Test will not continue" % (nRunning))


        config = self.getConfig()
        config.BossAir.pluginName = 'CondorPlugin'

        baAPI  = BossAirAPI(config = config)

        workload = self.createTestWorkload()

        workloadName = "basicWorkload"

        changeState = ChangeState(config)

        nSubs = 5
        nJobs = 10

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
        statusPoller = StatusPoller(config = config)

        jobSubmitter.algorithm()

        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, nSubs * nJobs)

        newJobs = baAPI._loadByStatus(status = 'New')
        self.assertEqual(len(newJobs), nSubs * nJobs)

        # Check WMBS
        getJobsAction = self.daoFactory(classname = "Jobs.GetAllJobs")
        result = getJobsAction.execute(state = 'Executing', jobType = "Processing")
        self.assertEqual(len(result), nSubs * nJobs)

        statusPoller.algorithm()

        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, nSubs * nJobs)

        newJobs = baAPI._loadByStatus(status = 'New')
        self.assertEqual(len(newJobs), 0)

        newJobs = baAPI._loadByStatus(status = 'Idle')
        self.assertEqual(len(newJobs), nSubs * nJobs)


        # Tracker should do nothing
        jobTracker.algorithm()

        result = getJobsAction.execute(state = 'Executing', jobType = "Processing")
        self.assertEqual(len(result), nSubs * nJobs)


        # Wait for jobs to timeout due to short Pending wait period
        time.sleep(12)


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

        return

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

        time.sleep(20)


        myThread.workerThreadManager.terminateWorkers()


        getJobsAction = self.daoFactory(classname = "Jobs.GetAllJobs")
        result = getJobsAction.execute(state = 'Executing', jobType = "Processing")
        self.assertEqual(len(result), 0)

        result = getJobsAction.execute(state = 'JobFailed', jobType = "Processing")
        self.assertEqual(len(result), nJobs * nSubs)
        return

    @attr('integration')
    def testF_WMSMode(self):
        """
        _WMSMode_

        Try running things in WMS Mode.
        """

        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, 0, "User currently has %i running jobs.  Test will not continue" % (nRunning))

        config = self.getConfig()
        config.BossAir.pluginName = 'CondorPlugin'
        config.BossAir.submitWMSMode = True

        baAPI  = BossAirAPI(config = config)

        workload = self.createTestWorkload()

        workloadName = "basicWorkload"

        changeState = ChangeState(config)

        nSubs = 5
        nJobs = 10

        cacheDir = os.path.join(self.testDir, 'CacheDir')

        jobGroupList = self.createJobGroups(nSubs = nSubs, nJobs = nJobs,
                                            task = workload.getTask("ReReco"),
                                            workloadSpec = os.path.join(self.testDir,
                                                                        'workloadTest',
                                                                        workloadName),
                                            site = None)
        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')


        jobSubmitter = JobSubmitterPoller(config = config)

        jobSubmitter.algorithm()

        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, nSubs * nJobs)

        baAPI.track()
        idleJobs = baAPI._loadByStatus(status = 'Idle')
        sn = "T2_US_UCSD"

        # Test the Site Info has been updated. Make Sure T2_US_UCSD is not in the sitelist
        # in BossAir_t.py
        baAPI.updateSiteInformation(idleJobs, sn, True)

        # Now kill 'em manually
        #        command = ['condor_rm', self.user]
        #        pipe = Popen(command, stdout = PIPE, stderr = PIPE, shell = False)
        #        pipe.communicate()
        
        del jobSubmitter

        return


    @attr('integration')
    def testT_updateJobInfo(self):
        """
        _updateJobInfo_

        Test the updateSiteInformation method from CondorPlugin.py
        """

        nRunning = getCondorRunningJobs(self.user)
        
        config = self.getConfig()
        config.BossAir.pluginName = 'CondorPlugin'
        baAPI  = BossAirAPI(config = config)
        baAPI.track()
        idleJobs = baAPI._loadByStatus(status = 'Idle')
        print idleJobs
        for job in idleJobs :
            print job['id']
        baAPI.updateSiteInformation(idleJobs, info = None)
        
        return


if __name__ == '__main__':
    unittest.main()
