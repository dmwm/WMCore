#!/usr/bin/python

"""
_CondorPluginProfile_

CondorPluginProfile unittests
"""
import time
import os.path
import threading
import unittest
import tempfile
import cProfile
import pstats
import shutil


from nose.plugins.attrib import attr
from subprocess import Popen, PIPE, STDOUT

from WMCore.BossAir.BossAirAPI   import BossAirAPI, BossAirException
from WMCore.BossAir.StatusPoller import StatusPoller
from WMCore.JobStateMachine.ChangeState          import ChangeState
from WMComponent.JobSubmitter.JobSubmitterPoller import JobSubmitterPoller
from WMComponent.JobTracker.JobTrackerPoller     import JobTrackerPoller

from WMCore_t.BossAir_t.BossAir_t import BossAirTest, getNArcJobs, getCondorRunningJobs

class CondorPluginProfileTest(BossAirTest):
    """
    _CondorPluginProfileTest_

    Inherit everything from BossAir
    """

    @attr('integration')
    def testF_WMSMode(self):
        """
        _WMSMode_

        Try running things in WMS Mode.
        """

        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, 0, "User currently has %i running jobs.  Test will not continue" % (nRunning))

        config = self.getConfig()
        config.BossAir.pluginName = 'PyCondorPlugin'
        config.BossAir.submitWMSMode = True

        baAPI  = BossAirAPI(config = config)

        workload = self.createTestWorkload()

        workloadName = "basicWorkload"

        changeState = ChangeState(config)

        nSubs = 50
        nJobs = 100

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

        baAPI.kill(jobs = idleJobs)

        del jobSubmitter

        return


    @attr('integration')
    def testT_updateJobInfo(self):
        """
        _updateJobInfo_

        Test the updateSiteInformation method from PyCondorPlugin.py
        """

        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, 0, "User currently has %i running jobs.  Test will not continue" % (nRunning))
        
        config = self.getConfig()
        config.BossAir.pluginName = 'PyCondorPlugin'
        config.BossAir.submitWMSMode = True

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
                                            site="se.T2_US_UCSD")
        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')
        jobSubmitter = JobSubmitterPoller(config = config)
        jobSubmitter.algorithm()
        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, nSubs * nJobs)

        baAPI.track()
        idleJobs = baAPI._loadByStatus(status = 'Idle')

        ##
        # Make one of the sites in the sitelist to be True for ABORTED/DRAINING/DOWN 
        # updateSiteInformation() method should edit the classAd for all the jobs
        # that are bound for the site
        # Check the Q manually using condor_q -l <job id>
        jtok = baAPI.updateSiteInformation(idleJobs, "T2_US_UCSD", True)
        if jtok != None :
            baAPI.kill(jtok, errorCode=61301)  # errorCode can be either 61301/61302/61303 (Aborted/Draining/Down)

        return

    def createProfile(self, name, function):
        file=name
        prof=cProfile.Profile()
        prof.runcall(function)
        prof.dump_stats(file)
        p = pstats.Stats(file)
        p.strip_dirs().sort_stats('cumulative').print_stats(0.1)
        p.strip_dirs().sort_stats('time').print_stats(0.1)
        p.strip_dirs().sort_stats('calls').print_stats(0.1)
        #p.strip_dirs().sort_stats('name').print_stats(10)

    def ProfileWMSMode(self):
        self.createProfile('PyCondorProfile.prof', self.testF_WMSMode)


if __name__ == '__main__':
    unittest.main()
