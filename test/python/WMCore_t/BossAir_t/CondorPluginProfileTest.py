#!/usr/bin/python

"""
_CondorPluginProfile_

CondorPluginProfile unittests
"""
import cProfile
import os.path
import pstats
import unittest

from WMCore_t.BossAir_t.BossAir_t import BossAirTest, getCondorRunningJobs
from nose.plugins.attrib import attr

from WMComponent.JobSubmitter.JobSubmitterPoller import JobSubmitterPoller
from WMCore.BossAir.BossAirAPI import BossAirAPI
from WMCore.JobStateMachine.ChangeState import ChangeState


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

        baAPI = BossAirAPI(config=config, insertStates=True)

        workload = self.createTestWorkload()

        workloadName = "basicWorkload"

        changeState = ChangeState(config)

        nSubs = 50
        nJobs = 100

        dummyCacheDir = os.path.join(self.testDir, 'CacheDir')

        jobGroupList = self.createJobGroups(nSubs=nSubs, nJobs=nJobs,
                                            task=workload.getTask("ReReco"),
                                            workloadSpec=os.path.join(self.testDir,
                                                                      'workloadTest',
                                                                      workloadName),
                                            site=None)
        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')

        jobSubmitter = JobSubmitterPoller(config=config)

        jobSubmitter.algorithm()

        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, nSubs * nJobs)

        baAPI.track()
        idleJobs = baAPI._loadByStatus(status='Idle')

        baAPI.kill(jobs=idleJobs)

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

        baAPI = BossAirAPI(config=config, insertStates=True)
        workload = self.createTestWorkload()
        workloadName = "basicWorkload"
        changeState = ChangeState(config)

        nSubs = 1
        nJobs = 2
        dummycacheDir = os.path.join(self.testDir, 'CacheDir')
        jobGroupList = self.createJobGroups(nSubs=nSubs, nJobs=nJobs,
                                            task=workload.getTask("ReReco"),
                                            workloadSpec=os.path.join(self.testDir,
                                                                      'workloadTest',
                                                                      workloadName),
                                            site="se.T2_US_UCSD")
        for group in jobGroupList:
            changeState.propagate(group.jobs, 'created', 'new')
        jobSubmitter = JobSubmitterPoller(config=config)
        jobSubmitter.algorithm()
        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, nSubs * nJobs)

        baAPI.track()
        idleJobs = baAPI._loadByStatus(status='Idle')

        ##
        # Make one of the sites in the sitelist to be True for ABORTED/DRAINING/DOWN
        # updateSiteInformation() method should edit the classAd for all the jobs
        # that are bound for the site
        # Check the Q manually using condor_q -l <job id>
        jtok = baAPI.updateSiteInformation(idleJobs, "T2_US_UCSD", True)
        if jtok != None:
            baAPI.kill(jtok, errorCode=71301)  # errorCode can be either 71301/71302/71303 (Aborted/Draining/Down)

        return

    def createProfile(self, name, function):
        fileName = name
        prof = cProfile.Profile()
        prof.runcall(function)
        prof.dump_stats(fileName)
        p = pstats.Stats(fileName)
        p.strip_dirs().sort_stats('cumulative').print_stats(0.1)
        p.strip_dirs().sort_stats('time').print_stats(0.1)
        p.strip_dirs().sort_stats('calls').print_stats(0.1)
        # p.strip_dirs().sort_stats('name').print_stats(10)

    def ProfileWMSMode(self):
        self.createProfile('PyCondorProfile.prof', self.testF_WMSMode)


if __name__ == '__main__':
    unittest.main()
