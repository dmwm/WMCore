#!/usr/bin/python

"""
_ARCTest_t_

ARCPlugin unittests
"""

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

class ARCTest(BossAirTest):


    @attr('integration')
    def testH_ARCTest(self):
        """
        _ARCTest_

        This test works on the ARCPlugin, checking all of
        its functions with a single set of jobs
        """

        nRunning = getNArcJobs()
        self.assertEqual(nRunning, 0, "User currently has %i running jobs.  Test will not continue" % (nRunning))

        config = self.getConfig()
        config.BossAir.pluginNames.append("ARCPlugin")
        #config.BossAir.pluginNames = ["ARCPlugin"]
        baAPI  = BossAirAPI(config = config)

        nJobs = 2
        jobDummies = self.createDummyJobs(nJobs = nJobs, location = 'jade-cms.hip.fi')
        #baAPI.createNewJobs(wmbsJobs = jobDummies)
        #changeState = ChangeState(config)
        #changeState.propagate(jobDummies, 'created', 'new')
        #changeState.propagate(jobDummies, 'executing', 'created')

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
            job = j # {'id': j['id']}
            job['custom']      = {'location': 'jade-cms.hip.fi'}
            job['location'] = 'jade-cms.hip.fi'
            job['plugin'] = 'ARCPlugin'
            job['name']        = j['name']
            job['cache_dir']   = self.testDir
            job['retry_count'] = 0
            job['owner']       = 'edelmann'
            job['packageDir']  = self.testDir
            job['sandbox']     = sandbox
            job['priority']    = None
            jobList.append(job)

        baAPI.submit(jobs = jobList)

        nRunning = getNArcJobs()
        self.assertEqual(nRunning, nJobs)

        newJobs = baAPI._loadByStatus(status = 'New')
        self.assertEqual(len(newJobs), nJobs)

        baAPI.track()

        newJobs = baAPI._loadByStatus(status = 'New')
        self.assertEqual(len(newJobs), 0)

        rJobs = baAPI._listRunJobs()
        nOldJobs = 0
        for j in rJobs:
            if j['status'] != "New":
                nOldJobs += 1
        self.assertEqual(nOldJobs, nJobs)

            #if baAPI.plugins['ARCPlugin'].stateDict[j['status']] in [ "Pending", "Running" ]:

        baAPI.kill(jobs = jobList)

        nRunning = getNArcJobs()
        self.assertEqual(nRunning, 0)

        # Try resubmission
        for j in jobList:
            j['retry_count'] = 1

        succ, fail = baAPI.submit(jobs = jobList)

        time.sleep(30)

        nRunning = getNArcJobs()
        self.assertEqual(nRunning, nJobs)

        newJobs = baAPI._loadByStatus(status = 'New')
        self.assertEqual(len(newJobs), nJobs)

        # See where they are
        baAPI.track()

        newJobs = baAPI._loadByStatus(status = 'New')
        self.assertEqual(len(newJobs), 0)

        rJobs = baAPI._listRunJobs()
        nOldJobs = 0
        idStr = ""
        for j in rJobs:
            idStr += " " + j['gridid']
            if j['status'] != "New":
                nOldJobs += 1
        self.assertEqual(nOldJobs, nJobs)

        # Now kill 'em manually
        no_jobs = True
        while no_jobs:
            command = 'LD_LIBRARY_PATH=$CLEAN_LD_LIBRARY_PATH ngkill -t 180 -a'
            pipe = Popen(command, stdout = PIPE, stderr = STDOUT, shell = True)
            output = pipe.communicate()[0]
            if output.find("Job information not found") >= 0:
                # It seems the jobs hasn't reached the ARC info.sys yet.
                # Sleep a while and try again
                time.sleep(20)
                continue
            else:
                no_jobs = False

            # Just to be sure, if the jobs were already finished, do a
            # 'ngclean' too.
            command = 'LD_LIBRARY_PATH=$CLEAN_LD_LIBRARY_PATH ngclean -t 180 -a'
            pipe = Popen(command, stdout = PIPE, stderr = STDOUT, shell = True)
            output = pipe.communicate()[0]

        # Make sure the killing of the jobs reaches the info.sys.
        still_jobs = True
        while still_jobs:
            command = 'LD_LIBRARY_PATH=$CLEAN_LD_LIBRARY_PATH ngstat -t 180 ' + idStr
            pipe = Popen(command, stdout = PIPE, stderr = STDOUT, shell = True)
            output = pipe.communicate()[0]
            if output.find("Job information not found") < 0:
                # It seems the killing of the jobs hasn't reached the ARC info.sys yet.
                # Sleep a while and try again
                time.sleep(20)
                continue
            else:
                still_jobs = False

        # See what happened
        baAPI.track()

        idJobs = baAPI._loadByID(rJobs)
        nActiveJobs = 0
        nRemovedJobs = 0
        for j in idJobs:
            if j['status'] not in [ "New", "KILLING", "KILLED", "LOST" ]:
                nActiveJobs += 1
            if j['status'] in [ "KILLING", "KILLED", "LOST" ]:
                nRemovedJobs += 1
        self.assertEqual(nActiveJobs, 0)
        self.assertEqual(nRemovedJobs, nJobs)

        return



if __name__ == '__main__':
    unittest.main()
