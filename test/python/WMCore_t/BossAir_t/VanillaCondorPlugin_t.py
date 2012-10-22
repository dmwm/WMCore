#!/usr/bin/python

"""
_VanillaCondorPlugin_t_

VanillaCondorPlugin unittests
"""
import os
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
from WMCore.Algorithms                           import SubprocessAlgos

from WMCore_t.BossAir_t.BossAir_t import BossAirTest, getNArcJobs, getCondorRunningJobs

class VanillaCondorPluginTest(BossAirTest):
    """
    _VanillaCondorPluginTest_

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
        config.BossAir.pluginNames.append('VanillaCondorPlugin')

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
            tmpJob['plugin']      = 'VanillaCondorPlugin'
            tmpJob['owner']       = 'mnorman'
            tmpJob['packageDir']  = self.testDir
            tmpJob['sandbox']     = sandbox
            tmpJob['priority']    = None
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

        newJobs = baAPI._loadByStatus(status = 'Removed')
        self.assertEqual(len(newJobs), nJobs)

        # Because removal time is -10.0, jobs should remove immediately
        baAPI.track()

        # Assert that jobs were listed as completed
        myThread = threading.currentThread()
        newJobs = baAPI._loadByStatus(status = 'Removed', complete = '0')
        self.assertEqual(len(newJobs), nJobs)

        return


    @attr('integration')
    def testD_MyProxyDelegation(self):
        """
        _MyProxyDelegation_

        Test whether we can delegate a proxy via myproxy to this job

        IMPORTANT:
        If you are going to run this test you will have to set the serverCert/Key
        config options to point to your local server cert.  You will also have to
        run this job with your DN.  I don't recommend figuring out how to do this
        without knowing what you're doing in regards to proxy stuff.
        """

        nRunning = getCondorRunningJobs(self.user)
        self.assertEqual(nRunning, 0, "User currently has %i running jobs.  Test will not continue" % (nRunning))

        # Get the config and set the removal time to -10 for testing
        proxyDir = os.path.join(self.testDir, 'proxyDir')
        os.mkdir(proxyDir)
        config = self.getConfig()
        config.BossAir.removeTime = -10.0
        config.BossAir.pluginNames.append('VanillaCondorPlugin')
        config.BossAir.delegatedServerCert = '/uscms/home/mnorman/.globus/cms-xen39crab3devcert.pem'
        config.BossAir.delegatedServerKey  = '/uscms/home/mnorman/.globus/cms-xen39crab3devkey.pem'
        config.BossAir.myproxyServer       = 'myproxy.cern.ch'
        config.BossAir.proxyDir            = proxyDir
        config.BossAir.delegatedServerHash = 'a6f078516a0beed5dcb31ba866868fa690069f9a'

        userDN = '/DC=org/DC=doegrids/OU=People/CN=Matthew Norman 453632'

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
            tmpJob['plugin']      = 'VanillaCondorPlugin'
            tmpJob['owner']       = userDN
            tmpJob['packageDir']  = self.testDir
            tmpJob['sandbox']     = sandbox
            tmpJob['priority']    = None
            jobList.append(tmpJob)


        info = {}
        #info['packageDir'] = self.testDir
        info['index']      = 0
        info['sandbox']    = sandbox

        baAPI.submit(jobs = jobList, info = info)

        proxyFile = os.listdir(proxyDir)[0]
        stdout, stderr = SubprocessAlgos.runCommand(cmd = 'export X509_USER_PROXY=%s; voms-proxy-info' \
                                                    % os.path.join(proxyDir, proxyFile))
        self.assertEqual(stdout.split('\n')[0],
                         'subject   : %s/CN=proxy/CN=proxy/CN=proxy/CN=proxy' % userDN)

        # Now kill 'em manually
        command = ['condor_rm', self.user]
        SubprocessAlgos.runCommand(cmd = command, shell = False)

        return



if __name__ == '__main__':
    unittest.main()
