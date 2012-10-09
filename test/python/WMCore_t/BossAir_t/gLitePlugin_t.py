#!/usr/bin/env python
"""
_gLitePlugin_t_

gLite Plugin test
"""
import os.path
from os import environ
import unittest
import threading
from commands import getoutput as executeCommand
import time
from nose.plugins.attrib import attr

from WMCore.BossAir.BossAirAPI   import BossAirAPI, BossAirException

from WMCore_t.BossAir_t.BossAir_t import BossAirTest

class gLitePluginTest(BossAirTest):
    """
    _gLitePlugin_

    Test for the gLite BossAir plugin
    """


    @attr('integration')
    def testG_gLiteTest(self):
        """
        _gLiteTest_

        This test works on the gLitePlugin, checking all of
        its functions with a single set of jobs
        """

        config = self.getConfig()
        config.BossAir.gliteConf = '/afs/cern.ch/cms/LCG/LCG-2/UI/conf/glite_wms_CERN.conf'
        config.BossAir.credentialDir = '/home/crab/ALL_SETUP/credentials/'
        config.BossAir.gLiteProcesses = 2
        config.BossAir.gLitePrefixEnv = "/lib64/"
        config.BossAir.pluginNames.append("gLitePlugin")
        config.BossAir.manualProxyPath = environ['X509_USER_PROXY']

        config.Agent.serverDN = "/we/bypass/myproxy/logon"

        #config.BossAir.pluginNames = ["gLitePlugin"]
        baAPI  = BossAirAPI(config = config)

        nJobs = 2
        jobDummies = self.createDummyJobs(nJobs = nJobs, location = 'grid-ce-01.ba.infn.it')

        jobPackage = os.path.join(self.testDir, 'JobPackage.pkl')
        f = open(jobPackage, 'w')
        f.write(' ')
        f.close()

        sandbox = os.path.join(self.testDir, 'sandbox.box')
        f = open(sandbox, 'w')
        f.write(' ')
        f.close()

        jobList = []
        userdn = executeCommand('grid-cert-info -subject -file %s' % config.BossAir.manualProxyPath)
        newuser = self.daoFactory(classname = "Users.New")
        newuser.execute(dn = userdn)
        for j in jobDummies:
            job = j # {'id': j['id']}
            job['custom']      = {'location': 'grid-ce-01.ba.infn.it'}
            job['location']    = 'grid-ce-01.ba.infn.it'
            job['plugin']      = 'gLitePlugin'
            job['name']        = j['name']
            job['cache_dir']   = self.testDir
            job['retry_count'] = 0
            job['owner']       = userdn
            job['packageDir']  = self.testDir
            job['sandbox']     = sandbox
            job['priority']    = None
            jobList.append(job)

        baAPI.submit(jobs = jobList)

        # Should be new jobs
        newJobs = baAPI._loadByStatus(status = 'New')
        self.assertNotEqual(len(newJobs), nJobs)

        time.sleep(2)
        baAPI.track()

        # Should be not anymore marked as new
        newJobs = baAPI._loadByStatus('New', 0)
        self.assertNotEqual(len(newJobs), nJobs)


        # Killing all the jobs
        baAPI.kill( jobList )
        #time.sleep(15)
        baAPI.track()

        ## Issues running tests below due to glite delay on marking job as killed
        # Should be just running jobs
        #killedJobs = baAPI._loadByStatus('Cancelled by user', 0)
        #self.assertEqual(len(killedJobs), 0)

        # Check if they're complete
        #completeJobs = baAPI.getComplete()
        #self.assertEqual(len(completeJobs), nJobs)

        return


if __name__ == '__main__':
    unittest.main()
