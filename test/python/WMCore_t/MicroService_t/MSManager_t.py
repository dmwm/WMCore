"""
Unit tests for MSManager.py module

Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
"""

from __future__ import division, print_function

import unittest

from Utils.PythonVersion import PY3

from WMCore.Agent.Configuration import Configuration
from WMCore.MicroService.MSManager import MSManager


class MSManagerTest(unittest.TestCase):
    "Unit test for MSManager module"

    def setUp(self):
        "Setup MSManager for testing"
        config = Configuration()
        data = config.section_('data')
        data.reqmgr2Url = "http://localhost/reqmgr2"
        data.verbose = True
        data.interval = 600
        data.quotaUsage = 0.8
        data.quotaAccount = "DataOps"
        data.enableStatusTransition = True
        data.rucioAccount = "wma_test"
        data.rucioUrl = "http://cms-rucio-int.cern.ch"
        data.rucioAuthUrl = "https://cms-rucio-auth-int.cern.ch"
        data.phedexUrl = "https://cmsweb.cern.ch/phedex/datasvc/json/prod"
        data.dbsUrl = "https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader"
        data.smtpServer = "localhost"
        data.fromAddr = "noreply@cern.ch"
        data.toAddr = ["cms-comp-ops-workflow-team@cern.ch"]
        data.warningTransferThreshold = 100. * (1000 ** 4)  # 100 TB (terabyte)
        self.mgr = MSManager(data)

        data.services = ['monitor']
        self.mgr_monit = MSManager(data)

        data.services = ['transferor']
        data.limitRequestsPerCycle = 50
        data.enableDataTransfer = True
        self.mgr_trans = MSManager(data)

        if PY3:
            self.assertItemsEqual = self.assertCountEqual

    def tearDown(self):
        "Tear down MSManager"
        self.mgr.stop()
        self.mgr_trans.stop()
        self.mgr_monit.stop()

    def test_services(self):
        "test MSManager services"
        # check self.mgrt object attributes
        self.assertItemsEqual(self.mgr.services, [])
        self.assertEqual(hasattr(self.mgr, 'msTransferor'), False)
        self.assertEqual(hasattr(self.mgr, 'transfThread'), False)
        self.assertEqual(hasattr(self.mgr, 'msMonitor'), False)
        self.assertEqual(hasattr(self.mgr, 'monitThread'), False)

        # check self.mgr_trans object attributes
        self.assertEqual('monitor' in self.mgr_trans.services, False)
        self.assertEqual('transferor' in self.mgr_trans.services, True)
        self.assertEqual(hasattr(self.mgr_trans, 'msTransferor'), True)
        self.assertEqual(hasattr(self.mgr_trans, 'transfThread'), True)
        self.assertEqual(hasattr(self.mgr_trans, 'msMonitor'), False)
        self.assertEqual(hasattr(self.mgr_trans, 'monitThread'), False)

        # check self.mgr_monit object attributes
        self.assertEqual('monitor' in self.mgr_monit.services, True)
        self.assertEqual('transferor' in self.mgr_monit.services, False)
        self.assertEqual(hasattr(self.mgr_monit, 'msTransferor'), False)
        self.assertEqual(hasattr(self.mgr_monit, 'transfThread'), False)
        self.assertEqual(hasattr(self.mgr_monit, 'msMonitor'), True)
        self.assertEqual(hasattr(self.mgr_monit, 'monitThread'), True)

        # test a few configuration parameters as well
        self.assertEqual(self.mgr_trans.msConfig.get("limitRequestsPerCycle"), 50)
        self.assertFalse("limitRequestsPerCycle" in self.mgr_monit.msConfig)
        self.assertTrue(self.mgr_trans.msConfig.get("enableStatusTransition"))
        self.assertTrue("enableStatusTransition" in self.mgr_monit.msConfig)
        self.assertTrue(self.mgr_trans.msConfig.get("enableDataTransfer"))
        self.assertFalse("enableDataTransfer" in self.mgr_monit.msConfig)


if __name__ == '__main__':
    unittest.main()
