"""
Unit tests for MSManager.py module

Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
"""

from __future__ import division, print_function

import unittest

from WMCore.MicroService.Unified.MSManager import MSManager
from WMCore.Agent.Configuration import Configuration


class MSManagerTest(unittest.TestCase):
    "Unit test for MSManager module"
    def setUp(self):
        "Setup MSManager for testing"
        config = Configuration()
        self.mgr = MSManager(config)

        config = Configuration()
        config.section_("MS")
        config.MS.services = ['transferor']
        self.mgr_trans = MSManager(config.MS)

        config = Configuration()
        config.section_("MS")
        config.MS.services = ['monitor']
        self.mgr_monit = MSManager(config.MS)

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

if __name__ == '__main__':
    unittest.main()
