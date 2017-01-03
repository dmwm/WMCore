#!/usr/bin/python

"""
A test of a emulator set up
"""

import unittest

from WMCore.Services.EmulatorSwitch import EmulatorHelper
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.RequestManager.RequestManager import RequestManager
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON


class EmulatorSetupTest(unittest.TestCase):
    """
    A test of a emulator set up
    """

    def setUp(self):
        self.globalDBS = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"

    def testEmulator(self):
        """
        The the remaining old-style emulators
        """

        EmulatorHelper.setEmulators(True, False, True, True)
        self.assertEqual(PhEDEx().wrapped.__module__,
                         'WMQuality.Emulators.PhEDExClient.PhEDEx')
        self.assertEqual(SiteDBJSON().wrapped.__module__,
                         'WMQuality.Emulators.SiteDBClient.SiteDB')
        self.assertEqual(RequestManager().wrapped.__module__,
                         'WMQuality.Emulators.RequestManagerClient.RequestManager')

        self.assertEqual(PhEDEx().__class__.__name__, 'PhEDEx')
        self.assertEqual(SiteDBJSON().__class__.__name__, 'SiteDBJSON')
        self.assertEqual(RequestManager().__class__.__name__, 'RequestManager')

        EmulatorHelper.resetEmulators()
        self.assertEqual(PhEDEx().wrapped.__module__,
                         'WMCore.Services.PhEDEx.PhEDEx')
        self.assertEqual(SiteDBJSON().wrapped.__module__,
                         'WMCore.Services.SiteDB.SiteDB')
        self.assertEqual(RequestManager().wrapped.__module__,
                         'WMCore.Services.RequestManager.RequestManager')

        self.assertEqual(PhEDEx().__class__.__name__, 'PhEDEx')
        self.assertEqual(SiteDBJSON().__class__.__name__, 'SiteDBJSON')
        self.assertEqual(RequestManager().__class__.__name__, 'RequestManager')


if __name__ == "__main__":
    unittest.main()
