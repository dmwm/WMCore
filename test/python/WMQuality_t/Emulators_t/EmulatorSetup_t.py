#!/usr/bin/python
import unittest
from WMCore.Services.EmulatorSwitch import EmulatorHelper
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.DBS.DBSReader import DBSReader
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
from WMCore.Services.RequestManager.RequestManager import RequestManager

class EmulatorSetupTest(unittest.TestCase):
    """
    A test of a emulator set up
    """
    def setUp(self):
        self.globalDBS = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"

    def testEmulator(self):

        EmulatorHelper.setEmulators(True, True, True, True)
        self.assertEqual(PhEDEx().wrapped.__module__,
                         'WMQuality.Emulators.PhEDExClient.PhEDEx')
        self.assertEqual(DBSReader(self.globalDBS).wrapped.__module__,
                         'WMQuality.Emulators.DBSClient.DBSReader')
        self.assertEqual(SiteDBJSON().wrapped.__module__,
                         'WMQuality.Emulators.SiteDBClient.SiteDB')
        self.assertEqual(RequestManager().wrapped.__module__,
                         'WMQuality.Emulators.RequestManagerClient.RequestManager')

        self.assertEqual(PhEDEx().__class__.__name__, 'PhEDEx')
        self.assertEqual(DBSReader(self.globalDBS).__class__.__name__, 'DBSReader')
        self.assertEqual(SiteDBJSON().__class__.__name__, 'SiteDBJSON')
        self.assertEqual(RequestManager().__class__.__name__, 'RequestManager')

        EmulatorHelper.resetEmulators()
        self.assertEqual(PhEDEx().wrapped.__module__,
                         'WMCore.Services.PhEDEx.PhEDEx')
        self.assertEqual(DBSReader(self.globalDBS).wrapped.__module__,
                         'WMCore.Services.DBS.DBS2Reader')
        self.assertEqual(SiteDBJSON().wrapped.__module__,
                         'WMCore.Services.SiteDB.SiteDB')
        self.assertEqual(RequestManager().wrapped.__module__,
                         'WMCore.Services.RequestManager.RequestManager')

        self.assertEqual(PhEDEx().__class__.__name__, 'PhEDEx')
        self.assertEqual(DBSReader(self.globalDBS).__class__.__name__, 'DBS2Reader')
        self.assertEqual(SiteDBJSON().__class__.__name__, 'SiteDBJSON')
        self.assertEqual(RequestManager().__class__.__name__, 'RequestManager')

if __name__ == "__main__":
    unittest.main()
