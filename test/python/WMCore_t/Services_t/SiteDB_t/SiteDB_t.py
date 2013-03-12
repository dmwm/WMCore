#!/usr/bin/env python
"""
Test case for SiteDB
"""

import unittest

from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
from WMCore.Services.EmulatorSwitch import EmulatorHelper

from nose.plugins.attrib import attr

class SiteDBTest(unittest.TestCase):
    """
    Unit tests for SiteScreening module
    """

    def setUp(self):
        """
        Setup for unit tests
        """
        EmulatorHelper.setEmulators(siteDB = True)
        self.mySiteDB = SiteDBJSON()

    def tearDown(self):
        EmulatorHelper.resetEmulators()

    def testCmsNametoPhEDExNode(self):
        """
        Tests CmsNametoSE
        """
        target = ['T1_US_FNAL_MSS','T1_US_FNAL_Buffer']
        results = self.mySiteDB.cmsNametoPhEDExNode("T1_US_FNAL")
        self.failUnless(sorted(results) == sorted(target))

    def testPhEDExNodetocmsName(self):
        """
        Tests PhEDExNodetocmsName
        """
        result = self.mySiteDB.phEDExNodetocmsName('T1_US_FNAL_MSS')
        self.failUnless(result == 'T1_US_FNAL')
        result = self.mySiteDB.phEDExNodetocmsName('T1_US_FNAL_Buffer')
        self.failUnless(result == 'T1_US_FNAL')
        result = self.mySiteDB.phEDExNodetocmsName('T2_UK_London_IC')
        self.failUnless(result == 'T2_UK_London_IC')
        # don't check this anymore, see comment in phEDExNodetocmsName function
        #self.assertRaises(ValueError, self.mySiteDB.phEDExNodetocmsName,
        #                  'T9_DOESNT_EXIST_Buffer')

    def testCmsNametoSE(self):
        """
        Tests CmsNametoSE
        """
        target = ['srm-cms.gridpp.rl.ac.uk']
        results = self.mySiteDB.cmsNametoSE("T1_UK_RAL")
        self.failUnless(sorted(results) == sorted(target))

    def testSEtoCmsName(self):
        """
        Tests CmsNametoSE
        """
        target = 'T1_US_FNAL'
        results = self.mySiteDB.seToCMSName("cmssrm.fnal.gov")
        self.failUnless(results == target)

    def testCmsNametoCE(self):
        """
        Tests CmsNametoCE
        """
        target = ['lcgce11.gridpp.rl.ac.uk', 'lcgce10.gridpp.rl.ac.uk',
                  'lcgce02.gridpp.rl.ac.uk']
        results = self.mySiteDB.cmsNametoCE("T1_UK_RAL")
        self.failUnless(sorted(results) == sorted(target))

    def testDNUserName(self):
        """
        Tests DN to Username lookup
        """
        testDn = "/C=UK/O=eScience/OU=Bristol/L=IS/CN=simon metson"
        testUserName = "metson"
        userName = self.mySiteDB.dnUserName(dn=testDn)
        self.failUnless(testUserName == userName)

    @attr("integration")
    def testDNWithApostrophe(self):
        """
        Tests a DN with an apostrophy in - will fail till SiteDB2 appears
        """
        testDn = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=liviof/CN=472739/CN=Livio Fano'"
        testUserName = "liviof"
        userName = self.mySiteDB.dnUserName(dn=testDn)
        self.failUnless(testUserName == userName)

    def testSEFinder(self):
        """
        _testSEFinder_

        See if we can retrieve seNames from all sites
        """

        seNames = self.mySiteDB.getAllSENames()
        self.assertTrue(len(seNames) > 1)
        self.assertTrue('cmssrm.fnal.gov' in seNames)
        return

if __name__ == '__main__':
    unittest.main()
