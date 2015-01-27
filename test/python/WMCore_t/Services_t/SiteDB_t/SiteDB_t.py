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

    def testCmsNametoSE(self):
        """
        Tests CmsNametoSE
        """
        target = [u'srm-cms-disk.gridpp.rl.ac.uk', u'srm-cms.gridpp.rl.ac.uk']
        results = self.mySiteDB.cmsNametoSE("T1_UK_RAL")
        self.failUnless(sorted(results) == sorted(target))

    def testCmsNamePatterntoSE(self):
        """
        Tests CmsNamePatterntoSE
        """
        target = [u'T2_XX_SiteA', u'T2_XX_SiteB', u'T2_XX_SiteC']
        results = self.mySiteDB.cmsNametoSE("%T2_XX")
        self.failUnless(sorted(results) == sorted(target))

    def testSEtoCmsName(self):
        """
        Tests CmsNametoSE
        """
        target = [u'T1_US_FNAL']
        results = self.mySiteDB.seToCMSName("cmssrm.fnal.gov")
        self.failUnless(results == target)
        target = sorted([u'T2_CH_CERN', u'T2_CH_CERN_AI', u'T2_CH_CERN_HLT', u'T2_CH_CERN_T0'])
        results = sorted(self.mySiteDB.seToCMSName("srm-eoscms.cern.ch"))
        self.failUnless(sorted(results) == sorted(target))

    def testDNUserName(self):
        """
        Tests DN to Username lookup
        """
        testDn = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=gutsche/CN=582680/CN=Oliver Gutsche"
        testUserName = "gutsche"
        userName = self.mySiteDB.dnUserName(dn=testDn)
        self.failUnless(testUserName == userName)

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

    def testPNNtoPSN(self):
        """
        _testPNNtoPSN_

        Test converting PhEDEx Node Name to Processing Site Name
        """

        result = self.mySiteDB.PNNtoPSN('T1_US_FNAL_Disk')
        self.failUnless(result == ['T1_US_FNAL'])
        result = self.mySiteDB.PNNtoPSN('T1_US_FNAL_Tape')
        self.failUnless(result == [])
        result = self.mySiteDB.PNNtoPSN('T2_UK_London_IC')
        self.failUnless(result == ['T2_UK_London_IC'])
        return

if __name__ == '__main__':
    unittest.main()
