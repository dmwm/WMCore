#!/usr/bin/env python
"""
Test case for SiteDB
"""
from __future__ import print_function

import unittest

from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
from WMCore.Services.EmulatorSwitch import EmulatorHelper
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase

class SiteDBTest(EmulatedUnitTestCase):
    """
    Unit tests for SiteScreening module
    """

    def  __init__(self, methodName='runTest'):
        super(SiteDBTest, self).__init__(methodName=methodName)

    def setUp(self):
        """
        Setup for unit tests
        """
        super(SiteDBTest, self).setUp()
        EmulatorHelper.setEmulators(phedex=False, dbs=False, siteDB=False, requestMgr=True)
        self.mySiteDB = SiteDBJSON()


    def tearDown(self):
        """
        _tearDown_
        """
        super(SiteDBTest, self).tearDown()
        EmulatorHelper.resetEmulators()
        return


    def testCmsNametoPhEDExNode(self):
        """
        #Tests CmsNametoSE
        """
        target = ['T1_US_FNAL_Buffer', 'T1_US_FNAL_MSS']
        results = self.mySiteDB.cmsNametoPhEDExNode('T1_US_FNAL')
        self.assertItemsEqual(results, target)

    def testSEtoCmsName(self):
        """
        Tests CmsNametoSE
        """
        target = [u'T1_US_FNAL', u'T1_US_FNAL_Disk']
        results = self.mySiteDB.seToCMSName("cmsdcadisk01.fnal.gov")
        self.assertTrue(results == target)
        target = sorted([u'T2_CH_CERN', u'T2_CH_CERN_HLT'])
        results = sorted(self.mySiteDB.seToCMSName("srm-eoscms.cern.ch"))
        self.assertItemsEqual(results, target)
        target = sorted([u'T0_CH_CERN', u'T1_CH_CERN'])
        results = sorted(self.mySiteDB.seToCMSName("srm-cms.cern.ch"))
        self.assertItemsEqual(results, target)
        target = sorted([u'T2_CH_CERN_AI'])
        results = sorted(self.mySiteDB.seToCMSName("eoscmsftp.cern.ch"))
        self.assertItemsEqual(results, target)

    def testDNUserName(self):
        """
        Tests DN to Username lookup
        """
        testDn = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=jha/CN=618566/CN=Manoj Jha"
        testUserName = "jha"
        userName = self.mySiteDB.dnUserName(dn=testDn)
        self.assertTrue(testUserName == userName)

    def testDNWithApostrophe(self):
        """
        Tests a DN with an apostrophy in - will fail till SiteDB2 appears
        """
        testDn = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=liviof/CN=472739/CN=Livio Fano'"
        testUserName = "liviof"
        userName = self.mySiteDB.dnUserName(dn=testDn)
        self.assertTrue(testUserName == userName)

    def testSEFinder(self):
        """
        _testSEFinder_

        See if we can retrieve seNames from all sites
        """

        seNames = self.mySiteDB.getAllSENames()
        self.assertTrue(len(seNames) > 1)
        self.assertTrue('cmsdcadisk01.fnal.gov' in seNames)
        return

    def testPNNtoPSN(self):
        """
        _testPNNtoPSN_

        Test converting PhEDEx Node Name to Processing Site Name
        """

        result = self.mySiteDB.PNNtoPSN('T1_US_FNAL_Disk')
        self.assertTrue(result == ['T1_US_FNAL'])
        result = self.mySiteDB.PNNtoPSN('T1_US_FNAL_Tape')
        self.assertTrue(result == [])
        result = self.mySiteDB.PNNtoPSN('T2_UK_London_IC')
        self.assertTrue(result == ['T2_UK_London_IC'])
        return

    def testCMSNametoList(self):
        """
        Test PNN to storage list
        """
        result = self.mySiteDB.cmsNametoList("T1_US*", "SE")
        self.assertItemsEqual(result, [u'cmsdcadisk01.fnal.gov'])

    def testCheckAndConvertSENameToPNN(self):
        """
        Test the conversion of SE name to PNN for single
        and multiple sites/PNNs using checkAndConvertSENameToPNN
        """

        fnalSE = u'cmsdcadisk01.fnal.gov'
        purdueSE = u'srm.rcac.purdue.edu'
        fnalPNNs = [u'T1_US_FNAL_Buffer', u'T1_US_FNAL_MSS', u'T1_US_FNAL_Disk']
        purduePNN = [u'T2_US_Purdue']

        pnnList = fnalPNNs + purduePNN
        seList = [fnalSE, purdueSE]

        self.assertItemsEqual(self.mySiteDB.checkAndConvertSENameToPNN(fnalSE), fnalPNNs)
        self.assertItemsEqual(self.mySiteDB.checkAndConvertSENameToPNN([fnalSE]), fnalPNNs)
        self.assertItemsEqual(self.mySiteDB.checkAndConvertSENameToPNN(purdueSE), purduePNN)
        self.assertItemsEqual(self.mySiteDB.checkAndConvertSENameToPNN([purdueSE]), purduePNN)
        self.assertItemsEqual(self.mySiteDB.checkAndConvertSENameToPNN(seList), purduePNN + fnalPNNs)
        self.assertItemsEqual(self.mySiteDB.checkAndConvertSENameToPNN(pnnList), pnnList)
        return


if __name__ == '__main__':
    unittest.main()
