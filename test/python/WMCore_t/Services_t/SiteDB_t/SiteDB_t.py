#!/usr/bin/env python
"""
Test case for SiteDB
"""
from __future__ import print_function

import unittest

from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase

class SiteDBTest(EmulatedUnitTestCase):
    """
    Unit tests for SiteScreening module
    """

    def __init__(self, methodName='runTest'):
        super(SiteDBTest, self).__init__(methodName=methodName)

    def setUp(self):
        """
        Setup for unit tests
        """
        super(SiteDBTest, self).setUp()
        self.mySiteDB = SiteDBJSON()

    def testCmsNametoPhEDExNode(self):
        """
        Tests CMS Name to PhEDEx Node Name
        """
        target = ['T1_US_FNAL_Buffer', 'T1_US_FNAL_MSS']
        results = self.mySiteDB.cmsNametoPhEDExNode('T1_US_FNAL')
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

    def testPNNstoPSNs(self):
        """
        _testPNNstoPSNs_

        Test converting PhEDEx Node Names to Processing Site Names
        """
        result = self.mySiteDB.PNNstoPSNs(['T1_US_FNAL_Disk', 'T1_US_FNAL_Buffer', 'T1_US_FNAL_MSS'])
        self.assertTrue(result == ['T1_US_FNAL'])
        result = self.mySiteDB.PNNstoPSNs(['T2_UK_London_IC', 'T2_US_Purdue'])
        self.assertItemsEqual(result, ['T2_UK_London_IC', 'T2_US_Purdue'])
        return

    def testPSNtoPNNMap(self):
        """
        _PSNtoPNNMap_

        Test API to get a map of PSNs and PNNs 
        """
        result = self.mySiteDB.PSNtoPNNMap()
        self.assertTrue([psn for psn in result.keys() if psn.startswith('T1_')])
        self.assertTrue([psn for psn in result.keys() if psn.startswith('T2_')])
        self.assertTrue([psn for psn in result.keys() if psn.startswith('T3_')])
        self.assertTrue(len(result) > 50)

        result = self.mySiteDB.PSNtoPNNMap(psnPattern='T1.*')
        self.assertFalse([psn for psn in result.keys() if not psn.startswith('T1_')])
        self.assertTrue(len(result) < 10)

        result = self.mySiteDB.PSNtoPNNMap(psnPattern='T2.*')
        self.assertFalse([psn for psn in result.keys() if not psn.startswith('T2_')])
        self.assertTrue(len(result) > 10)

        result = self.mySiteDB.PSNtoPNNMap(psnPattern='T3.*')
        self.assertFalse([psn for psn in result.keys() if not psn.startswith('T3_')])
        self.assertTrue(len(result) > 10)

        return

    def testGetAllPhEDExNodeNames(self):
        """
        _testGetAllPhEDExNodeNames_

        Test API to get all PhEDEx Node Names 
        """
        result = self.mySiteDB.getAllPhEDExNodeNames(excludeBuffer=True)
        self.assertFalse([pnn for pnn in result if pnn.endswith('_Buffer')])

        result = self.mySiteDB.getAllPhEDExNodeNames(excludeBuffer=False)
        self.assertTrue(len([pnn for pnn in result if pnn.endswith('_Buffer')]) > 5)

        result = self.mySiteDB.getAllPhEDExNodeNames(pattern='T1.*', excludeBuffer=True)
        self.assertFalse([pnn for pnn in result if not pnn.startswith('T1_')])
        self.assertTrue(len(result) > 10)

        result = self.mySiteDB.getAllPhEDExNodeNames(pattern='.*', excludeBuffer=True)
        self.assertTrue([pnn for pnn in result if pnn.startswith('T1_')])
        self.assertTrue([pnn for pnn in result if pnn.startswith('T2_')])
        self.assertTrue([pnn for pnn in result if pnn.startswith('T3_')])
        self.assertTrue(len(result) > 60)

        return

if __name__ == '__main__':
    unittest.main()
