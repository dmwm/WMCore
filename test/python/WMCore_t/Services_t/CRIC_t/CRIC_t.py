#!/usr/bin/env python
"""
Test case for CRIC
"""
from __future__ import print_function

import unittest

from WMCore.Services.CRIC.CRIC import CRIC
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase

class CRICTest(EmulatedUnitTestCase):
    """
    Unit tests for SiteScreening module
    """

    def __init__(self, methodName='runTest'):
        super(CRICTest, self).__init__(methodName=methodName)

    def setUp(self):
        """
        Setup for unit tests
        """
        super(CRICTest, self).setUp()
        self.myCRIC = CRIC()

    def testWhoAmI(self):
        print("XXXXX")
        print(self.myCRIC.whoAmI())

    def testUserNameDN(self):
        """
        Tests DN to Username lookup
        """
        testDn = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=jha/CN=618566/CN=Manoj Jha"
        testUserName = "jha"
        userName = self.myCRIC.userNameDn(testDn)
        self.assertTrue(testUserName == userName)

    def testDNWithApostrophe(self):
        """
        Tests a DN with an apostrophy in - will fail till CRIC2 appears
        """
        testDn = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=liviof/CN=472739/CN=Livio Fano'"
        testUserName = "liviof"
        userName = self.myCRIC.userNameDn(testDn)
        self.assertTrue(testUserName == userName)

    def testPNNstoPSNs(self):
        """
        _testPNNstoPSNs_

        Test converting PhEDEx Node Names to Processing Site Names
        """
        result = self.myCRIC.PNNstoPSNs(['T1_US_FNAL_Disk', 'T1_US_FNAL_Buffer', 'T1_US_FNAL_MSS'])
        self.assertTrue(result == ['T1_US_FNAL'])
        result = self.myCRIC.PNNstoPSNs(['T2_UK_London_IC', 'T2_US_Purdue'])
        print(result)
        self.assertItemsEqual(result, [u'T3_UK_London_QMUL', u'T2_US_Purdue', u'T3_UK_SGrid_Oxford',
                                       u'T2_UK_London_IC', u'T3_UK_ScotGrid_GLA', u'T3_UK_London_RHUL'])
        return

    def testPSNtoPNNMap(self):
        """
        _PSNtoPNNMap_

        Test API to get a map of PSNs and PNNs
        """
        result = self.myCRIC.PSNtoPNNMap()
        self.assertTrue([psn for psn in result.keys() if psn.startswith('T1_')])
        self.assertTrue([psn for psn in result.keys() if psn.startswith('T2_')])
        self.assertTrue([psn for psn in result.keys() if psn.startswith('T3_')])
        self.assertTrue(len(result) > 50)

        result = self.myCRIC.PSNtoPNNMap(psnPattern='T1.*')
        self.assertFalse([psn for psn in result.keys() if not psn.startswith('T1_')])
        self.assertTrue(len(result) < 10)

        result = self.myCRIC.PSNtoPNNMap(psnPattern='T2.*')
        self.assertFalse([psn for psn in result.keys() if not psn.startswith('T2_')])
        self.assertTrue(len(result) > 10)

        result = self.myCRIC.PSNtoPNNMap(psnPattern='T3.*')
        self.assertFalse([psn for psn in result.keys() if not psn.startswith('T3_')])
        self.assertTrue(len(result) > 10)

        return

    def testGetAllPSNs(self):

        print(self.myCRIC.getAllPSNs())

    def testGetAllPhEDExNodeNames(self):
        """
        _testGetAllPhEDExNodeNames_

        Test API to get all PhEDEx Node Names
        """
        result = self.myCRIC.getAllPhEDExNodeNames(excludeBuffer=True)
        self.assertFalse([pnn for pnn in result if pnn.endswith('_Buffer')])

        result = self.myCRIC.getAllPhEDExNodeNames(excludeBuffer=False)
        self.assertTrue(len([pnn for pnn in result if pnn.endswith('_Buffer')]) > 5)

        result = self.myCRIC.getAllPhEDExNodeNames(pattern='T1.*', excludeBuffer=True)
        self.assertFalse([pnn for pnn in result if not pnn.startswith('T1_')])
        self.assertTrue(len(result) > 10)

        result = self.myCRIC.getAllPhEDExNodeNames(pattern='.*', excludeBuffer=True)
        self.assertTrue([pnn for pnn in result if pnn.startswith('T1_')])
        self.assertTrue([pnn for pnn in result if pnn.startswith('T2_')])
        self.assertTrue([pnn for pnn in result if pnn.startswith('T3_')])
        self.assertTrue(len(result) > 60)

        return

if __name__ == '__main__':
    unittest.main()
