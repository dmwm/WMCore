#!/usr/bin/env python
"""
Test case for CRIC
"""
from __future__ import print_function, division

import unittest

from nose.plugins.attrib import attr

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

    def testConfig(self):
        self.assertEqual(self.myCRIC['endpoint'], 'https://cms-cric.cern.ch/')
        self.assertEqual(self.myCRIC['cacheduration'], 1)
        self.assertEqual(self.myCRIC['accept_type'], 'application/json')
        self.assertEqual(self.myCRIC['content_type'], 'application/json')
        self.assertEqual(self.myCRIC['requests'].pycurl, None)

        newParams = {"cacheduration":100, "pycurl": True}
        cric = CRIC(url='https://BLAH.cern.ch/', configDict=newParams)
        self.assertEqual(cric['endpoint'], 'https://BLAH.cern.ch/')
        self.assertEqual(cric['cacheduration'], newParams['cacheduration'])
        self.assertTrue(cric['requests'].pycurl)

    @attr('integration')
    def testWhoAmI(self):
        print("This test only works with service certificates, not user proxies")
        print(self.myCRIC.whoAmI())

    def testUserNameDN(self):
        """
        Tests user name to DN lookup
        """
        expectedDN = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=amaltaro/CN=718748/CN=Alan Malta Rodrigues"
        username = "amaltaro"
        dn = self.myCRIC.userNameDn(username)
        self.assertEqual(dn, expectedDN)

    def testUserNameDNWithApostrophe(self):
        """
        Test DN lookup with an apostrophe
        """
        expectedDN = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=gdimperi/CN=728001/CN=Giulia D'Imperio"
        username = "gdimperi"
        dn = self.myCRIC.userNameDn(username)
        self.assertEqual(dn, expectedDN)

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
