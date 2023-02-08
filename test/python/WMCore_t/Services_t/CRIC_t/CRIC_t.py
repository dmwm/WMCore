#!/usr/bin/env python
"""
Test case for CRIC
"""
from __future__ import print_function, division

import unittest

from nose.plugins.attrib import attr

from Utils.PythonVersion import PY3

from WMCore.Services.CRIC.CRIC import CRIC
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase


class CRICTest(EmulatedUnitTestCase):
    """
    Unit tests for CRIC Services module
    """

    def __init__(self, methodName='runTest'):
        super(CRICTest, self).__init__(methodName=methodName)

    def setUp(self):
        """
        Setup for unit tests
        """
        super(CRICTest, self).setUp()
        self.myCRIC = CRIC()
        if PY3:
            self.assertItemsEqual = self.assertCountEqual

    def testConfig(self):
        """
        Test service attributes and the override mechanism
        """
        self.assertEqual(self.myCRIC['endpoint'], 'https://cms-cric.cern.ch/')
        self.assertEqual(self.myCRIC['cacheduration'], 1)
        self.assertEqual(self.myCRIC['accept_type'], 'application/json')
        self.assertEqual(self.myCRIC['content_type'], 'application/json')
        self.assertEqual(self.myCRIC['requests'].pycurl, True)

        newParams = {"cacheduration": 100, "pycurl": False}
        cric = CRIC(url='https://BLAH.cern.ch/', configDict=newParams)
        self.assertEqual(cric['endpoint'], 'https://BLAH.cern.ch/')
        self.assertEqual(cric['cacheduration'], newParams['cacheduration'])
        self.assertEqual(cric['requests'].pycurl, False)

    @attr('integration')
    def testWhoAmI(self):
        """
        Test user mapping information from the request headers
        """
        print("This test only works with service certificates, not user proxies")
        self.assertTrue(self.myCRIC.whoAmI())  # empty list if nothing

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

    def testGetAllPSNs(self):
        """
        Test API which fetches all PSNs
        """
        result = self.myCRIC.getAllPSNs()
        self.assertNotIn('T1_US_FNAL_Disk', result)
        self.assertNotIn('T1_US_FNAL_MSS', result)
        self.assertIn('T1_US_FNAL', result)
        self.assertIn('T2_CH_CERN', result)
        self.assertIn('T2_CH_CERN_HLT', result)

        t1s = [psn for psn in result if psn.startswith('T1_')]
        self.assertTrue(len(t1s) < 10)

        t2s = [psn for psn in result if psn.startswith('T2_')]
        self.assertTrue(len(t2s) > 30)

        t3s = [psn for psn in result if psn.startswith('T3_')]
        self.assertTrue(len(t3s) > 10)

        self.assertTrue(len(result) > 70)

        return

    def testGetAllPhEDExNodeNames(self):
        """
        Test API to get all PhEDEx Node Names
        """
        result = self.myCRIC.getAllPhEDExNodeNames(excludeBuffer=True)
        self.assertFalse([pnn for pnn in result if pnn.endswith('_Buffer')])

        result = self.myCRIC.getAllPhEDExNodeNames(excludeBuffer=False)
        self.assertTrue(len([pnn for pnn in result if pnn.endswith('_Buffer')]) > 5)

        result = self.myCRIC.getAllPhEDExNodeNames(pattern='T1.*', excludeBuffer=True)
        self.assertFalse([pnn for pnn in result if not pnn.startswith('T1_')])
        self.assertTrue(len(result) > 10)

        result = self.myCRIC.getAllPhEDExNodeNames(excludeBuffer=True)
        self.assertTrue([pnn for pnn in result if pnn.startswith('T1_')])
        self.assertTrue([pnn for pnn in result if pnn.startswith('T2_')])
        self.assertTrue([pnn for pnn in result if pnn.startswith('T3_')])
        self.assertTrue(len(result) > 60)

        result1 = self.myCRIC.getAllPhEDExNodeNames(pattern='.*', excludeBuffer=True)
        self.assertTrue([pnn for pnn in result1 if pnn.startswith('T1_')])
        self.assertTrue([pnn for pnn in result1 if pnn.startswith('T2_')])
        self.assertTrue([pnn for pnn in result1 if pnn.startswith('T3_')])
        self.assertTrue(len(result) > 60)

        self.assertItemsEqual(result, result1)

        # test a few PSNs
        self.assertNotIn('T1_US_FNAL', result)
        self.assertNotIn('T2_CH_CERN_HLT', result)

        # test a few PNNs
        self.assertIn('T1_US_FNAL_Disk', result)
        self.assertIn('T1_US_FNAL_MSS', result)
        self.assertIn('T2_CH_CERN', result)

        return

    def testPNNstoPSNs(self):
        """
        Test mapping PhEDEx Node Names to Processing Site Names
        """
        self.assertEqual(self.myCRIC.PNNstoPSNs([]), [])

        self.assertItemsEqual(self.myCRIC.PNNstoPSNs(['T1_US_FNAL_MSS']), [])
        self.assertItemsEqual(self.myCRIC.PNNstoPSNs(['T1_US_FNAL_Disk']), ['T1_US_FNAL'])

        pnns = ['T1_US_FNAL_Disk', 'T1_US_FNAL_Buffer', 'T1_US_FNAL_MSS']
        self.assertItemsEqual(self.myCRIC.PNNstoPSNs(pnns), ['T1_US_FNAL'])

        pnns = ['T2_CH_CERN', 'T2_DE_DESY']
        psns = ['T2_CH_CERN_HLT', 'T2_CH_CERN_P5', 'T2_DE_DESY', 'T2_CH_CERN']
        self.assertItemsEqual(self.myCRIC.PNNstoPSNs(pnns), psns)

        psns = ['T2_UK_London_IC', 'T3_UK_London_QMUL', 'T3_UK_SGrid_Oxford', 'T3_UK_London_RHUL', 'T3_UK_ScotGrid_GLA']
        self.assertItemsEqual(self.myCRIC.PNNstoPSNs('T2_UK_London_IC'), psns)

        pnns = ['T2_UK_London_IC', 'T2_US_Purdue']
        self.assertItemsEqual(self.myCRIC.PNNstoPSNs(pnns), psns + ['T2_US_Purdue'])

        return

    def testPSNstoPNNs(self):
        """
        Test mapping Processing Site Names to PhEDEx Node Names
        """
        self.assertEqual(self.myCRIC.PSNstoPNNs([]), [])

        # test a few PNNs
        self.assertItemsEqual(self.myCRIC.PSNstoPNNs('T1_US_FNAL_Disk'), [])
        self.assertItemsEqual(self.myCRIC.PSNstoPNNs(['T1_DE_KIT_MSS']), [])

        # test a few PSNs
        self.assertItemsEqual(self.myCRIC.PSNstoPNNs(['T1_US_FNAL']), ['T1_US_FNAL_Disk', 'T3_US_FNALLPC'])
        self.assertItemsEqual(self.myCRIC.PSNstoPNNs(['T2_CH_CERN_HLT']), ['T2_CH_CERN'])

        pnns = ['T2_CH_CERN', 'T2_CH_CERNBOX']
        self.assertItemsEqual(self.myCRIC.PSNstoPNNs(['T2_CH_CERN']), pnns)
        psns = ['T2_CH_CERN', 'T2_CH_CERN_HLT']
        self.assertItemsEqual(self.myCRIC.PSNstoPNNs(psns), pnns)

        self.assertItemsEqual(self.myCRIC.PSNstoPNNs(['T2_UK_London_IC']), ['T2_UK_London_IC'])
        pnns = ['T2_UK_London_IC', 'T2_UK_London_Brunel', 'T2_UK_SGrid_RALPP']
        self.assertItemsEqual(self.myCRIC.PSNstoPNNs('T3_UK_London_QMUL'), pnns)
        self.assertItemsEqual(self.myCRIC.PSNstoPNNs('T3_UK_ScotGrid_GLA'), pnns)
        pnns = ['T2_UK_London_IC', 'T2_UK_SGrid_RALPP']
        self.assertItemsEqual(self.myCRIC.PSNstoPNNs('T3_UK_SGrid_Oxford'), pnns)
        self.assertItemsEqual(self.myCRIC.PSNstoPNNs('T3_UK_London_RHUL'), pnns)

        self.assertItemsEqual(self.myCRIC.PSNstoPNNs(['T2_US_Purdue']), ['T2_US_Purdue'])

        return

    def testPSNtoPNNMap(self):
        """
        Test API to get a map of PSNs to PNNs
        """
        with self.assertRaises(TypeError):
            self.myCRIC.PSNtoPNNMap(1)
        with self.assertRaises(TypeError):
            self.myCRIC.PSNtoPNNMap({'config': 'blah'})
        with self.assertRaises(TypeError):
            self.myCRIC.PSNtoPNNMap(['blah'])

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

        result = self.myCRIC.PSNtoPNNMap('T2_CH_CERN$')
        self.assertItemsEqual(list(result), ['T2_CH_CERN'])
        self.assertItemsEqual(result['T2_CH_CERN'], ['T2_CH_CERNBOX', 'T2_CH_CERN'])

        # test a exact site name, which is treated as a regex and yields a confusing result!!!
        result = self.myCRIC.PSNtoPNNMap('T2_CH_CERN')
        self.assertItemsEqual(list(result), ['T2_CH_CERN', 'T2_CH_CERN_HLT', 'T2_CH_CERN_P5'])
        self.assertItemsEqual(result['T2_CH_CERN'], ['T2_CH_CERNBOX', 'T2_CH_CERN'])
        self.assertItemsEqual(result['T2_CH_CERN_HLT'], ['T2_CH_CERN'])

        result = self.myCRIC.PSNtoPNNMap('T2_CH_CERN_HLT')
        self.assertItemsEqual(list(result), ['T2_CH_CERN_HLT'])
        self.assertItemsEqual(result['T2_CH_CERN_HLT'], ['T2_CH_CERN'])

        result = self.myCRIC.PSNtoPNNMap('T1_US_FNAL')
        self.assertItemsEqual(list(result), ['T1_US_FNAL'])
        self.assertItemsEqual(result['T1_US_FNAL'], ['T1_US_FNAL_Disk', 'T3_US_FNALLPC'])

        # test a PNN as input, expecting nothing mapped to it
        self.assertItemsEqual(self.myCRIC.PSNtoPNNMap('T1_US_FNAL_Disk'), {})

        return

    def testPNNtoPSNMap(self):
        """
        Test API to get a map of PSNs to PNNs
        """
        with self.assertRaises(TypeError):
            self.myCRIC.PNNtoPSNMap(1)
        with self.assertRaises(TypeError):
            self.myCRIC.PNNtoPSNMap({'config': 'blah'})
        with self.assertRaises(TypeError):
            self.myCRIC.PNNtoPSNMap(['blah'])

        result = self.myCRIC.PNNtoPSNMap()
        self.assertTrue([psn for psn in result.keys() if psn.startswith('T1_')])
        self.assertTrue([psn for psn in result.keys() if psn.startswith('T2_')])
        self.assertTrue([psn for psn in result.keys() if psn.startswith('T3_')])
        self.assertTrue(len(result) > 50)

        result = self.myCRIC.PNNtoPSNMap(pnnPattern='T1.*')
        self.assertFalse([psn for psn in result.keys() if not psn.startswith('T1_')])
        self.assertTrue(len(result) < 10)

        result = self.myCRIC.PNNtoPSNMap(pnnPattern='T2.*')
        self.assertFalse([psn for psn in result.keys() if not psn.startswith('T2_')])
        self.assertTrue(len(result) > 10)

        result = self.myCRIC.PNNtoPSNMap(pnnPattern='T3.*')
        self.assertFalse([psn for psn in result.keys() if not psn.startswith('T3_')])
        self.assertTrue(len(result) > 10)

        result = self.myCRIC.PNNtoPSNMap('T2_CH_CERN$')
        self.assertItemsEqual(list(result), ['T2_CH_CERN'])
        self.assertItemsEqual(result['T2_CH_CERN'], ['T2_CH_CERN_HLT', 'T2_CH_CERN_P5', 'T2_CH_CERN'])

        # test a exact site name, which is treated as a regex and yields a confusing result!!!
        result = self.myCRIC.PNNtoPSNMap('T2_CH_CERN')
        self.assertItemsEqual(list(result), ['T2_CH_CERN', 'T2_CH_CERNBOX'])
        self.assertItemsEqual(result['T2_CH_CERN'], ['T2_CH_CERN_HLT', 'T2_CH_CERN_P5', 'T2_CH_CERN'])
        self.assertItemsEqual(result['T2_CH_CERNBOX'], ['T2_CH_CERN'])

        result = self.myCRIC.PNNtoPSNMap('T2_CH_CERN_HLT')
        self.assertItemsEqual(result, {})

        result = self.myCRIC.PNNtoPSNMap('T1_US_FNAL')
        self.assertItemsEqual(list(result), ['T1_US_FNAL_Disk'])
        self.assertItemsEqual(result['T1_US_FNAL_Disk'], ['T1_US_FNAL'])

        return



if __name__ == '__main__':
    unittest.main()
