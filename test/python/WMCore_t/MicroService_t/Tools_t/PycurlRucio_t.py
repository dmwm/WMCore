#!/usr/bin/env python
"""
Unittests for the Rucio wrapper PyCurl-based module
"""
from __future__ import division, print_function
from builtins import str

import unittest

from Utils.PythonVersion import PY3

from WMCore.MicroService.Tools.PycurlRucio import (getRucioToken, parseNewLineJson,
                                                   getPileupContainerSizesRucio, listReplicationRules,
                                                   getBlocksAndSizeRucio, stringDateToEpoch)

CONT1 = "/NoBPTX/Run2018D-12Nov2019_UL2018-v1/MINIAOD"
CONT2 = "/NoBPTX/Run2016F-23Sep2016-v1/DQMIO"
CONT3 = "/RelValTTbar_14TeV/CMSSW_11_2_0_pre8-112X_mcRun3_2024_realistic_v10_forTrk-v1/GEN-SIM"
# Pileup container
PU_CONT = "/Neutrino_E-10_gun/RunIISummer20ULPrePremix-UL16_106X_mcRun2_asymptotic_v13-v1/PREMIX"
PU_CONT_BLK = "/Neutrino_E-10_gun/RunIISummer20ULPrePremix-UL16_106X_mcRun2_asymptotic_v13-v1/PREMIX#00287791-198c-4000-aafa-a796878d51bb"


class PycurlRucioTests(unittest.TestCase):
    """Test pycurl_manager module"""

    def setUp(self):
        "initialization"
        self.rucioUrl = "http://cms-rucio-int.cern.ch"
        self.rucioAuthUrl = "https://cms-rucio-auth-int.cern.ch"
        self.rucioAccount = "wma_test"
        self.rucioScope = "cms"
        #self.rucioToken, self.tokenValidity = getRucioToken(self.rucioAuthUrl, self.rucioAccount)
        self.badDID = "/wrong/did/name"
        if PY3:
            self.assertItemsEqual = self.assertCountEqual

    def testParseNewLineJson(self):
        """
        Test the parseNewLineJson function
        """
        validate = {"1": {"first_item": 1},
                    "2": {"second_item": "2nd"},
                    "3": {"third_item": None}}
        dataStream = '{"first_item": 1}\n{"second_item": "2nd"}\n{"third_item": null}\n'
        for num, item in enumerate(parseNewLineJson(dataStream)):
            self.assertItemsEqual(item, validate[str(num + 1)])

    def testGetPileupContainerSizesRucio(self):
        """
        Test the getPileupContainerSizesRucio function, which fetches the container
        total bytes.
        """
        self.rucioToken, self.tokenValidity = getRucioToken(self.rucioAuthUrl, self.rucioAccount)
        # Test 1: no DIDs provided as input
        resp = getPileupContainerSizesRucio([], self.rucioUrl,
                                            self.rucioToken, scope=self.rucioScope)
        self.assertEqual(resp, {})

        # Test 2: multiple valid/invalid DIDs provided as input
        containers = [CONT1, PU_CONT, self.badDID]
        resp = getPileupContainerSizesRucio(containers, self.rucioUrl,
                                            self.rucioToken, scope=self.rucioScope)
        self.assertTrue(len(resp) == 3)
        self.assertTrue(resp[PU_CONT] > 0)
        self.assertIsNone(resp[self.badDID])

    def testListReplicationRules(self):
        """
        Test the listReplicationRules function, which fetches the container
        rules a return a list of RSEs key'ed by the container name.
        """
        self.rucioToken, self.tokenValidity = getRucioToken(self.rucioAuthUrl, self.rucioAccount)
        resp = listReplicationRules([], self.rucioAccount, grouping="A",
                                    rucioUrl=self.rucioUrl, rucioToken=self.rucioToken, scope=self.rucioScope)
        self.assertEqual(resp, {})

        with self.assertRaises(RuntimeError):
            listReplicationRules(["blah"], self.rucioAccount, grouping="Not valid!!!",
                                 rucioUrl=self.rucioUrl, rucioToken=self.rucioToken, scope=self.rucioScope)

        containers = [PU_CONT, CONT2, self.badDID]
        resp = listReplicationRules(containers, self.rucioAccount, grouping="A",
                                    rucioUrl=self.rucioUrl, rucioToken=self.rucioToken, scope=self.rucioScope)
        self.assertTrue(len(resp) == 3)
        self.assertTrue(PU_CONT in resp)
        self.assertTrue(isinstance(resp[CONT2], list))
        self.assertEqual(resp[self.badDID], [])

    def testGetBlocksAndSizeRucio(self):
        """
        Test the getBlocksAndSizeRucio function, which fetches all the blocks
        (in a container) and their sizes.
        """
        self.rucioToken, self.tokenValidity = getRucioToken(self.rucioAuthUrl, self.rucioAccount)
        # Test 1: no DIDs provided as input
        resp = getBlocksAndSizeRucio([], self.rucioUrl, self.rucioToken, self.rucioScope)
        self.assertEqual(resp, {})
        # Test 2: multiple valid/invalid DIDs provided as input
        containers = [PU_CONT, CONT3, self.badDID]
        resp = getBlocksAndSizeRucio(containers, self.rucioUrl, self.rucioToken, self.rucioScope)
        self.assertTrue(len(resp) == 3)
        self.assertTrue(resp[PU_CONT][PU_CONT_BLK]['blockSize'] > 0)
        self.assertTrue(isinstance(resp[PU_CONT][PU_CONT_BLK]['locations'], list))
        self.assertTrue(len(resp[CONT3]) > 0)
        self.assertIsNone(resp[self.badDID])

    def testStringDateToEpoch(self):
        """
        Test the stringDateToEpoch function
        """
        dateString = 'Thu, 29 Apr 2021 13:15:42 UTC'
        self.assertEqual(1619694942, stringDateToEpoch(dateString))
        dateString = 'Thu, 1 Jan 1970 01:00:00 UTC'
        self.assertEqual(0, stringDateToEpoch(dateString))


if __name__ == '__main__':
    unittest.main()
