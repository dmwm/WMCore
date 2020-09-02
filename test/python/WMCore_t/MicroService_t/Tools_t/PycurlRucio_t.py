#!/usr/bin/env python
"""
Unittests for the Rucio wrapper PyCurl-based module
"""
from __future__ import division, print_function

import unittest

from WMCore.MicroService.Tools.PycurlRucio import (getRucioToken, parseNewLineJson,
                                                   getPileupContainerSizesRucio, listReplicationRules,
                                                   getBlocksAndSizeRucio)

CONT1 = "/Pseudoscalar2HDM_MonoZLL_mScan_mH-500_ma-300/DMWM_Test-TC_PreMix_Agent122_Validation_Alanv1-v11/MINIAODSIM"
CONT2 = "/RelValH125GGgluonfusion_14/Integ_Test-SC_LumiMask_PhEDEx_HG2004_Val_Privv19-v11/NANOAODSIM"
CONT3 = "/RelValH125GGgluonfusion_13/CMSSW_8_1_0-RecoFullPU_2017PU_TaskChain_PUMCRecyc_Agent114_Validation_TEST_Alan_v18-v11/DQMIO"
CONT4 = "/DMSimp_MonoZLL_NLO_Vector_TuneCP3_GQ0p25_GDM1p0_MY1-500_MXd-1/RunIIFall17NanoAODv4-PU2017_12Apr2018_Nano14Dec2018_102X_mc2017_realistic_v6-v1/NANOAODSIM"


class PycurlRucioTests(unittest.TestCase):
    """Test pycurl_manager module"""

    def setUp(self):
        "initialization"
        self.rucioUrl = "http://cmsrucio-int.cern.ch"
        self.rucioAuthUrl = "https://cmsrucio-auth-int.cern.ch"
        self.rucioAccount = "wma_test"
        self.rucioScope = "cms"
        self.rucioToken, self.tokenValidity = getRucioToken(self.rucioAuthUrl, self.rucioAccount)
        self.badDID = "/wrong/did/name"

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
        resp = getPileupContainerSizesRucio([], self.rucioUrl,
                                            self.rucioToken, scope=self.rucioScope)
        self.assertEqual(resp, {})

        containers = [CONT1, CONT2, self.badDID]
        resp = getPileupContainerSizesRucio(containers, self.rucioUrl,
                                            self.rucioToken, scope=self.rucioScope)
        self.assertTrue(len(resp) == 3)
        self.assertTrue(CONT2 in resp)
        self.assertTrue(resp[CONT1] > 0)
        self.assertIsNone(resp[self.badDID])

    def testListReplicationRules(self):
        """
        Test the listReplicationRules function, which fetches the container
        rules a return a list of RSEs key'ed by the container name.
        """
        resp = listReplicationRules([], self.rucioAccount, grouping="A",
                                    rucioUrl=self.rucioUrl, rucioToken=self.rucioToken, scope=self.rucioScope)
        self.assertEqual(resp, {})

        with self.assertRaises(RuntimeError):
            listReplicationRules(["blah"], self.rucioAccount, grouping="Not valid!!!",
                                 rucioUrl=self.rucioUrl, rucioToken=self.rucioToken, scope=self.rucioScope)

        containers = [CONT2, CONT3, self.badDID]
        resp = listReplicationRules(containers, self.rucioAccount, grouping="A",
                                    rucioUrl=self.rucioUrl, rucioToken=self.rucioToken, scope=self.rucioScope)
        self.assertTrue(len(resp) == 3)
        self.assertTrue(CONT2 in resp)
        self.assertTrue(isinstance(resp[CONT3], list))
        self.assertEqual(resp[self.badDID], [])

    def testGetBlocksAndSizeRucio(self):
        """
        Test the getBlocksAndSizeRucio function, which fetches all the blocks
        (in a container) and their sizes.
        """
        BLOCK = "/DMSimp_MonoZLL_NLO_Vector_TuneCP3_GQ0p25_GDM1p0_MY1-500_MXd-1/RunIIFall17NanoAODv4-PU2017_12Apr2018_Nano14Dec2018_102X_mc2017_realistic_v6-v1/NANOAODSIM#048c25e9-38bb-496d-86f7-405ffd3d3fd8"
        resp = getBlocksAndSizeRucio([], self.rucioUrl, self.rucioToken, self.rucioScope)
        self.assertEqual(resp, {})

        containers = [CONT2, CONT4, self.badDID]
        resp = getBlocksAndSizeRucio(containers, self.rucioUrl, self.rucioToken, self.rucioScope)
        self.assertTrue(len(resp) == 3)
        self.assertTrue(CONT2 in resp)
        self.assertTrue(len(resp[CONT4]) > 3)
        self.assertItemsEqual(resp[CONT4][BLOCK].viewkeys(), ["blockSize", "locations"])
        self.assertIsNone(resp[self.badDID])


if __name__ == '__main__':
    unittest.main()
