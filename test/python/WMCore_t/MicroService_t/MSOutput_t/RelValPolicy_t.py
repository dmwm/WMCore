"""
Unit tests for the WMCore/MicroService/MSOutput/RelValPolicy.py module
"""
from __future__ import division, print_function

import json
import unittest

from WMCore.MicroService.MSOutput.RelValPolicy import RelValPolicy, RelValPolicyException


class RelValPolicyTests(unittest.TestCase):
    """Unit tests for the RelValPolicy module"""

    def setUp(self):
        """Basic setup for each unit test"""
        pass

    def tearDown(self):
        """Basic tear down operation when leaving each unit test"""
        pass

    def testBrokenTierPolicy(self):
        """Tests for the RelValPolicy class with a broken tier-based policy"""
        validDatatiers = ["GEN-SIM", "GEN", "SIM", "AOD"]
        validRSEs = ["rse_1", "rse_2", "rse_3"]
        timePol = [{"releaseType": "pre", "lifetimeSecs": 1},
                   {"releaseType": "default", "lifetimeSecs": 2}]

        # test output policies with a wrong data type
        for testPolicy in [None, {}, "blah", 123]:
            with self.assertRaises(RelValPolicyException):
                RelValPolicy(testPolicy, timePol, validDatatiers, validRSEs)

        # test internal structure with wrong data type for datatier
        testPolicy = [{"datatier": ["tier1", "tier2"], "destinations": ["rse_1", "rse_2"]},
                      {"datatier": "default", "destinations": ["rse_1"]}]
        with self.assertRaises(RelValPolicyException):
            RelValPolicy(testPolicy, timePol, validDatatiers, validRSEs)

        # test internal structure with wrong data type for destinations
        testPolicy = [{"datatier": "tier1", "destinations": "rse_1"},
                      {"datatier": "default", "destinations": ["rse_1"]}]
        with self.assertRaises(RelValPolicyException):
            RelValPolicy(testPolicy, timePol, validDatatiers, validRSEs)

        # test internal structure missing the required "default" key/value pair
        testPolicy = [{"datatier": "GEN", "destinations": ["rse_1", "rse_2"]}]
        with self.assertRaises(RelValPolicyException):
            RelValPolicy(testPolicy, timePol, validDatatiers, validRSEs)

    def testBrokenLifePolicy(self):
        """Tests for the RelValPolicy class with a broken lifetime-based policy"""
        validDatatiers = ["GEN-SIM", "GEN", "SIM", "AOD"]
        validRSEs = ["rse_1", "rse_2", "rse_3"]
        tierPolicy = [{"datatier": "GEN", "destinations": ["rse_1"]},
                      {"datatier": "default", "destinations": ["rse_2"]}]

        # test output lifetime policy with a wrong data type
        for testPolicy in [None, {}, "blah", 123]:
            with self.assertRaises(RelValPolicyException):
                RelValPolicy(tierPolicy, testPolicy, validDatatiers, validRSEs)

        # test internal structure with wrong data type for releaseType
        pol1 = [{"releaseType": 123, "lifetimeSecs": 1}]
        pol2 = [{"releaseType": None, "lifetimeSecs": 1}]
        pol3 = [{"releaseType": ["pre"], "lifetimeSecs": 1}, {"releaseType": "final", "lifetimeSecs": 2}]
        for lifePol in (pol1, pol2, pol3):
            with self.assertRaises(RelValPolicyException):
                RelValPolicy(tierPolicy, lifePol, validDatatiers, validRSEs)

        # test internal structure with wrong data type for lifetimeSecs
        pol1 = [{"releaseType": "pre", "lifetimeSecs": 1.5}]
        pol2 = [{"releaseType": "default", "lifetimeSecs": "1"}]
        pol3 = [{"releaseType": "pre", "lifetimeSecs": None}, {"releaseType": "final", "lifetimeSecs": 2}]
        pol4 = [{"releaseType": "pre", "lifetimeSecs": 0}, {"releaseType": "final", "lifetimeSecs": -10}]
        for lifePol in (pol1, pol2, pol3, pol4):
            with self.assertRaises(RelValPolicyException):
                RelValPolicy(tierPolicy, lifePol, validDatatiers, validRSEs)

        # test missing release type
        pol1 = [{"releaseType": "pre", "lifetimeSecs": 1}]
        pol2 = [{"releaseType": "default", "lifetimeSecs": 1}]
        pol3 = [{"releaseType": "pre", "lifetimeSecs": 1}, {"releaseType": "final", "lifetimeSecs": 2}]
        for lifePol in (pol1, pol2, pol3):
            with self.assertRaises(RelValPolicyException):
                RelValPolicy(tierPolicy, lifePol, validDatatiers, validRSEs)

    def testValidPolicy(self):
        """Tests for the RelValPolicy class with a valid policy"""
        validDatatiers = ["GEN-SIM", "GEN", "SIM", "AOD"]
        validRSEs = ["rse_1", "rse_2", "rse_3"]

        tierPolicy = [{"datatier": "SIM", "destinations": ["rse_1", "rse_2"]},
                      {"datatier": "GEN-SIM", "destinations": ["rse_1"]},
                      {"datatier": "default", "destinations": ["rse_2"]}]
        lifePolicy = [{"releaseType": "pre", "lifetimeSecs": 1},
                      {"releaseType": "default", "lifetimeSecs": 2}]

        policyObj = RelValPolicy(tierPolicy, lifePolicy, validDatatiers, validRSEs)

        # now test the method to get destinations for a given dataset (datatier)
        for policyItem in tierPolicy:
            resp = policyObj.getDestinationByDataset("/PD/ProcStr-v1/{}".format(policyItem['datatier']))
            self.assertEqual(resp, policyItem['destinations'])

        # and this should fallback to the 'default' case
        resp = policyObj.getDestinationByDataset("/PD/ProcStr-v1/BLAH")
        self.assertEqual(resp, ["rse_2"])

        # now test the release type lifetime policy using the default value
        for dsetName in {"/PD/CMSSW_1_2_3_patch1-ProcStr-v1/TIER",
                         "/PD/CMSSW_1_2_3_HLT1-ProcStr-v1/TIER",
                         "/PD/CMSSW_1_2_3-ProcStr-v1/TIER"}:
            self.assertEqual(policyObj.getLifetimeByDataset(dsetName), 2)
        # and a pre-release
        self.assertEqual(policyObj.getLifetimeByDataset("/PD/CMSSW_1_2_3_pre4-ProcStr-v1/TIER"), 1)

    def testStringification(self):
        """Test the stringification of the RelValPolicy object"""
        validDatatiers = []
        validRSEs = ["rse_2"]
        testPolicy = [{"datatier": "default", "destinations": ["rse_2"]}]
        lifePolicy = [{"releaseType": "pre", "lifetimeSecs": 3},
                      {"releaseType": "default", "lifetimeSecs": 12}]

        policyObj = str(RelValPolicy(testPolicy, lifePolicy, validDatatiers, validRSEs))

        self.assertTrue(isinstance(policyObj, str))
        policyObj = json.loads(policyObj)
        self.assertCountEqual(policyObj["originalTierPolicy"], testPolicy)
        self.assertCountEqual(policyObj["mappedTierPolicy"], {"default": ["rse_2"]})

        self.assertCountEqual(policyObj["originalLifetimePolicy"], lifePolicy)
        self.assertCountEqual(policyObj["mappedLifetimePolicy"], {"pre": 3, "default": 12})

    def testIsPreRelease(self):
        """Test the _isPreRelease method"""
        tierPolicy = [{"datatier": "SIM", "destinations": ["rse_1"]},
                      {"datatier": "default", "destinations": ["rse_1"]}]
        lifePolicy = [{"releaseType": "pre", "lifetimeSecs": 3},
                      {"releaseType": "default", "lifetimeSecs": 12}]

        policyObj = RelValPolicy(tierPolicy, lifePolicy, ["SIM"], ["rse_1"])

        # first test a broken dataset name
        for dsetName in ("", None), 123:
            with self.assertRaises(RuntimeError):
                policyObj._isPreRelease(dsetName)

        for dsetName in {"/PD/CMSSW_1_2_3_patch1-ProcStr-v1/TIER",
                         "/PD/CMSSW_1_2_3_HLT1-ProcStr-v1/TIER",
                         "/PD/CMSSW_1_2_3-ProcStr-v1/TIER",
                         "/PD/AcqEra-ProcStr-v1/TIER"}:
            self.assertFalse(policyObj._isPreRelease(dsetName))
        self.assertTrue(policyObj._isPreRelease("/PD/CMSSW_1_2_3_pre4-ProcStr-v1/TIER"))


if __name__ == '__main__':
    unittest.main()
