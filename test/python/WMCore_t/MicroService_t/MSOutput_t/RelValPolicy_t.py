"""
Unit tests for the WMCore/MicroService/MSOutput/RelValPolicy.py module
"""
from __future__ import division, print_function

import unittest
import json
from WMCore.MicroService.MSOutput.RelValPolicy import RelValPolicy, RelValPolicyException


class RelValPolicyTests(unittest.TestCase):
    """Unit tests for the RelValPolicy module"""

    def setUp(self):
        """Basic setup for each unit test"""
        pass

    def tearDown(self):
        """Basic tear down operation when leaving each unit test"""
        pass

    def testBrokenPolicy(self):
        """Tests for the RelValPolicy class with a broken policy"""
        validDatatiers = ["GEN-SIM", "GEN", "SIM", "AOD"]
        validRSEs = ["rse_1", "rse_2", "rse_3"]

        # test output policies with a wrong data type
        for testPolicy in [None, {}, "blah", 123]:
            with self.assertRaises(RelValPolicyException):
                RelValPolicy(testPolicy, validDatatiers, validRSEs)

        # test internal structure with wrong data type for datatier
        testPolicy = [{"datatier": ["tier1", "tier2"], "destinations": ["rse_1", "rse_2"]},
                      {"datatier": "default", "destinations": ["rse_1"]}]
        with self.assertRaises(RelValPolicyException):
            RelValPolicy(testPolicy, validDatatiers, validRSEs)

        # test internal structure with wrong data type for destinations
        testPolicy = [{"datatier": "tier1", "destinations": "rse_1"},
                      {"datatier": "default", "destinations": ["rse_1"]}]
        with self.assertRaises(RelValPolicyException):
            RelValPolicy(testPolicy, validDatatiers, validRSEs)

        # test internal structure missing the required "default" key/value pair
        testPolicy = [{"datatier": "GEN", "destinations": ["rse_1", "rse_2"]}]
        with self.assertRaises(RelValPolicyException):
            RelValPolicy(testPolicy, validDatatiers, validRSEs)

    def testValidPolicy(self):
        """Tests for the RelValPolicy class with a valid policy"""
        validDatatiers = ["GEN-SIM", "GEN", "SIM", "AOD"]
        validRSEs = ["rse_1", "rse_2", "rse_3"]

        testPolicy = [{"datatier": "SIM", "destinations": ["rse_1", "rse_2"]},
                      {"datatier": "GEN-SIM", "destinations": ["rse_1"]},
                      {"datatier": "default", "destinations": ["rse_2"]}]
        policyObj = RelValPolicy(testPolicy, validDatatiers, validRSEs)

        # now test the method to get destinations for a given dataset (datatier)
        for policyItem in testPolicy:
            resp = policyObj.getDestinationByDataset("/PD/ProcStr-v1/{}".format(policyItem['datatier']))
            self.assertEqual(resp, policyItem['destinations'])

        # and this should fallback to the 'default' case
        resp = policyObj.getDestinationByDataset("/PD/ProcStr-v1/BLAH")
        self.assertEqual(resp, ["rse_2"])

    def testStringification(self):
        """Test the stringification of the RelValPolicy object"""
        validDatatiers = []
        validRSEs = ["rse_2"]
        testPolicy = [{"datatier": "default", "destinations": ["rse_2"]}]

        policyObj = str(RelValPolicy(testPolicy, validDatatiers, validRSEs))

        self.assertTrue(isinstance(policyObj, str))
        policyObj = json.loads(policyObj)
        self.assertCountEqual(policyObj["originalPolicy"], testPolicy)
        self.assertCountEqual(policyObj["mappedPolicy"], {"default": ["rse_2"]})


if __name__ == '__main__':
    unittest.main()
