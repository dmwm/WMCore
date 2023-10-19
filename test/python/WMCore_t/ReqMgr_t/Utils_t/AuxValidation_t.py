#!/usr/bin/env python
"""
Unittests for IteratorTools functions
"""
import unittest

from WMCore.ReqMgr.Utils.AuxValidation import validateUnifiedConfig
from WMCore.REST.Error import InvalidUnifiedSchema


class AuxValidationTests(unittest.TestCase):
    """
    Unittests for ReqMgr Utils Validation functions
    """

    def test1ValidateUnifiedConfig(self):
        """
        Test the `validateUnifiedConfig` function with wrong input data
        """
        # bad data type
        for wrongData in ["", [], 123, {}]:
            with self.assertRaises(RuntimeError):
                validateUnifiedConfig(wrongData)

        # missing top level parameters
        userD = {"tiers_to_DDM": {"value": ["LHE"], "description": "tiers to DDM"}}
        with self.assertRaises(RuntimeError):
            validateUnifiedConfig(userD)
        userD = {"tiers_to_DDM": {"value": ["LHE"], "description": "tiers to DDM"},
                 "tiers_no_DDM": {"value": ["GEN-SIM"], "description": "tiers no DDM"}}
        with self.assertRaises(RuntimeError):
            validateUnifiedConfig(userD)
        userD = {"tiers_to_DDM": {"value": ["LHE"], "description": "tiers to DDM"},
                 "tiers_with_no_custodial": {"value": ["DQM"], "description": "tiers no custodial"}}
        with self.assertRaises(RuntimeError):
            validateUnifiedConfig(userD)

        # and now with 1 unsupported parameter
        userD = {"tiers_to_DDM": {"value": ["LHE"], "description": "tiers to DDM"},
                 "tiers_no_DDM": {"value": ["GEN-SIM"], "description": "tiers no DDM"},
                 "tiers_with_no_custodial": {"value": ["DQM"], "description": "tiers no custodial"},
                 "BAD_KEY": {"value": ["LHE"], "description": "unsupported parameter"}
                 }
        with self.assertRaises(RuntimeError):
            validateUnifiedConfig(userD)

    def test2ValidateUnifiedConfig(self):
        """
        Test the `validateUnifiedConfig` function with wrong
        nested schema, but also with the correct and expected schema.
        """
        # missing nested keys
        userD = {"tiers_to_DDM": {},
                 "tiers_no_DDM": {},
                 "tiers_with_no_custodial": {}}
        with self.assertRaises(RuntimeError):
            validateUnifiedConfig(userD)
        userD = {"tiers_to_DDM": {"value": ["LHE"]},
                 "tiers_no_DDM": {"value": ["GEN-SIM"]},
                 "tiers_with_no_custodial": {"value": ["DQM"]}}
        with self.assertRaises(RuntimeError):
            validateUnifiedConfig(userD)
        userD = {"tiers_to_DDM": {"description": "tiers to DDM"},
                 "tiers_no_DDM": {"description": "tiers no DDM"},
                 "tiers_with_no_custodial": {"description": "tiers no custodial"}}
        with self.assertRaises(RuntimeError):
            validateUnifiedConfig(userD)

        # now with too many nested keys
        userD = {"tiers_to_DDM": {"value": ["LHE"], "description": "tiers to DDM", "BAD_KEY": "blah"},
                 "tiers_no_DDM": {"value": ["GEN-SIM"], "description": "tiers no DDM"},
                 "tiers_with_no_custodial": {"value": ["DQM"], "description": "tiers no custodial"}}
        with self.assertRaises(RuntimeError):
            validateUnifiedConfig(userD)

        # last but not least, a good looking and supported schema
        userD = {"tiers_to_DDM": {"value": ["LHE"], "description": "tiers to DDM"},
                 "tiers_no_DDM": {"value": ["GEN-SIM"], "description": "tiers no DDM"},
                 "tiers_with_no_custodial": {"value": ["DQM"], "description": "tiers no custodial"}}
        validateUnifiedConfig(userD)

    def testInvalidUnifiedSchemaException(self):
        """
        Test the `InvalidUnifiedSchema` exception
        """
        errorMsg = ""
        try:
            try:
                validateUnifiedConfig([])
            except Exception as exc:
                raise InvalidUnifiedSchema(str(exc)) from None
        except InvalidUnifiedSchema as exc2:
            errorMsg = str(exc2)
        self.assertTrue("Unified schema error." in errorMsg)


if __name__ == '__main__':
    unittest.main()
