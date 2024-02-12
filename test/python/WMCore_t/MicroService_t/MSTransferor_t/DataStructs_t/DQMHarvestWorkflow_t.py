"""
Unit tests for the WMCore/MicroService/DataStructs/DQMHarvestWorkflow module
"""
from __future__ import division, print_function

import unittest
from copy import deepcopy

from WMCore.MicroService.MSTransferor.DataStructs.DQMHarvestWorkflow import DQMHarvestWorkflow


class DQMHarvestWorkflowTest(unittest.TestCase):
    """
    Test the very basic functionality of the DQMHarvestWorkflow module
    """

    def setUp(self):
        """
        Defined some basic data structs to use in the unit tests
        """
        self.primaryDict = {"block_A": {"blockSize": 1, "locations": ["Site_A", "Site_B"]},
                            "block_B": {"blockSize": 2, "locations": ["Site_B", "Site_C"]}}
        self.dqmSpec = {"RequestType": "ReReco",
                        "InputDataset": "/rereco/input-dataset/tier",
                        "Campaign": "any-campaign",
                        "RequestName": "whatever_name",
                        "DbsUrl": "a_dbs_url",
                        "SiteWhitelist": ["Site_A", "Site_B", "Site_C"],
                        "SiteBlacklist": ["Site_B"]}

    def tearDown(self):
        pass

    def testInstance(self):
        """
        Test object instance type
        """
        wflow = DQMHarvestWorkflow(self.dqmSpec['RequestName'], deepcopy(self.dqmSpec))
        self.assertIsInstance(wflow, DQMHarvestWorkflow)

    def testDQMHarvestWflow(self):
        """
        Test loading a DQMHarvest like request into Workflow object
        """
        wflow = DQMHarvestWorkflow(self.dqmSpec['RequestName'], deepcopy(self.dqmSpec))
        self.assertEqual(wflow.getName(), self.dqmSpec['RequestName'])
        self.assertEqual(wflow.getDbsUrl(), self.dqmSpec['DbsUrl'])
        self.assertCountEqual(wflow.getSitelist(), ["Site_A", "Site_C"])
        self.assertCountEqual(wflow.getCampaigns(), [self.dqmSpec["Campaign"]])
        self.assertEqual(wflow.getInputDataset(), self.dqmSpec["InputDataset"])
        self.assertCountEqual(wflow.getPileupDatasets(), set())
        self.assertFalse(wflow.hasParents())
        self.assertEqual(wflow.getParentDataset(), "")
        self.assertEqual(wflow.getPrimaryBlocks(), {})
        self.assertEqual(wflow.getSecondarySummary(), {})
        self.assertEqual(wflow.getParentBlocks(), {})
        self.assertEqual(wflow._getValue("NoKey"), None)
        self.assertEqual(len(wflow.getDataCampaignMap()), 1)

    def testGetInputData(self):
        """
        Test the `getInputData` method for this template, which
        is supposed to return a list of input blocks
        """
        wflow = DQMHarvestWorkflow(self.dqmSpec['RequestName'], deepcopy(self.dqmSpec))
        wflow.setPrimaryBlocks(self.primaryDict)
        inputBlocks, blockSize = wflow.getInputData()
        # this MSTransferor policy deals with the whole container
        self.assertEqual(inputBlocks, [self.dqmSpec["InputDataset"]])
        self.assertEqual(blockSize, 3)

    def testGetRucioGrouping(self):
        """
        Test the `getRucioGrouping` method, which is supposed to return
        a basic string with the Rucio grouping for this template (static
        output).
        """
        wflow = DQMHarvestWorkflow(self.dqmSpec['RequestName'], deepcopy(self.dqmSpec))
        self.assertEqual(wflow.getRucioGrouping(), "ALL")


if __name__ == '__main__':
    unittest.main()
