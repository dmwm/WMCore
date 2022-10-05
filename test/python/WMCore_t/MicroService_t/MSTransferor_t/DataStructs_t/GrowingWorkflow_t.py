"""
Unit tests for the WMCore/MicroService/DataStructs/GrowingWorkflow module
"""
from __future__ import division, print_function

import unittest
from copy import deepcopy

from WMCore.MicroService.MSTransferor.DataStructs.GrowingWorkflow import GrowingWorkflow


class GrowingWorkflowTest(unittest.TestCase):
    """
    Test the very basic functionality of the GrowingWorkflow module
    """

    def setUp(self):
        """
        Defined some basic data structs to use in the unit tests
        :return: None
        """
        self.primaryDict = {"block_A": {"blockSize": 1, "locations": ["Site_A", "Site_B"]},
                            "block_B": {"blockSize": 2, "locations": ["Site_B", "Site_C"]}}
        self.taskSpec = {"RequestType": "TaskChain",
                         "SubRequestType": "ReReco",
                         "TaskChain": 2,
                         "Campaign": "top-campaign",
                         "RequestName": "whatever_name",
                         "DbsUrl": "a_dbs_url",
                         "SiteWhitelist": ["Site_A", "Site_B", "Site_C"],
                         "SiteBlacklist": [],
                         "Task1": {"InputDataset": "/task1/input-dataset/tier",
                                   "Campaign": "task1-campaign"},
                         "Task2": {"Campaign": "task2-campaign"},
                         }
        self.rerecoSpec = {"RequestType": "ReReco",
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
        wflow = GrowingWorkflow(self.taskSpec['RequestName'], deepcopy(self.taskSpec))
        self.assertIsInstance(wflow, GrowingWorkflow)

    def testGrowingTaskChainWflow(self):
        """
        Test loading a growing TaskChain workflow
        """
        expCampaigns = {'task2-campaign', 'task1-campaign'}

        wflow = GrowingWorkflow(self.taskSpec['RequestName'], self.taskSpec)
        self.assertEqual(wflow.getName(), self.taskSpec['RequestName'])
        self.assertEqual(wflow.getDbsUrl(), self.taskSpec['DbsUrl'])
        self.assertCountEqual(wflow.getSitelist(), self.taskSpec["SiteWhitelist"])
        self.assertCountEqual(wflow.getCampaigns(), expCampaigns)
        self.assertEqual(wflow.getInputDataset(), self.taskSpec["Task1"].get("InputDataset", ""))
        self.assertCountEqual(wflow.getPileupDatasets(), [])
        self.assertFalse(wflow.hasParents())
        self.assertEqual(wflow.getParentDataset(), "")
        self.assertEqual(wflow.getPrimaryBlocks(), {})
        self.assertEqual(wflow.getSecondarySummary(), {})
        self.assertEqual(wflow.getParentBlocks(), {})
        self.assertEqual(wflow._getValue("NoKey"), None)

    def testGrowingReRecoWflow(self):
        """
        Test loading a growing ReReco workflow
        """
        wflow = GrowingWorkflow(self.rerecoSpec['RequestName'], self.rerecoSpec)
        self.assertEqual(wflow.getName(), self.rerecoSpec['RequestName'])
        self.assertEqual(wflow.getDbsUrl(), self.rerecoSpec['DbsUrl'])
        self.assertCountEqual(wflow.getSitelist(), ["Site_A", "Site_C"])
        self.assertCountEqual(wflow.getCampaigns(), [self.rerecoSpec["Campaign"]])
        self.assertEqual(wflow.getInputDataset(), self.rerecoSpec.get("InputDataset", ""))
        self.assertCountEqual(wflow.getPileupDatasets(), [])
        self.assertFalse(wflow.hasParents())
        self.assertEqual(wflow.getParentDataset(), "")
        self.assertEqual(wflow.getPrimaryBlocks(), {})
        self.assertEqual(wflow.getSecondarySummary(), {})
        self.assertEqual(wflow.getParentBlocks(), {})
        self.assertEqual(wflow._getValue("NoKey"), None)

    def testGetInputData(self):
        """
        Test the `getInputData` method for this template, which
        is supposed to return a list with the input dataset name
        """
        wflow = GrowingWorkflow(self.rerecoSpec['RequestName'], deepcopy(self.rerecoSpec))
        wflow.setPrimaryBlocks(self.primaryDict)
        inputBlocks, blockSize = wflow.getInputData()
        print(inputBlocks)
        self.assertEqual(len(inputBlocks), 1)
        # note that it returns the input container name!
        self.assertCountEqual(inputBlocks, [self.rerecoSpec['InputDataset']])
        self.assertEqual(blockSize, 3)

    def testGetRucioGrouping(self):
        """
        Test the `getRucioGrouping` method, which is supposed to return
        a basic string with the Rucio grouping for this template (static
        output).
        """
        wflow = GrowingWorkflow(self.rerecoSpec['RequestName'], deepcopy(self.rerecoSpec))
        self.assertEqual(wflow.getRucioGrouping(), "DATASET")

    def testGetReplicaCopies(self):
        """
        Test the `getReplicaCopies` method, which is supposed to return
        an integer with the number of copies that a rule has to request
        """
        wflow = GrowingWorkflow(self.rerecoSpec['RequestName'], deepcopy(self.rerecoSpec))
        self.assertEqual(wflow.getReplicaCopies(), 1)


if __name__ == '__main__':
    unittest.main()
