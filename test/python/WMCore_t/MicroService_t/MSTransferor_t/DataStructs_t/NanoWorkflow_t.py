"""
Unit tests for the WMCore/MicroService/DataStructs/NanoWorkflow module
"""
import unittest
from copy import deepcopy

from WMCore.MicroService.MSTransferor.DataStructs.NanoWorkflow import NanoWorkflow


class NanoWorkflowTest(unittest.TestCase):
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
                         "Task1": {"InputDataset": "/task1/input-dataset/MINIAODSIM",
                                   "Campaign": "task1-campaign"},
                         "Task2": {"Campaign": "task2-campaign"},
                         }

    def tearDown(self):
        pass

    def testInstance(self):
        """
        Test object instance type
        """
        wflow = NanoWorkflow(self.taskSpec['RequestName'], deepcopy(self.taskSpec))
        self.assertIsInstance(wflow, NanoWorkflow)

    def testNanoWflow(self):
        """
        Test loading a growing TaskChain workflow
        """
        expCampaigns = {'task2-campaign', 'task1-campaign'}

        wflow = NanoWorkflow(self.taskSpec['RequestName'], self.taskSpec)
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

    def testGetInputData(self):
        """
        Test the `getInputData` method for this template, which
        is supposed to return a list with the input dataset name
        """
        wflow = NanoWorkflow(self.taskSpec['RequestName'], deepcopy(self.taskSpec))
        wflow.setPrimaryBlocks(self.primaryDict)
        inputBlocks, blockSize = wflow.getInputData()
        self.assertEqual(len(inputBlocks), 1)
        # note that it returns the input container name!
        self.assertCountEqual(inputBlocks, [self.taskSpec['Task1']['InputDataset']])
        self.assertEqual(blockSize, 3)

    def testGetRucioGrouping(self):
        """
        Test the `getRucioGrouping` method, which is supposed to return
        a basic string with the Rucio grouping for this template (static
        output).
        """
        wflow = NanoWorkflow(self.taskSpec['RequestName'], deepcopy(self.taskSpec))
        self.assertEqual(wflow.getRucioGrouping(), "ALL")

    def testGetReplicaCopies(self):
        """
        Test the `getReplicaCopies` method, which is supposed to return
        an integer with the number of copies that a rule has to request
        """
        wflow = NanoWorkflow(self.taskSpec['RequestName'], deepcopy(self.taskSpec))
        self.assertEqual(wflow.getReplicaCopies(), 2)


if __name__ == '__main__':
    unittest.main()
