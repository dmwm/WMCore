"""
Unit tests for the WMCore/MicroService/DataStructs/RelValWorkflow module
"""
from __future__ import division, print_function

import unittest
from copy import deepcopy

from WMCore.MicroService.MSTransferor.DataStructs.RelValWorkflow import RelValWorkflow


class RelValWorkflowTest(unittest.TestCase):
    """
    Test the very basic functionality of the RelValWorkflow module
    """

    def setUp(self):
        """
        Defined some basic data structs to use in the unit tests
        :return: None
        """
        self.primaryDict = {"block_A": {"blockSize": 1, "locations": ["Site_A", "Site_B"]},
                            "block_B": {"blockSize": 2, "locations": ["Site_B", "Site_C"]}}
        self.parentDict = {"parent_A": {"blockSize": 11, "locations": ["Site_A"]},
                           "parent_B": {"blockSize": 12, "locations": ["Site_B"]}}
        self.relvalSpec = {"RequestType": "TaskChain",
                           "SubRequestType": "RelVal",
                           "TaskChain": 3,
                           "Campaign": "top-campaign",
                           "RequestName": "whatever_name",
                           "DbsUrl": "a_dbs_url",
                           "SiteWhitelist": ["Site_A", "Site_B", "Site_C"],
                           "SiteBlacklist": [],
                           "Task1": {"Campaign": "task1-campaign"},
                           "Task2": {"Campaign": "task2-campaign"},
                           "Task3": {"Campaign": "task3-campaign"},
                           }
        self.relvalNoInputPU = {"Task1": {"Campaign": "task1-campaign"},
                                "Task2": {"MCPileup": "/task1/mc-pileup/tier",
                                          "Campaign": "task2-campaign"},
                                "Task3": {"Campaign": "task3-campaign"}}
        self.relvalInputNoPU = {"Task1": {"InputDataset": "/task1/input-dataset/tier",
                                          "Campaign": "task1-campaign"},
                                "Task2": {"Campaign": "task2-campaign"},
                                "Task3": {"Campaign": "task3-campaign"}}
        self.relvalInputPU = {"Task1": {"InputDataset": "/task1/input-dataset/tier",
                                        "Campaign": "task1-campaign"},
                              "Task2": {"MCPileup": "/task2/mc-pileup/tier",
                                        "Campaign": "task2-campaign"},
                              "Task3": {"Campaign": "task3-campaign"}}
        self.relvalInputDualPU = {"Task1": {"InputDataset": "/task1/input-dataset/tier",
                                            "Campaign": "task1-campaign"},
                                  "Task2": {"MCPileup": "/task2/mc-pileup/tier",
                                            "Campaign": "task2-campaign"},
                                  "Task3": {"MCPileup": "/task3/mc-pileup/tier",
                                            "Campaign": "task3-campaign"}}

    def tearDown(self):
        pass

    def testInstance(self):
        """
        Test object instance type
        """
        wflow = RelValWorkflow(self.relvalSpec['RequestName'], deepcopy(self.relvalSpec))
        self.assertIsInstance(wflow, RelValWorkflow)

    def testRelValWflowNoInputNoPU(self):
        """
        Test loading a RelVal like request without any input and pileup
        """
        expCampaigns = {'task2-campaign', 'task1-campaign', 'task3-campaign'}
        wflow = RelValWorkflow(self.relvalSpec['RequestName'], deepcopy(self.relvalSpec))
        self.assertEqual(wflow.getName(), self.relvalSpec['RequestName'])
        self.assertEqual(wflow.getDbsUrl(), self.relvalSpec['DbsUrl'])
        self.assertCountEqual(wflow.getSitelist(), self.relvalSpec["SiteWhitelist"])
        self.assertCountEqual(wflow.getCampaigns(), expCampaigns)
        self.assertEqual(wflow.getInputDataset(), self.relvalSpec["Task1"].get("InputDataset", ""))
        self.assertCountEqual(wflow.getPileupDatasets(), set())
        self.assertFalse(wflow.hasParents())
        self.assertEqual(wflow.getParentDataset(), "")
        self.assertEqual(wflow.getPrimaryBlocks(), {})
        self.assertEqual(wflow.getSecondarySummary(), {})
        self.assertEqual(wflow.getParentBlocks(), {})
        self.assertEqual(wflow._getValue("NoKey"), None)

    def testRelValWflowNoInputWithPU(self):
        """
        Test loading a RelVal like request without input but with pileup
        """
        expCampaigns = {'task2-campaign', 'task1-campaign', 'task3-campaign'}
        specDict = deepcopy(self.relvalSpec)
        specDict.update(deepcopy(self.relvalNoInputPU))

        wflow = RelValWorkflow(self.relvalSpec['RequestName'], specDict)
        self.assertEqual(wflow.getName(), specDict['RequestName'])
        self.assertEqual(wflow.getDbsUrl(), specDict['DbsUrl'])
        self.assertCountEqual(wflow.getSitelist(), specDict["SiteWhitelist"])
        self.assertCountEqual(wflow.getCampaigns(), expCampaigns)
        self.assertEqual(wflow.getInputDataset(), specDict["Task1"].get("InputDataset", ""))
        self.assertCountEqual(wflow.getPileupDatasets(), [specDict["Task2"].get("MCPileup", "")])
        self.assertFalse(wflow.hasParents())
        self.assertEqual(wflow.getParentDataset(), "")
        self.assertEqual(wflow.getPrimaryBlocks(), {})
        self.assertEqual(wflow.getSecondarySummary(), {})
        self.assertEqual(wflow.getParentBlocks(), {})
        self.assertEqual(wflow._getValue("NoKey"), None)

    def testRelValWflowInputNoPU(self):
        """
        Test loading a RelVal like request with input but no pileup
        """
        expCampaigns = {'task2-campaign', 'task1-campaign', 'task3-campaign'}
        specDict = deepcopy(self.relvalSpec)
        specDict.update(deepcopy(self.relvalInputNoPU))

        wflow = RelValWorkflow(self.relvalSpec['RequestName'], specDict)
        self.assertEqual(wflow.getName(), specDict['RequestName'])
        self.assertEqual(wflow.getDbsUrl(), specDict['DbsUrl'])
        self.assertCountEqual(wflow.getSitelist(), specDict["SiteWhitelist"])
        self.assertCountEqual(wflow.getCampaigns(), expCampaigns)
        self.assertEqual(wflow.getInputDataset(), specDict["Task1"].get("InputDataset", ""))
        self.assertCountEqual(wflow.getPileupDatasets(), [])
        self.assertFalse(wflow.hasParents())
        self.assertEqual(wflow.getParentDataset(), "")
        self.assertEqual(wflow.getPrimaryBlocks(), {})
        self.assertEqual(wflow.getSecondarySummary(), {})
        self.assertEqual(wflow.getParentBlocks(), {})
        self.assertEqual(wflow._getValue("NoKey"), None)

    def testRelValWflowInputPU(self):
        """
        Test loading a RelVal like request with input and pileup
        """
        expCampaigns = {'task2-campaign', 'task1-campaign', 'task3-campaign'}
        specDict = deepcopy(self.relvalSpec)
        specDict.update(deepcopy(self.relvalInputPU))
        puName = specDict["Task2"].get("MCPileup", "")

        wflow = RelValWorkflow(self.relvalSpec['RequestName'], specDict)
        self.assertEqual(wflow.getName(), specDict['RequestName'])
        self.assertEqual(wflow.getDbsUrl(), specDict['DbsUrl'])
        self.assertCountEqual(wflow.getSitelist(), specDict["SiteWhitelist"])
        self.assertCountEqual(wflow.getCampaigns(), expCampaigns)
        self.assertEqual(wflow.getInputDataset(), specDict["Task1"].get("InputDataset", ""))
        self.assertCountEqual(wflow.getPileupDatasets(), [puName])
        self.assertFalse(wflow.hasParents())
        self.assertEqual(wflow.getParentDataset(), "")
        self.assertEqual(wflow.getParentBlocks(), {})
        self.assertEqual(wflow._getValue("NoKey"), None)
        self.assertEqual(wflow.getPrimaryBlocks(), {})

        # checking pileup data
        self.assertEqual(wflow.getSecondarySummary(), {})
        wflow.setSecondarySummary(puName, locations=["Site_B"])
        self.assertCountEqual(list(wflow.getSecondarySummary()), [puName])
        self.assertCountEqual(wflow.getSecondarySummary()[puName]["locations"], ["Site_B"])

    def testRelValWflowWithParent(self):
        """
        Test loading a RelVal like request with input and parent data
        """
        expCampaigns = {'task2-campaign', 'task1-campaign', 'task3-campaign'}
        specDict = deepcopy(self.relvalSpec)
        specDict.update(deepcopy(self.relvalInputNoPU))
        specDict["Task1"].update(dict(IncludeParents=True))

        wflow = RelValWorkflow(self.relvalSpec['RequestName'], specDict)
        self.assertEqual(wflow.getName(), specDict['RequestName'])
        self.assertEqual(wflow.getDbsUrl(), specDict['DbsUrl'])
        self.assertCountEqual(wflow.getSitelist(), specDict["SiteWhitelist"])
        self.assertCountEqual(wflow.getCampaigns(), expCampaigns)
        self.assertEqual(wflow.getInputDataset(), specDict["Task1"].get("InputDataset", ""))
        self.assertCountEqual(wflow.getPileupDatasets(), [])
        self.assertTrue(wflow.hasParents())
        self.assertEqual(wflow.getParentDataset(), "")
        self.assertEqual(wflow.getParentBlocks(), {})
        self.assertEqual(wflow._getValue("NoKey"), None)
        self.assertEqual(wflow.getPrimaryBlocks(), {})

        wflow.setParentBlocks(self.parentDict)
        wflow.setPrimaryBlocks(self.primaryDict)
        self.assertCountEqual(list(wflow.getPrimaryBlocks()), ["block_A", "block_B"])
        self.assertCountEqual(list(wflow.getParentBlocks()), ["parent_A", "parent_B"])

    def testRelValWflowDualPU(self):
        """
        Test loading a RelVal like request with input and two pileups
        """
        expCampaigns = {'task2-campaign', 'task1-campaign', 'task3-campaign'}
        specDict = deepcopy(self.relvalSpec)
        specDict.update(deepcopy(self.relvalInputDualPU))
        puName1 = specDict["Task2"].get("MCPileup", "")
        puName2 = specDict["Task3"].get("MCPileup", "")

        wflow = RelValWorkflow(self.relvalSpec['RequestName'], specDict)
        self.assertEqual(wflow.getName(), specDict['RequestName'])
        self.assertEqual(wflow.getDbsUrl(), specDict['DbsUrl'])
        self.assertCountEqual(wflow.getSitelist(), specDict["SiteWhitelist"])
        self.assertCountEqual(wflow.getCampaigns(), expCampaigns)
        self.assertEqual(wflow.getInputDataset(), specDict["Task1"].get("InputDataset", ""))
        self.assertCountEqual(wflow.getPileupDatasets(), [puName1, puName2])
        self.assertFalse(wflow.hasParents())
        self.assertEqual(wflow.getParentDataset(), "")
        self.assertEqual(wflow.getParentBlocks(), {})
        self.assertEqual(wflow._getValue("NoKey"), None)
        self.assertEqual(wflow.getPrimaryBlocks(), {})

        # checking pileup data
        self.assertEqual(wflow.getSecondarySummary(), {})
        wflow.setSecondarySummary(puName1, locations=["Site_B"])
        self.assertCountEqual(list(wflow.getSecondarySummary()), [puName1])
        self.assertCountEqual(wflow.getSecondarySummary()[puName1]["locations"], ["Site_B"])

        # now set the second pileup
        wflow.setSecondarySummary(puName2, locations=["Site_C"])
        self.assertCountEqual(list(wflow.getSecondarySummary()), [puName1, puName2])
        self.assertCountEqual(wflow.getSecondarySummary()[puName1]["locations"], ["Site_B"])
        self.assertCountEqual(wflow.getSecondarySummary()[puName2]["locations"], ["Site_C"])

    def testGetInputData(self):
        """
        Test the `getInputData` method for this template, which
        is supposed to return a list of input blocks
        """
        wflow = RelValWorkflow(self.relvalSpec['RequestName'], deepcopy(self.relvalSpec))
        wflow.setPrimaryBlocks(self.primaryDict)
        inputBlocks, blockSize = wflow.getInputData()
        self.assertEqual(len(inputBlocks), 2)
        self.assertCountEqual(inputBlocks, list(self.primaryDict))
        self.assertEqual(blockSize, 3)

    def testGetInputDataParent(self):
        """
        Test the `getInputData` method for this template, which
        is supposed to return a list of input blocks
        """
        specDict = deepcopy(self.relvalSpec)
        specDict.update(deepcopy(self.relvalInputNoPU))
        specDict["Task1"].update(dict(IncludeParents=True))
        wflow = RelValWorkflow(specDict['RequestName'], deepcopy(specDict))

        wflow.setPrimaryBlocks(self.primaryDict)
        wflow.setParentDataset("parent_dset")
        wflow.setParentBlocks(self.parentDict)

        inputBlocks, blockSize = wflow.getInputData()
        self.assertEqual(len(inputBlocks), 4)
        self.assertCountEqual(inputBlocks, list(self.primaryDict) + list(self.parentDict))
        self.assertEqual(blockSize, 26)

    def testGetRucioGrouping(self):
        """
        Test the `getRucioGrouping` method, which is supposed to return
        a basic string with the Rucio grouping for this template (static
        output).
        """
        wflow = RelValWorkflow(self.relvalSpec['RequestName'], deepcopy(self.relvalSpec))
        self.assertEqual(wflow.getRucioGrouping(), "ALL")


if __name__ == '__main__':
    unittest.main()
