"""
Unit tests for the WMCore/MicroService/DataStructs/Workflow.py module
"""
from __future__ import division, print_function

import unittest

from WMCore.MicroService.DataStructs.Workflow import Workflow


class WorkflowTest(unittest.TestCase):
    """
    Test the very basic functionality of the Workflow module
    """

    def testCampaignMap(self):
        """
        Test setting the data campaign map for a TaskChain-like request
        """
        parentDset = "/any/parent-dataset/tier"
        tChainSpec = {"RequestType": "TaskChain",
                      "TaskChain": 4,
                      "Campaign": "top-campaign",
                      "RequestName": "whatever_name",
                      "Task1": {"InputDataset": "/task1/input-dataset/tier",
                                "Campaign": "task1-campaign",
                                "IncludeParents": True},
                      "Task2": {"DataPileup": "/task2/data-pileup/tier"},
                      "Task3": {"MCPileup": "/task3/mc-pileup/tier",
                                "Campaign": "task3-campaign"},
                      "Task4": {"MCPileup": "/task3/mc-pileup/tier",
                                "Campaign": "task3-campaign"},
                      }
        wflow = Workflow(tChainSpec['RequestName'], tChainSpec)
        self.assertEqual(len(wflow.getDataCampaignMap()), 3)
        for dataIn in wflow.getDataCampaignMap():
            if dataIn['type'] == "primary":
                self.assertItemsEqual(dataIn, {"type": "primary", "campaign": tChainSpec['Task1']['Campaign'],
                                               "name": tChainSpec['Task1']['InputDataset']})
            elif dataIn['name'] == tChainSpec['Task2']['DataPileup']:
                self.assertItemsEqual(dataIn, {"type": "secondary", "campaign": tChainSpec['Campaign'],
                                               "name": tChainSpec['Task2']['DataPileup']})
            else:
                self.assertItemsEqual(dataIn, {"type": "secondary", "campaign": tChainSpec['Task3']['Campaign'],
                                               "name": tChainSpec['Task3']['MCPileup']})

        wflow.setParentDataset(parentDset)
        self.assertEqual(wflow.getParentDataset(), parentDset)
        self.assertEqual(len(wflow.getDataCampaignMap()), 4)
        for dataIn in wflow.getDataCampaignMap():
            if dataIn['type'] == "parent":
                self.assertItemsEqual(dataIn, {"type": "parent", "campaign": tChainSpec['Task1']['Campaign'],
                                               "name": parentDset})

    def testReRecoWflow(self):
        """
        Test loading a ReReco like request into Workflow
        """
        parentDset = "/rereco/parent-dataset/tier"
        rerecoSpec = {"RequestType": "ReReco",
                      "InputDataset": "/rereco/input-dataset/tier",
                      "Campaign": "any-campaign",
                      "RequestName": "whatever_name",
                      "DbsUrl": "a_dbs_url",
                      "SiteWhitelist": ["CERN", "FNAL", "DESY"],
                      "SiteBlacklist": ["FNAL"]}
        wflow = Workflow(rerecoSpec['RequestName'], rerecoSpec)
        self.assertEqual(wflow.getName(), rerecoSpec['RequestName'])
        self.assertEqual(wflow.getDbsUrl(), rerecoSpec['DbsUrl'])
        self.assertItemsEqual(wflow.getSitelist(), ["CERN", "DESY"])
        self.assertItemsEqual(wflow.getCampaigns(), [rerecoSpec["Campaign"]])
        self.assertEqual(wflow.getInputDataset(), rerecoSpec["InputDataset"])
        self.assertItemsEqual(wflow.getPileupDatasets(), set())
        self.assertFalse(wflow.hasParents())
        self.assertEqual(wflow.getParentDataset(), "")
        self.assertEqual(wflow.getPrimaryBlocks(), {})
        self.assertEqual(wflow.getSecondarySummary(), {})
        self.assertEqual(wflow.getParentBlocks(), {})
        self.assertEqual(wflow._getValue("NoKey"), None)
        self.assertEqual(len(wflow.getDataCampaignMap()), 1)

        wflow.setParentDataset(parentDset)
        self.assertEqual(wflow.getParentDataset(), parentDset)
        self.assertEqual(len(wflow.getDataCampaignMap()), 2)

    def testTaskChainWflow(self):
        """
        Test loading a TaskChain like request into Workflow
        """
        tChainSpec = {"RequestType": "TaskChain",
                      "TaskChain": 3,
                      "Campaign": "top-campaign",
                      "RequestName": "whatever_name",
                      "DbsUrl": "a_dbs_url",
                      "SiteWhitelist": ["CERN", "FNAL", "DESY"],
                      "SiteBlacklist": [],
                      "Task1": {"InputDataset": "/task1/input-dataset/tier",
                                "MCPileup": "/task1/mc-pileup/tier",
                                "Campaign": "task1-campaign"},
                      "Task2": {"DataPileup": "/task2/data-pileup/tier",
                                "Campaign": "task2-campaign"},
                      "Task3": {"MCPileup": "/task1/mc-pileup/tier",
                                "Campaign": "task3-campaign"},
                      }
        wflow = Workflow(tChainSpec['RequestName'], tChainSpec)
        self.assertEqual(wflow.getName(), tChainSpec['RequestName'])
        self.assertEqual(wflow.getDbsUrl(), tChainSpec['DbsUrl'])
        self.assertItemsEqual(wflow.getSitelist(), tChainSpec['SiteWhitelist'])
        campaigns = ["%s-campaign" % c for c in {"task1", "task2", "task3"}]
        self.assertItemsEqual(wflow.getCampaigns(), campaigns)
        self.assertEqual(wflow.getInputDataset(), tChainSpec['Task1']['InputDataset'])
        pileups = [tChainSpec['Task1']['MCPileup'], tChainSpec['Task2']['DataPileup']]
        self.assertItemsEqual(wflow.getPileupDatasets(), pileups)
        self.assertFalse(wflow.hasParents())
        self.assertEqual(wflow.getParentDataset(), "")
        self.assertEqual(wflow.getPrimaryBlocks(), {})
        self.assertEqual(wflow.getSecondarySummary(), {})
        self.assertEqual(wflow.getParentBlocks(), {})
        self.assertEqual(wflow._getValue("NoKey"), None)
        self.assertEqual(len(wflow.getDataCampaignMap()), 3)

    def testStepChainWflow(self):
        """
        Test loading a StepChain like request into Workflow
        """
        tChainSpec = {"RequestType": "StepChain",
                      "StepChain": 3,
                      "Campaign": "top-campaign",
                      "RequestName": "whatever_name",
                      "DbsUrl": "a_dbs_url",
                      "SiteWhitelist": ["CERN", "FNAL", "DESY"],
                      "SiteBlacklist": [],
                      "Step1": {"InputDataset": "/step1/input-dataset/tier",
                                "MCPileup": "/step1/mc-pileup/tier",
                                "Campaign": "step1-campaign"},
                      "Step2": {"DataPileup": "/step2/data-pileup/tier",
                                "Campaign": "step2-campaign"},
                      "Step3": {"MCPileup": "/step1/mc-pileup/tier",
                                "Campaign": "step3-campaign"},
                      }
        wflow = Workflow(tChainSpec['RequestName'], tChainSpec)
        self.assertEqual(wflow.getName(), tChainSpec['RequestName'])
        self.assertEqual(wflow.getDbsUrl(), tChainSpec['DbsUrl'])
        self.assertItemsEqual(wflow.getSitelist(), tChainSpec['SiteWhitelist'])
        campaigns = ["%s-campaign" % c for c in {"step1", "step2", "step3"}]
        self.assertItemsEqual(wflow.getCampaigns(), campaigns)
        self.assertEqual(wflow.getInputDataset(), tChainSpec['Step1']['InputDataset'])
        pileups = [tChainSpec['Step1']['MCPileup'], tChainSpec['Step2']['DataPileup']]
        self.assertItemsEqual(wflow.getPileupDatasets(), pileups)
        self.assertFalse(wflow.hasParents())
        self.assertEqual(wflow.getParentDataset(), "")
        self.assertEqual(wflow.getPrimaryBlocks(), {})
        self.assertEqual(wflow.getSecondarySummary(), {})
        self.assertEqual(wflow.getParentBlocks(), {})
        self.assertEqual(wflow._getValue("NoKey"), None)
        self.assertEqual(len(wflow.getDataCampaignMap()), 3)

    def testResubmission(self):
        """
        Test loading a Resubmission like request into Workflow
        """
        rerecoSpec = {"RequestType": "Resubmission",
                      "InputDataset": "/rereco/input-dataset/tier",
                      "Campaign": "any-campaign",
                      "RequestName": "whatever_name",
                      "DbsUrl": "a_dbs_url",
                      "SiteWhitelist": ["CERN", "FNAL", "DESY"],
                      "SiteBlacklist": ["FNAL"]}
        wflow = Workflow(rerecoSpec['RequestName'], rerecoSpec)
        # we do not set any map for Resubmission workflows
        self.assertEqual(wflow.getDataCampaignMap(), [])

    def testGetParam(self):
        """
        Test the `getReqParam` method
        """
        tChainSpec = {"RequestType": "StepChain",
                      "StepChain": 1,
                      "Campaign": "top-campaign",
                      "RequestName": "whatever_name",
                      "DbsUrl": "a_dbs_url",
                      "TrustSitelists": True,
                      "SiteWhitelist": ["CERN", "FNAL", "DESY"],
                      "SiteBlacklist": [],
                      "Step1": {"InputDataset": "/step1/input-dataset/tier",
                                "MCPileup": "/step1/mc-pileup/tier",
                                "Campaign": "step1-campaign"},
                      }
        wflow = Workflow(tChainSpec['RequestName'], tChainSpec)
        self.assertTrue(wflow.getReqParam("TrustSitelists"))
        self.assertIsNone(wflow.getReqParam("MCPileup"))
        self.assertEqual(wflow.getReqParam("RequestType"), wflow.getReqType())
        self.assertEqual(wflow.getReqParam("RequestName"), wflow.getName())

    def testComparison(self):
        """
        Perform basic operations over Workflow objects
        """
        wflow1 = Workflow("workflow_1", {"RequestType": "StepChain"})
        wflow2 = Workflow("workflow_2", {"RequestType": "TaskChain"})
        wflow3 = Workflow("workflow_3", {"RequestType": "ReReco"})
        wflow4 = Workflow("workflow_4", {"RequestType": "StepChain"})
        listWflows = [wflow1, wflow2, wflow3, wflow4]

        self.assertNotEqual(wflow1, wflow4)

        badWflows = [wflow3, wflow3]
        self.assertEqual(len(listWflows), 4)
        self.assertEqual(len(badWflows), 2)
        for wflow in set(badWflows):
            listWflows.remove(wflow)
        self.assertEqual(len(listWflows), 3)
        self.assertEqual(len(badWflows), 2)

    def testParentageRelationship(self):
        """
        Test methods related to the primary and parent datasets and blocks
        """
        primDict = {"block_A": {"blockSize": 1, "locations": ["Site_A"]},
                    "block_B": {"blockSize": 2, "locations": ["Site_B"]}}
        parentDict = {"parent_A": {"blockSize": 11, "locations": ["Site_A"]},
                      "parent_B": {"blockSize": 12, "locations": ["Site_B"]},
                      "parent_C": {"blockSize": 13, "locations": ["Site_A", "Site_B"]}}
        parentage = {"block_A": ["parent_B", "parent_D"],  # parent_D has no replicas!
                     "block_B": ["parent_A", "parent_C"]}
        wflow = Workflow("workflow_1", {"RequestType": "TaskChain",
                                        "InputDataset": "Dataset_name_XXX",
                                        "IncludeParents": True})

        self.assertEqual(wflow.getParentDataset(), "")
        wflow.setParentDataset("Parent_dataset_XXX")
        self.assertEqual(wflow.getParentDataset(), "Parent_dataset_XXX")

        self.assertEqual(wflow.getPrimaryBlocks(), {})
        wflow.setPrimaryBlocks(primDict)
        self.assertItemsEqual(wflow.getPrimaryBlocks().keys(), ["block_A", "block_B"])

        self.assertEqual(wflow.getParentBlocks(), {})
        wflow.setParentBlocks(parentDict)
        self.assertItemsEqual(wflow.getParentBlocks().keys(), ["parent_A", "parent_B", "parent_C"])

        self.assertEqual(wflow.getChildToParentBlocks(), {})
        wflow.setChildToParentBlocks(parentage)
        self.assertItemsEqual(wflow.getChildToParentBlocks(), parentage)

    def testGetChunkBlocks1(self):
        """
        Perform single chunk tests on the `getChunkBlocks` method.
        """
        primDict = {"block_A": {"blockSize": 1, "locations": ["Site_A", "Site_B"]}}
        parentDict = {"parent_A": {"blockSize": 11, "locations": ["Site_A", "Site_B"]},
                      "parent_B": {"blockSize": 8, "locations": []}}
        wflow = Workflow("workflow_1", {"RequestType": "TaskChain",
                                        "InputDataset": "Dataset_name_XXX"})
        wflow.setPrimaryBlocks(primDict)
        blockChunks, sizeChunks = wflow.getChunkBlocks(1)
        self.assertEqual(len(blockChunks), 1)
        self.assertItemsEqual(blockChunks[0], {"block_A"})
        self.assertEqual(len(sizeChunks), 1)
        self.assertEqual(sizeChunks[0], 1)

        # now set a parent
        wflow.setParentDataset("Parent_dataset_XXX")
        wflow.setParentBlocks(parentDict)
        blockChunks, sizeChunks = wflow.getChunkBlocks(1)
        self.assertEqual(len(blockChunks), 1)
        self.assertItemsEqual(blockChunks[0], {"block_A", "parent_A", "parent_B"})
        self.assertEqual(len(sizeChunks), 1)
        self.assertEqual(sizeChunks[0], 20)

    def testGetChunkBlocks2(self):
        """
        Perform block distribution among many chunks, testing the `getChunkBlocks` method.
        """
        primDict = {"block_A": {"blockSize": 1, "locations": ["Site_A"]},
                    "block_B": {"blockSize": 2, "locations": ["Site_B"]}}
        wflow = Workflow("workflow_1", {"RequestType": "TaskChain",
                                        "InputDataset": "Dataset_name_XXX"})
        wflow.setPrimaryBlocks(primDict)

        # same number of chunks and primary blocks
        blockChunks, sizeChunks = wflow.getChunkBlocks(2)
        self.assertEqual(len(blockChunks), 2)
        self.assertItemsEqual(blockChunks[0], {"block_B"})
        self.assertItemsEqual(blockChunks[1], {"block_A"})
        self.assertEqual(len(sizeChunks), 2)
        self.assertEqual(sizeChunks[0], 2)
        self.assertEqual(sizeChunks[1], 1)

        # more chunks than blocks
        blockChunks, sizeChunks = wflow.getChunkBlocks(5)
        self.assertEqual(len(blockChunks), 2)
        self.assertItemsEqual(blockChunks[0], {"block_B"})
        self.assertItemsEqual(blockChunks[1], {"block_A"})
        self.assertEqual(len(sizeChunks), 2)
        self.assertEqual(sizeChunks[0], 2)
        self.assertEqual(sizeChunks[1], 1)

        # more blocks than chunks
        primDict.update({"block_C": {"blockSize": 3, "locations": ["Site_C"]},
                         "block_D": {"blockSize": 4, "locations": ["Site_D"]},
                         "block_E": {"blockSize": 5, "locations": ["Site_E"]}})
        wflow.setPrimaryBlocks(primDict)
        blockChunks, sizeChunks = wflow.getChunkBlocks(3)
        self.assertEqual(len(blockChunks), 3)
        self.assertItemsEqual(blockChunks[0], {"block_E"})
        self.assertItemsEqual(blockChunks[1], {"block_D", "block_A"})
        self.assertItemsEqual(blockChunks[2], {"block_C", "block_B"})
        self.assertEqual(len(sizeChunks), 3)
        self.assertEqual(sizeChunks[0], 5)
        self.assertEqual(sizeChunks[1], 5)
        self.assertEqual(sizeChunks[2], 5)

    def testGetChunkBlocks3(self):
        """
        Test the `getChunkBlocks` method and especially the parent/child
        relationship
        """
        primDict = {"block_A": {"blockSize": 1, "locations": ["Site_A"]},
                    "block_B": {"blockSize": 2, "locations": ["Site_B"]}}
        parentDict = {"parent_A": {"blockSize": 11, "locations": ["Site_A"]},
                      "parent_B": {"blockSize": 12, "locations": ["Site_B"]},
                      "parent_C": {"blockSize": 13, "locations": ["Site_A", "Site_B"]}}
        parentage = {"block_A": ["parent_B", "parent_D"],  # parent_D has no replicas!
                     "block_B": ["parent_A", "parent_C"]}
        wflow = Workflow("workflow_1", {"RequestType": "TaskChain",
                                        "InputDataset": "Dataset_name_XXX"})

        # now set a parent
        wflow.setParentDataset("Parent_dataset_XXX")
        wflow.setPrimaryBlocks(primDict)
        wflow.setParentBlocks(parentDict)
        wflow.setChildToParentBlocks(parentage)

        blockChunks, sizeChunks = wflow.getChunkBlocks(1)
        self.assertEqual(len(blockChunks), 1)
        self.assertItemsEqual(blockChunks[0], {"block_A", "block_B", "parent_A", "parent_B", "parent_C"})
        self.assertEqual(len(sizeChunks), 1)
        self.assertEqual(sizeChunks[0], 39)

        blockChunks, sizeChunks = wflow.getChunkBlocks(2)
        self.assertEqual(len(blockChunks), 2)
        self.assertItemsEqual(blockChunks[0], {"block_B", "parent_A", "parent_C"})
        self.assertItemsEqual(blockChunks[1], {"block_A", "parent_B"})
        self.assertEqual(len(sizeChunks), 2)
        self.assertEqual(sizeChunks[0], 26)
        self.assertEqual(sizeChunks[1], 13)

    def testIsRelVal(self):
        """
        Test the `isRelVal` method functionality
        """
        requestTypes = ("StepChain", "TaskChain", "ReReco")
        for wflowType in requestTypes:
            wflowObj = Workflow("wflow_test", {"RequestType": wflowType})
            self.assertFalse(wflowObj.isRelVal())

            wflowObj = Workflow("wflow_test", {"RequestType": wflowType, "SubRequestType": "ReDigi"})
            self.assertFalse(wflowObj.isRelVal())

        for wflowType in requestTypes:
            wflowObj = Workflow("wflow_test", {"RequestType": wflowType, "SubRequestType": "RelVal"})
            self.assertTrue(wflowObj.isRelVal())

            wflowObj = Workflow("wflow_test", {"RequestType": wflowType, "SubRequestType": "HIRelVal"})
            self.assertTrue(wflowObj.isRelVal())

    def testGetWorkflowGroup(self):
        """
        Test the `getWorkflowGroup` method functionality
        """
        requestTypes = ("StepChain", "TaskChain", "ReReco")
        for wflowType in requestTypes:
            wflowObj = Workflow("wflow_test", {"RequestType": wflowType})
            self.assertEqual(wflowObj.getWorkflowGroup(), "production")

            wflowObj = Workflow("wflow_test", {"RequestType": wflowType, "SubRequestType": "ReDigi"})
            self.assertEqual(wflowObj.getWorkflowGroup(), "production")

        for wflowType in requestTypes:
            wflowObj = Workflow("wflow_test", {"RequestType": wflowType, "SubRequestType": "RelVal"})
            self.assertEqual(wflowObj.getWorkflowGroup(), "relval")


if __name__ == '__main__':
    unittest.main()
