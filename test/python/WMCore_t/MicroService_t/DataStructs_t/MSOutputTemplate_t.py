"""
Unit tests for the WMCore/MicroService/DataStructs/MSOutputTemplate.py module
"""
from __future__ import division, print_function

import unittest
from copy import deepcopy
from pprint import pprint

from WMCore.MicroService.DataStructs.MSOutputTemplate import MSOutputTemplate


class MSOutputTemplateTest(unittest.TestCase):
    """
    Test the very basic functionality of the MSOutputTemplate module
    """
    taskchainSpec = {"_id": "taskchain_id",
                     "RequestType": "TaskChain",
                     "SubRequestType": "",
                     "TaskChain": 2,
                     "Campaign": "top-campaign",
                     "RequestName": "taskchain_request_name",
                     "SiteWhitelist": ["Site_1", "Site_2"],
                     "Task1": {"KeepOutput": False,
                               "Campaign": "task1-campaign"},
                     "Task2": {"KeepOutput": False,
                               "Campaign": "task2-campaign"},
                     "OutputDatasets": ["output-dataset-1", "output-dataset-2"],
                     "ChainParentageMap": {"Task1": {"ParentDset": None,
                                                     "ChildDsets": ["output-dataset-1"]},
                                           "Task2": {"ParentDset": "output-dataset-1",
                                                     "ChildDsets": ["output-dataset-2"]}}
                     }
    stepchainSpec = {"_id": "stepchain_id",
                     "RequestType": "StepChain",
                     "SubRequestType": "",
                     "StepChain": 2,
                     "Campaign": "top-campaign",
                     "RequestName": "stepchain_request_name",
                     "SiteWhitelist": ["Site_1", "Site_2"],
                     "Step1": {"KeepOutput": False,
                               "Campaign": "step1-campaign"},
                     "Step2": {"KeepOutput": False,
                               "Campaign": "step2-campaign"},
                     "OutputDatasets": ["output-dataset-1", "output-dataset-2"],
                     "ChainParentageMap": {"Step1": {"ParentDset": None,
                                                     "ChildDsets": ["output-dataset-1"]},
                                           "Step2": {"ParentDset": "output-dataset-1",
                                                     "ChildDsets": ["output-dataset-2"]}}
                     }
    rerecoSpec = {"_id": "rereco_id",
                  "RequestType": "ReReco",
                  "SubRequestType": "",
                  "Campaign": "top-campaign",
                  "RequestName": "rereco_request_name",
                  "SiteWhitelist": ["Site_1", "Site_2"],
                  "OutputDatasets": ["output-dataset-1", "output-dataset-2"]
                  }

    mongoDoc = {"_id": "any-mongo-doc-id",
                "RequestName": "mongo-doc-name",
                "Campaign": "top-campaign",
                "CreationTime": 123,
                "LastUpdate": 123456,
                "IsRelVal": False,
                "OutputDatasets": ["output-dataset-1", "output-dataset-2"],
                "Destination": ["Site_1", "Site_2"],
                "DestinationOutputMap": [{"Destination": ["Site_1"],
                                          "Datasets": ["output-dataset-1"]},
                                         {"Destination": ["Site_2"],
                                          "Datasets": ["output-dataset-2"]}],
                "CampaignOutputMap": [{"CampaignName": "top-campaign",
                                       "Datasets": ["output-dataset-1", "output-dataset-2"]}],
                "TransferOutputMap": [{"TransferID": "xxx",
                                       "TransferType": "disk",
                                       "DatasetName": "output-dataset-1"},
                                      {"TransferID": "yyy",
                                       "TransferType": "tape",
                                       "DatasetName": "output-dataset-2"}],
                "TransferStatus": "pending",  # either "pending" or "done",
                "TransferIDs": ["123456"],
                "NumberOfCopies": 1
                }

    def testTaskChainSpec(self):
        """
        Test creating a MSOutputTemplate object out of a TaskChain request dictionary
        """
        msOutDoc = MSOutputTemplate(self.taskchainSpec)
        for key in ["Destination", "DestinationOutputMap", "TransferIDs", "TransferOutputMap"]:
            self.assertEqual(msOutDoc[key], [])
        pprint(msOutDoc)

        self.assertTrue(msOutDoc["CreationTime"] > 0)
        self.assertIsNone(msOutDoc["LastUpdate"])
        self.assertFalse(msOutDoc["IsRelVal"])
        self.assertEqual(msOutDoc["TransferStatus"], "pending")
        self.assertEqual(msOutDoc["NumberOfCopies"], 1)
        self.assertEqual(msOutDoc["Campaign"], self.taskchainSpec["Campaign"])
        self.assertEqual(msOutDoc["OutputDatasets"], self.taskchainSpec["OutputDatasets"])
        self.assertEqual(msOutDoc["RequestName"], self.taskchainSpec["RequestName"])
        self.assertEqual(msOutDoc["_id"], self.taskchainSpec["_id"])
        self.assertEqual(len(msOutDoc["CampaignOutputMap"]), 2)
        for idx in range(2):
            if msOutDoc["CampaignOutputMap"][idx]["CampaignName"] == self.taskchainSpec["Task1"]["Campaign"]:
                self.assertEqual(msOutDoc["CampaignOutputMap"][idx]["Datasets"], ["output-dataset-1"])
            else:
                self.assertEqual(msOutDoc["CampaignOutputMap"][idx]["CampaignName"],
                                 self.taskchainSpec["Task2"]["Campaign"])
                self.assertEqual(msOutDoc["CampaignOutputMap"][idx]["Datasets"], ["output-dataset-2"])

    def testStepChainSpec(self):
        """
        Test creating a MSOutputTemplate object out of a StepChain request dictionary
        """
        msOutDoc = MSOutputTemplate(self.stepchainSpec)
        for key in ["Destination", "DestinationOutputMap", "TransferIDs", "TransferOutputMap"]:
            self.assertEqual(msOutDoc[key], [])
        pprint(msOutDoc)

        self.assertTrue(msOutDoc["CreationTime"] > 0)
        self.assertIsNone(msOutDoc["LastUpdate"])
        self.assertFalse(msOutDoc["IsRelVal"])
        self.assertEqual(msOutDoc["TransferStatus"], "pending")
        self.assertEqual(msOutDoc["NumberOfCopies"], 1)
        self.assertEqual(msOutDoc["Campaign"], self.stepchainSpec["Campaign"])
        self.assertEqual(msOutDoc["OutputDatasets"], self.stepchainSpec["OutputDatasets"])
        self.assertEqual(msOutDoc["RequestName"], self.stepchainSpec["RequestName"])
        self.assertEqual(msOutDoc["_id"], self.stepchainSpec["_id"])
        self.assertEqual(len(msOutDoc["CampaignOutputMap"]), 2)
        for idx in range(2):
            if msOutDoc["CampaignOutputMap"][idx]["CampaignName"] == self.stepchainSpec["Step1"]["Campaign"]:
                self.assertEqual(msOutDoc["CampaignOutputMap"][idx]["Datasets"], ["output-dataset-1"])
            else:
                self.assertEqual(msOutDoc["CampaignOutputMap"][idx]["CampaignName"],
                                 self.stepchainSpec["Step2"]["Campaign"])
                self.assertEqual(msOutDoc["CampaignOutputMap"][idx]["Datasets"], ["output-dataset-2"])
        self.assertTrue(msOutDoc["CreationTime"] > 0)

    def testReRecoSpec(self):
        """
        Test creating a MSOutputTemplate object out of a ReReco request dictionary
        """
        msOutDoc = MSOutputTemplate(self.rerecoSpec)
        for key in ["Destination", "DestinationOutputMap", "TransferIDs", "TransferOutputMap"]:
            self.assertEqual(msOutDoc[key], [])
        pprint(msOutDoc)

        self.assertTrue(msOutDoc["CreationTime"] > 0)
        self.assertIsNone(msOutDoc["LastUpdate"])
        self.assertFalse(msOutDoc["IsRelVal"])
        self.assertEqual(msOutDoc["TransferStatus"], "pending")
        self.assertEqual(msOutDoc["NumberOfCopies"], 1)
        self.assertEqual(msOutDoc["Campaign"], self.rerecoSpec["Campaign"])
        self.assertEqual(msOutDoc["OutputDatasets"], self.rerecoSpec["OutputDatasets"])
        self.assertEqual(msOutDoc["RequestName"], self.rerecoSpec["RequestName"])
        self.assertEqual(msOutDoc["_id"], self.rerecoSpec["_id"])
        self.assertEqual(len(msOutDoc["CampaignOutputMap"]), 1)
        self.assertEqual(msOutDoc["CampaignOutputMap"][0]["CampaignName"], self.rerecoSpec["Campaign"])
        self.assertItemsEqual(msOutDoc["CampaignOutputMap"][0]["Datasets"], self.rerecoSpec["OutputDatasets"])
        self.assertTrue(msOutDoc["CreationTime"] > 0)

    def testMongoDoc(self):
        """
        Test creating a MSOutputTemplate object out of a MongoDB record
        """
        msOutDoc = MSOutputTemplate(self.mongoDoc)
        pprint(msOutDoc)
        self.assertFalse(msOutDoc["IsRelVal"])

        self.assertEqual(msOutDoc["TransferStatus"], "pending")
        self.assertEqual(msOutDoc["CreationTime"], self.mongoDoc["CreationTime"])
        self.assertEqual(msOutDoc["LastUpdate"], self.mongoDoc["LastUpdate"])
        self.assertEqual(msOutDoc["NumberOfCopies"], self.mongoDoc["NumberOfCopies"])
        self.assertItemsEqual(msOutDoc["TransferIDs"], self.mongoDoc["TransferIDs"])

        self.assertEqual(msOutDoc["Campaign"], self.mongoDoc["Campaign"])
        self.assertEqual(msOutDoc["OutputDatasets"], self.mongoDoc["OutputDatasets"])
        self.assertEqual(msOutDoc["RequestName"], self.mongoDoc["RequestName"])
        self.assertEqual(msOutDoc["_id"], self.mongoDoc["_id"])
        self.assertItemsEqual(msOutDoc["Destination"], self.mongoDoc["Destination"])
        self.assertItemsEqual(msOutDoc["DestinationOutputMap"], self.mongoDoc["DestinationOutputMap"])
        self.assertItemsEqual(msOutDoc["CampaignOutputMap"], self.mongoDoc["CampaignOutputMap"])
        self.assertItemsEqual(msOutDoc["TransferOutputMap"], self.mongoDoc["TransferOutputMap"])

        newDoc = deepcopy(self.mongoDoc)
        newDoc.update({"IsRelVal": True, "TransferStatus": "done", "LastUpdate": 333})
        msOutDoc = MSOutputTemplate(newDoc)
        self.assertTrue(msOutDoc["IsRelVal"])
        self.assertEqual(msOutDoc["TransferStatus"], "done")
        self.assertEqual(msOutDoc["LastUpdate"], 333)

    def testSetters(self):
        """
        Test the MSOutputTemplate setter methods
        """
        msOutDoc = MSOutputTemplate(self.mongoDoc)
        self.assertFalse(msOutDoc["IsRelVal"])
        msOutDoc._setRelVal({"SubRequestType": "RelVal"})
        msOutDoc.setKey("IsRelVal", True)
        self.assertTrue(msOutDoc["IsRelVal"])

        with self.assertRaises(KeyError):
            msOutDoc.setKey("alan", True)

        self.assertItemsEqual(msOutDoc["Destination"], ["Site_1", "Site_2"])
        msOutDoc.updateDoc({"Destination": [], "LastUpdate": 444})
        self.assertItemsEqual(msOutDoc["Destination"], [])
        self.assertEqual(msOutDoc["LastUpdate"], 444)

        msOutDoc.updateTime()
        self.assertTrue(msOutDoc["LastUpdate"] > 444)

        with self.assertRaises(RuntimeError):
            msOutDoc.setTransferStatus("bad_status")
        self.assertEqual(msOutDoc["TransferStatus"], "pending")
        msOutDoc.setTransferStatus("done")
        self.assertEqual(msOutDoc["TransferStatus"], "done")


if __name__ == '__main__':
    unittest.main()
