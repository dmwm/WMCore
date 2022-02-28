"""
Unit tests for the WMCore/MicroService/DataStructs/MSOutputTemplate.py module
"""
from __future__ import division, print_function

import unittest
from copy import deepcopy

from builtins import range

from Utils.PythonVersion import PY3

from WMCore.MicroService.MSOutput.MSOutputTemplate import MSOutputTemplate


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
                "RequestType": "mongo-doc-type",
                "Campaign": "top-campaign",
                "CreationTime": 123,
                "LastUpdate": 123456,
                "IsRelVal": False,
                "OutputDatasets": ["output-dataset-1", "output-dataset-2"],
                "OutputMap": [{'Campaign': 'campaign-1',
                               'Dataset': 'output-dataset-1',
                               'DatasetSize': 123,
                               'Copies': 1,
                               'DiskDestination': "",
                               'TapeDestination': "",
                               'DiskRuleID': "",
                               'TapeRuleID': ""},
                              {'Campaign': 'campaign-2',
                               'Dataset': 'output-dataset-2',
                               'DatasetSize': 456,
                               'Copies': 0,
                               'DiskDestination': "",
                               'TapeDestination': "",
                               'DiskRuleID': "",
                               'TapeRuleID': ""}],
                "TransferStatus": "pending"
                }

    outputMapKeys = ["Campaign", "Copies", "Dataset", "DatasetSize", "DiskDestination",
                     "DiskRuleID", "TapeDestination", "TapeRuleID"]

    def setUp(self):
        if PY3:
            self.assertItemsEqual = self.assertCountEqual

    def testTaskChainSpec(self):
        """
        Test creating a MSOutputTemplate object out of a TaskChain request dictionary
        """
        msOutDoc = MSOutputTemplate(self.taskchainSpec)

        self.assertTrue(msOutDoc["CreationTime"] > 0)
        self.assertIsNone(msOutDoc["LastUpdate"])
        self.assertFalse(msOutDoc["IsRelVal"])
        self.assertEqual(msOutDoc["TransferStatus"], "pending")
        self.assertEqual(msOutDoc["Campaign"], self.taskchainSpec["Campaign"])
        self.assertEqual(msOutDoc["OutputDatasets"], self.taskchainSpec["OutputDatasets"])
        self.assertEqual(msOutDoc["RequestName"], self.taskchainSpec["RequestName"])
        self.assertEqual(msOutDoc["RequestType"], self.taskchainSpec["RequestType"])
        self.assertEqual(msOutDoc["_id"], self.taskchainSpec["_id"])
        self.assertEqual(len(msOutDoc["OutputMap"]), 2)
        self.assertItemsEqual(list(msOutDoc["OutputMap"][0]), self.outputMapKeys)
        for idx in range(2):
            self.assertEqual(msOutDoc["OutputMap"][idx]["DatasetSize"], 0)
            if msOutDoc["OutputMap"][idx]["Campaign"] == self.taskchainSpec["Task1"]["Campaign"]:
                self.assertEqual(msOutDoc["OutputMap"][idx]["Dataset"], "output-dataset-1")
            else:
                self.assertEqual(msOutDoc["OutputMap"][idx]["Campaign"],
                                 self.taskchainSpec["Task2"]["Campaign"])
                self.assertEqual(msOutDoc["OutputMap"][idx]["Dataset"], "output-dataset-2")
            self.assertEqual(msOutDoc["OutputMap"][idx]["Copies"], 1)
            self.assertEqual(msOutDoc["OutputMap"][idx]["DiskDestination"], "")
            self.assertEqual(msOutDoc["OutputMap"][idx]["DiskRuleID"], "")
            self.assertEqual(msOutDoc["OutputMap"][idx]["TapeDestination"], "")
            self.assertEqual(msOutDoc["OutputMap"][idx]["TapeRuleID"], "")

    def testStepChainSpec(self):
        """
        Test creating a MSOutputTemplate object out of a StepChain request dictionary
        """
        msOutDoc = MSOutputTemplate(self.stepchainSpec)

        self.assertTrue(msOutDoc["CreationTime"] > 0)
        self.assertIsNone(msOutDoc["LastUpdate"])
        self.assertFalse(msOutDoc["IsRelVal"])
        self.assertEqual(msOutDoc["TransferStatus"], "pending")
        self.assertEqual(msOutDoc["Campaign"], self.stepchainSpec["Campaign"])
        self.assertEqual(msOutDoc["OutputDatasets"], self.stepchainSpec["OutputDatasets"])
        self.assertEqual(msOutDoc["RequestName"], self.stepchainSpec["RequestName"])
        self.assertEqual(msOutDoc["RequestType"], self.stepchainSpec["RequestType"])
        self.assertEqual(msOutDoc["_id"], self.stepchainSpec["_id"])
        self.assertEqual(len(msOutDoc["OutputMap"]), 2)
        self.assertItemsEqual(list(msOutDoc["OutputMap"][0]), self.outputMapKeys)
        for idx in range(2):
            self.assertEqual(msOutDoc["OutputMap"][idx]["DatasetSize"], 0)
            if msOutDoc["OutputMap"][idx]["Campaign"] == self.stepchainSpec["Step1"]["Campaign"]:
                self.assertEqual(msOutDoc["OutputMap"][idx]["Dataset"], "output-dataset-1")
            else:
                self.assertEqual(msOutDoc["OutputMap"][idx]["Campaign"],
                                 self.stepchainSpec["Step2"]["Campaign"])
                self.assertEqual(msOutDoc["OutputMap"][idx]["Dataset"], "output-dataset-2")

    def testReRecoSpec(self):
        """
        Test creating a MSOutputTemplate object out of a ReReco request dictionary
        """
        msOutDoc = MSOutputTemplate(self.rerecoSpec)

        self.assertTrue(msOutDoc["CreationTime"] > 0)
        self.assertIsNone(msOutDoc["LastUpdate"])
        self.assertFalse(msOutDoc["IsRelVal"])
        self.assertEqual(msOutDoc["TransferStatus"], "pending")
        self.assertEqual(msOutDoc["Campaign"], self.rerecoSpec["Campaign"])
        self.assertEqual(msOutDoc["OutputDatasets"], self.rerecoSpec["OutputDatasets"])
        self.assertEqual(msOutDoc["RequestName"], self.rerecoSpec["RequestName"])
        self.assertEqual(msOutDoc["RequestType"], self.rerecoSpec["RequestType"])
        self.assertEqual(msOutDoc["_id"], self.rerecoSpec["_id"])
        self.assertEqual(len(msOutDoc["OutputMap"]), 2)
        self.assertEqual(msOutDoc["OutputMap"][0]["DatasetSize"], 0)
        self.assertItemsEqual(list(msOutDoc["OutputMap"][0]), self.outputMapKeys)
        self.assertEqual(msOutDoc["OutputMap"][0]["Campaign"], self.rerecoSpec["Campaign"])
        self.assertEqual(msOutDoc["OutputMap"][1]["Campaign"], self.rerecoSpec["Campaign"])
        self.assertItemsEqual([msOutDoc["OutputMap"][0]["Dataset"], msOutDoc["OutputMap"][1]["Dataset"]],
                              self.rerecoSpec["OutputDatasets"])

    def testMongoDoc(self):
        """
        Test creating a MSOutputTemplate object out of a MongoDB record
        """
        msOutDoc = MSOutputTemplate(self.mongoDoc, producerDoc=False)
        self.assertFalse(msOutDoc["IsRelVal"])

        self.assertEqual(msOutDoc["TransferStatus"], "pending")
        self.assertEqual(msOutDoc["CreationTime"], self.mongoDoc["CreationTime"])
        self.assertEqual(msOutDoc["LastUpdate"], self.mongoDoc["LastUpdate"])

        self.assertEqual(msOutDoc["Campaign"], self.mongoDoc["Campaign"])
        self.assertEqual(msOutDoc["OutputDatasets"], self.mongoDoc["OutputDatasets"])
        self.assertEqual(msOutDoc["RequestName"], self.mongoDoc["RequestName"])
        self.assertEqual(msOutDoc["RequestType"], self.mongoDoc["RequestType"])
        self.assertEqual(msOutDoc["_id"], self.mongoDoc["_id"])
        self.assertEqual(len(msOutDoc["OutputMap"]), 2)
        self.assertItemsEqual([msOutDoc["OutputMap"][0]["Dataset"], msOutDoc["OutputMap"][1]["Dataset"]],
                              self.mongoDoc["OutputDatasets"])
        self.assertTrue(msOutDoc["OutputMap"][0]["DatasetSize"] in [123, 456])

        newDoc = deepcopy(self.mongoDoc)
        newDoc.update({"IsRelVal": True, "TransferStatus": "done", "LastUpdate": 333})
        msOutDoc = MSOutputTemplate(newDoc, producerDoc=False)
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

        msOutDoc.updateDoc({"IsRelVal": False, "LastUpdate": 444})
        self.assertFalse(msOutDoc["IsRelVal"])
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
