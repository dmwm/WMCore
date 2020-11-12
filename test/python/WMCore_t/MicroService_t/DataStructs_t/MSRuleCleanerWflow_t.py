"""
Unit tests for the WMCore/MicroService/DataStructs/MSRuleCleanerWflow.py module
"""
from __future__ import division, print_function

import unittest

from WMCore.MicroService.DataStructs.MSRuleCleanerWflow import MSRuleCleanerWflow


class MSRuleCleanerWflowTest(unittest.TestCase):
    """
    Test the very basic functionality of the MSRuleCleanerWflow module
    """
    taskchainSpec = {"_id": "taskchain_id",
                     "RequestType": "TaskChain",
                     "RequestStatus": "announced",
                     "ParentageResolved": False,
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
                     "RequestStatus": "rejected",
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

    def testTaskChainSpec(self):
        """
        Test creating a MSRuleCleanerWflow object out of a TaskChain request dictionary
        """
        wflow = MSRuleCleanerWflow(self.taskchainSpec)
        self.assertEqual(wflow["RequestName"], "taskchain_request_name")
        self.assertEqual(wflow["RequestType"], "TaskChain")
        self.assertEqual(wflow["RequestStatus"], "announced")
        self.assertEqual(wflow["OutputDatasets"], ["output-dataset-1", "output-dataset-2"])
        self.assertEqual(wflow["RulesToClean"], {})
        self.assertEqual(wflow["CleanupStatus"], {})
        self.assertEqual(wflow["TransferDone"], False)
        self.assertEqual(wflow["TargetStatus"], None)
        self.assertEqual(wflow["ParentageResolved"], False)
        self.assertEqual(wflow["PlineMarkers"], None)
        self.assertEqual(wflow["IsClean"], False)
        self.assertEqual(wflow["ForceArchive"], False)

    def testStepChainSpec(self):
        """
        Test creating a MSRuleCleanerWflow object out of a StepChain request dictionary
        """
        wflow = MSRuleCleanerWflow(self.stepchainSpec)
        self.assertEqual(wflow["RequestName"], "stepchain_request_name")
        self.assertEqual(wflow["RequestType"], "StepChain")
        self.assertEqual(wflow["RequestStatus"], "rejected")
        self.assertEqual(wflow["OutputDatasets"], ["output-dataset-1", "output-dataset-2"])
        self.assertEqual(wflow["RulesToClean"], {})
        self.assertEqual(wflow["CleanupStatus"], {})
        self.assertEqual(wflow["TransferDone"], False)
        self.assertEqual(wflow["TargetStatus"], None)
        self.assertEqual(wflow["ParentageResolved"], True)
        self.assertEqual(wflow["PlineMarkers"], None)
        self.assertEqual(wflow["IsClean"], False)
        self.assertEqual(wflow["ForceArchive"], False)


if __name__ == '__main__':
    unittest.main()
