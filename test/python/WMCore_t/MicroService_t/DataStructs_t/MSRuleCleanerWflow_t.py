"""
Unit tests for the WMCore/MicroService/DataStructs/MSRuleCleanerWflow.py module
"""
from __future__ import division, print_function

import os
import json
import unittest

from WMCore.MicroService.DataStructs.MSRuleCleanerWflow import MSRuleCleanerWflow


def getTestFile(partialPath):
    """
    Returns the absolute path for the test json file
    """
    normPath = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))
    return os.path.join(normPath, partialPath)


class MSRuleCleanerWflowTest(unittest.TestCase):
    """
    Test the very basic functionality of the MSRuleCleanerWflow module
    """
    def setUp(self):
        self.maxDiff = None
        self.taskChainFile = getTestFile('data/ReqMgr/requests/Static/TaskChainRequestDump.json')
        self.stepChainFile = getTestFile('data/ReqMgr/requests/Static/StepChainRequestDump.json')
        self.reqRecordsFile = getTestFile('data/ReqMgr/requests/Static/BatchRequestsDump.json')
        with open(self.reqRecordsFile) as fd:
            self.reqRecords = json.load(fd)
        with open(self.taskChainFile) as fd:
            self.taskChainReq = json.load(fd)
        with open(self.stepChainFile) as fd:
            self.stepChainReq = json.load(fd)

        super(MSRuleCleanerWflowTest, self).setUp()

    def testTaskChainDefaults(self):
        """
        Test creating a MSRuleCleanerWflow object out of a TaskChain request dictionary
        """
        wflow = MSRuleCleanerWflow({})
        self.assertEqual(wflow["RequestName"], None)
        self.assertEqual(wflow["RequestType"], None)
        self.assertEqual(wflow["RequestStatus"], None)
        self.assertEqual(wflow["OutputDatasets"], [])
        self.assertEqual(wflow["RulesToClean"], {})
        self.assertEqual(wflow["CleanupStatus"], {})
        self.assertEqual(wflow["TransferDone"], False)
        self.assertEqual(wflow["TargetStatus"], None)
        self.assertEqual(wflow["ParentageResolved"], True)
        self.assertEqual(wflow["PlineMarkers"], None)
        self.assertEqual(wflow["IsClean"], False)
        self.assertEqual(wflow["ForceArchive"], False)

    def testTaskChain(self):
        # Test Taskchain:
        wflow = MSRuleCleanerWflow(self.taskChainReq)
        expectedWflow = {'CleanupStatus': {},
                         'ForceArchive': False,
                         'IsClean': False,
                         'OutputDatasets': [u'/JetHT/CMSSW_7_2_0-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/RECO',
                                            u'/JetHT/CMSSW_7_2_0-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/DQMIO',
                                            u'/JetHT/CMSSW_7_2_0-SiStripCalMinBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO',
                                            u'/JetHT/CMSSW_7_2_0-SiStripCalZeroBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO',
                                            u'/JetHT/CMSSW_7_2_0-TkAlMinBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO'],
                         'ParentageResolved': True,
                         'PlineMarkers': None,
                         'RequestName': u'TaskChain_LumiMask_multiRun_HG2011_Val_201029_112735_5891',
                         'RequestStatus': u'announced',
                         'RequestType': u'TaskChain',
                         'RulesToClean': {},
                         'TargetStatus': None,
                         'TransferDone': False}
        self.assertDictEqual(wflow, expectedWflow)


if __name__ == '__main__':
    unittest.main()
