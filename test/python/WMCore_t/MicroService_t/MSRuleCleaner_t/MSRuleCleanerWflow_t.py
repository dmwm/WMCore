"""
Unit tests for the WMCore/MicroService/DataStructs/MSRuleCleanerWflow.py module
"""
from __future__ import division, print_function

import json
import os
import unittest

from WMCore.MicroService.MSRuleCleaner.MSRuleCleanerWflow import MSRuleCleanerWflow


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
        self.reRecoFile = getTestFile('data/ReqMgr/requests/Static/ReRecoRequestDump.json')
        self.includeParentsFile = getTestFile('data/ReqMgr/requests/Static/IncludeParentsRequestDump.json')
        self.multiPUFile = getTestFile('data/ReqMgr/requests/Static/MultiPURequestDump.json')
        self.reqRecordsFile = getTestFile('data/ReqMgr/requests/Static/BatchRequestsDump.json')
        with open(self.reqRecordsFile, encoding="utf8") as fd:
            self.reqRecords = json.load(fd)
        with open(self.taskChainFile, encoding="utf8") as fd:
            self.taskChainReq = json.load(fd)
        with open(self.stepChainFile, encoding="utf8") as fd:
            self.stepChainReq = json.load(fd)
        with open(self.reRecoFile, encoding="utf8") as fd:
            self.reRecoReq = json.load(fd)
        with open(self.includeParentsFile, encoding="utf8") as fd:
            self.includeParentsReq = json.load(fd)
        with open(self.multiPUFile, encoding="utf8") as fd:
            self.multiPUReq = json.load(fd)
        super(MSRuleCleanerWflowTest, self).setUp()

    def testTaskChainDefaults(self):
        """
        Test creating a MSRuleCleanerWflow object out of a TaskChain request dictionary
        """
        wflow = MSRuleCleanerWflow({})
        self.assertEqual(wflow["RequestName"], None)
        self.assertEqual(wflow["RequestType"], None)
        self.assertEqual(wflow["SubRequestType"], None)
        self.assertEqual(wflow["RequestStatus"], None)
        self.assertEqual(wflow["OutputDatasets"], [])
        self.assertEqual(wflow["RulesToClean"], {})
        self.assertEqual(wflow["CleanupStatus"], {})
        self.assertEqual(wflow["TransferDone"], False)
        self.assertEqual(wflow["TargetStatus"], None)
        self.assertEqual(wflow["ParentageResolved"], True)
        self.assertEqual(wflow["PlineMarkers"], None)
        self.assertEqual(wflow["IsClean"], False)
        self.assertEqual(wflow["IsLogDBClean"], False)
        self.assertEqual(wflow["IsArchivalDelayExpired"], False)
        self.assertEqual(wflow["ForceArchive"], False)
        self.assertEqual(wflow["RequestTransition"], [])
        self.assertEqual(wflow['IncludeParents'], False)
        self.assertEqual(wflow['InputDataset'], None)
        self.assertEqual(wflow['ParentDataset'], [])
        self.assertEqual(wflow['TapeRulesStatus'], [])
        self.assertEqual(wflow['StatusAdvanceExpiredMsg'], "")

    def testTaskChain(self):
        # Test Taskchain:
        wflow = MSRuleCleanerWflow(self.taskChainReq)
        expectedWflow = {'CleanupStatus': {},
                         'ForceArchive': False,
                         'IncludeParents': False,
                         'InputDataset': '/JetHT/Run2012C-v1/RAW',
                         'IsArchivalDelayExpired': False,
                         'IsClean': False,
                         'IsLogDBClean': False,
                         'OutputDatasets': [
                             '/JetHT/CMSSW_7_2_0-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/RECO',
                             '/JetHT/CMSSW_7_2_0-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/DQMIO',
                             '/JetHT/CMSSW_7_2_0-SiStripCalMinBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO',
                             '/JetHT/CMSSW_7_2_0-SiStripCalZeroBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO',
                             '/JetHT/CMSSW_7_2_0-TkAlMinBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO'],
                         'ParentDataset': [],
                         'ParentageResolved': True,
                         'PlineMarkers': None,
                         'RequestName': 'TaskChain_LumiMask_multiRun_HG2011_Val_201029_112735_5891',
                         'RequestStatus': 'announced',
                         'RequestTransition': [{'DN': '',
                                                'Status': 'new',
                                                'UpdateTime': 1606723304},
                                               {'DN': '', 'Status': 'assignment-approved',
                                                'UpdateTime': 1606723305},
                                               {'DN': '', 'Status': 'assigned', 'UpdateTime': 1606723306},
                                               {'DN': '', 'Status': 'staging', 'UpdateTime': 1606723461},
                                               {'DN': '', 'Status': 'staged', 'UpdateTime': 1606723590},
                                               {'DN': '', 'Status': 'acquired', 'UpdateTime': 1606723968},
                                               {'DN': '', 'Status': 'running-open', 'UpdateTime': 1606724572},
                                               {'DN': '', 'Status': 'running-closed', 'UpdateTime': 1606724573},
                                               {'DN': '', 'Status': 'completed', 'UpdateTime': 1607018413},
                                               {'DN': '', 'Status': 'closed-out', 'UpdateTime': 1607347706},
                                               {'DN': '', 'Status': 'announced', 'UpdateTime': 1607359514}],
                         'RequestType': 'TaskChain',
                         'SubRequestType': '',
                         'RulesToClean': {},
                         'TargetStatus': None,
                         'TransferDone': False,
                         'TransferTape': False,
                         'TapeRulesStatus': [],
                         'StatusAdvanceExpiredMsg': ""}
        self.assertDictEqual(wflow, expectedWflow)

    def testReReco(self):
        # Test ReReco workflow:
        wflow = MSRuleCleanerWflow(self.reRecoReq)
        expectedWflow = {'CleanupStatus': {},
                         'ForceArchive': False,
                         'IncludeParents': False,
                         'InputDataset': '/SingleElectron/Run2017F-v1/RAW',
                         'IsArchivalDelayExpired': False,
                         'IsClean': False,
                         'IsLogDBClean': False,
                         'OutputDatasets': ['/SingleElectron/Run2017F-09Aug2019_UL2017_EcalRecovery-v1/MINIAOD',
                                            '/SingleElectron/Run2017F-EcalUncalWElectron-09Aug2019_UL2017_EcalRecovery-v1/ALCARECO',
                                            '/SingleElectron/Run2017F-EcalUncalZElectron-09Aug2019_UL2017_EcalRecovery-v1/ALCARECO',
                                            '/SingleElectron/Run2017F-09Aug2019_UL2017_EcalRecovery-v1/AOD',
                                            '/SingleElectron/Run2017F-EcalESAlign-09Aug2019_UL2017_EcalRecovery-v1/ALCARECO',
                                            '/SingleElectron/Run2017F-09Aug2019_UL2017_EcalRecovery-v1/DQMIO',
                                            '/SingleElectron/Run2017F-HcalCalIterativePhiSym-09Aug2019_UL2017_EcalRecovery-v1/ALCARECO'],
                         'ParentDataset': [],
                         'ParentageResolved': True,
                         'PlineMarkers': None,
                         'RequestName': 'pdmvserv_Run2017F-v1_SingleElectron_09Aug2019_UL2017_EcalRecovery_200506_120455_3146',
                         'RequestStatus': 'completed',
                         'RequestTransition': [{'DN': '',
                                                'Status': 'new',
                                                'UpdateTime': 1588759495},
                                               {'DN': '', 'Status': 'assignment-approved',
                                                'UpdateTime': 1588759500},
                                               {'DN': '', 'Status': 'assigned', 'UpdateTime': 1588760963},
                                               {'DN': '', 'Status': 'staging', 'UpdateTime': 1588761258},
                                               {'DN': '', 'Status': 'staged', 'UpdateTime': 1588761688},
                                               {'DN': '', 'Status': 'acquired', 'UpdateTime': 1588762166},
                                               {'DN': '', 'Status': 'running-open', 'UpdateTime': 1588763098},
                                               {'DN': '', 'Status': 'running-closed', 'UpdateTime': 1588775354},
                                               {'DN': '', 'Status': 'completed', 'UpdateTime': 1589041121}],
                         'RequestType': 'ReReco',
                         'SubRequestType': '',
                         'RulesToClean': {},
                         'TargetStatus': None,
                         'TransferDone': False,
                         'TransferTape': False,
                         'TapeRulesStatus': [],
                         'StatusAdvanceExpiredMsg': ""}
        self.assertDictEqual(wflow, expectedWflow)

    def testIncludeParents(self):
        # Test include parents::
        wflow = MSRuleCleanerWflow(self.includeParentsReq)
        expectedWflow = {'CleanupStatus': {},
                         'ForceArchive': False,
                         'IncludeParents': True,
                         'InputDataset': '/Cosmics/Commissioning2015-PromptReco-v1/RECO',
                         'IsArchivalDelayExpired': False,
                         'IsClean': False,
                         'IsLogDBClean': False,
                         'OutputDatasets': [
                             '/Cosmics/Integ_Test-CosmicSP-StepChain_InclParents_HG2004_Val_Privv12-v11/RAW-RECO'],
                         'ParentDataset': [],
                         'ParentageResolved': False,
                         'PlineMarkers': None,
                         'RequestName': 'amaltaro_StepChain_InclParents_April2020_Val_200414_120713_81',
                         'RequestStatus': 'completed',
                         'RequestTransition': [{'DN': '',
                                                'Status': 'new',
                                                'UpdateTime': 1586858833},
                                               {'DN': '', 'Status': 'assignment-approved',
                                                'UpdateTime': 1586858834},
                                               {'DN': '', 'Status': 'assigned', 'UpdateTime': 1586858835},
                                               {'DN': '', 'Status': 'staging', 'UpdateTime': 1586859358},
                                               {'DN': '', 'Status': 'staged', 'UpdateTime': 1586859733},
                                               {'DN': '', 'Status': 'acquired', 'UpdateTime': 1586860322},
                                               {'DN': '', 'Status': 'running-open', 'UpdateTime': 1586860927},
                                               {'DN': '', 'Status': 'running-closed', 'UpdateTime': 1586861535},
                                               {'DN': '', 'Status': 'completed', 'UpdateTime': 1586863942}],
                         'RequestType': 'StepChain',
                         'SubRequestType': '',
                         'RulesToClean': {},
                         'TargetStatus': None,
                         'TransferDone': False,
                         'TransferTape': False,
                         'TapeRulesStatus': [],
                         'StatusAdvanceExpiredMsg': ""}
        self.assertDictEqual(wflow, expectedWflow)

if __name__ == '__main__':
    unittest.main()
