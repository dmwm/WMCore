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
        with open(self.reqRecordsFile) as fd:
            self.reqRecords = json.load(fd)
        with open(self.taskChainFile) as fd:
            self.taskChainReq = json.load(fd)
        with open(self.stepChainFile) as fd:
            self.stepChainReq = json.load(fd)
        with open(self.reRecoFile) as fd:
            self.reRecoReq = json.load(fd)
        with open(self.includeParentsFile) as fd:
            self.includeParentsReq = json.load(fd)
        with open(self.multiPUFile) as fd:
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
        self.assertEqual(wflow['DataPileup'], [])
        self.assertEqual(wflow['MCPileup'], [])
        self.assertEqual(wflow['InputDataset'], None)
        self.assertEqual(wflow['ParentDataset'], [])

    def testTaskChain(self):
        # Test Taskchain:
        wflow = MSRuleCleanerWflow(self.taskChainReq)
        expectedWflow = {'CleanupStatus': {},
                         'DataPileup': [],
                         'ForceArchive': False,
                         'IncludeParents': False,
                         'InputDataset': u'/JetHT/Run2012C-v1/RAW',
                         'IsArchivalDelayExpired': False,
                         'IsClean': False,
                         'IsLogDBClean': False,
                         'MCPileup': [],
                         'OutputDatasets': [
                             u'/JetHT/CMSSW_7_2_0-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/RECO',
                             u'/JetHT/CMSSW_7_2_0-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/DQMIO',
                             u'/JetHT/CMSSW_7_2_0-SiStripCalMinBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO',
                             u'/JetHT/CMSSW_7_2_0-SiStripCalZeroBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO',
                             u'/JetHT/CMSSW_7_2_0-TkAlMinBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO'],
                         'ParentDataset': [],
                         'ParentageResolved': True,
                         'PlineMarkers': None,
                         'RequestName': u'TaskChain_LumiMask_multiRun_HG2011_Val_201029_112735_5891',
                         'RequestStatus': u'announced',
                         'RequestTransition': [{u'DN': u'',
                                                u'Status': u'new',
                                                u'UpdateTime': 1606723304},
                                               {u'DN': u'', u'Status': u'assignment-approved',
                                                u'UpdateTime': 1606723305},
                                               {u'DN': u'', u'Status': u'assigned', u'UpdateTime': 1606723306},
                                               {u'DN': u'', u'Status': u'staging', u'UpdateTime': 1606723461},
                                               {u'DN': u'', u'Status': u'staged', u'UpdateTime': 1606723590},
                                               {u'DN': u'', u'Status': u'acquired', u'UpdateTime': 1606723968},
                                               {u'DN': u'', u'Status': u'running-open', u'UpdateTime': 1606724572},
                                               {u'DN': u'', u'Status': u'running-closed', u'UpdateTime': 1606724573},
                                               {u'DN': u'', u'Status': u'completed', u'UpdateTime': 1607018413},
                                               {u'DN': u'', u'Status': u'closed-out', u'UpdateTime': 1607347706},
                                               {u'DN': u'', u'Status': u'announced', u'UpdateTime': 1607359514}],
                         'RequestType': u'TaskChain',
                         'SubRequestType': u'',
                         'RulesToClean': {},
                         'TargetStatus': None,
                         'TransferDone': False,
                         'TransferTape': False}
        self.assertDictEqual(wflow, expectedWflow)

    def testReReco(self):
        # Test ReReco workflow:
        wflow = MSRuleCleanerWflow(self.reRecoReq)
        expectedWflow = {'CleanupStatus': {},
                         'DataPileup': [],
                         'ForceArchive': False,
                         'IncludeParents': False,
                         'InputDataset': u'/SingleElectron/Run2017F-v1/RAW',
                         'IsArchivalDelayExpired': False,
                         'IsClean': False,
                         'IsLogDBClean': False,
                         'MCPileup': [],
                         'OutputDatasets': [u'/SingleElectron/Run2017F-09Aug2019_UL2017_EcalRecovery-v1/MINIAOD',
                                            u'/SingleElectron/Run2017F-EcalUncalWElectron-09Aug2019_UL2017_EcalRecovery-v1/ALCARECO',
                                            u'/SingleElectron/Run2017F-EcalUncalZElectron-09Aug2019_UL2017_EcalRecovery-v1/ALCARECO',
                                            u'/SingleElectron/Run2017F-09Aug2019_UL2017_EcalRecovery-v1/AOD',
                                            u'/SingleElectron/Run2017F-EcalESAlign-09Aug2019_UL2017_EcalRecovery-v1/ALCARECO',
                                            u'/SingleElectron/Run2017F-09Aug2019_UL2017_EcalRecovery-v1/DQMIO',
                                            u'/SingleElectron/Run2017F-HcalCalIterativePhiSym-09Aug2019_UL2017_EcalRecovery-v1/ALCARECO'],
                         'ParentDataset': [],
                         'ParentageResolved': True,
                         'PlineMarkers': None,
                         'RequestName': u'pdmvserv_Run2017F-v1_SingleElectron_09Aug2019_UL2017_EcalRecovery_200506_120455_3146',
                         'RequestStatus': u'completed',
                         'RequestTransition': [{u'DN': u'',
                                                u'Status': u'new',
                                                u'UpdateTime': 1588759495},
                                               {u'DN': u'', u'Status': u'assignment-approved',
                                                u'UpdateTime': 1588759500},
                                               {u'DN': u'', u'Status': u'assigned', u'UpdateTime': 1588760963},
                                               {u'DN': u'', u'Status': u'staging', u'UpdateTime': 1588761258},
                                               {u'DN': u'', u'Status': u'staged', u'UpdateTime': 1588761688},
                                               {u'DN': u'', u'Status': u'acquired', u'UpdateTime': 1588762166},
                                               {u'DN': u'', u'Status': u'running-open', u'UpdateTime': 1588763098},
                                               {u'DN': u'', u'Status': u'running-closed', u'UpdateTime': 1588775354},
                                               {u'DN': u'', u'Status': u'completed', u'UpdateTime': 1589041121}],
                         'RequestType': u'ReReco',
                         'SubRequestType': u'',
                         'RulesToClean': {},
                         'TargetStatus': None,
                         'TransferDone': False,
                         'TransferTape': False}
        self.assertDictEqual(wflow, expectedWflow)

    def testIncludeParents(self):
        # Test include parents::
        wflow = MSRuleCleanerWflow(self.includeParentsReq)
        expectedWflow = {'CleanupStatus': {},
                         'DataPileup': [],
                         'ForceArchive': False,
                         'IncludeParents': True,
                         'InputDataset': u'/Cosmics/Commissioning2015-PromptReco-v1/RECO',
                         'IsArchivalDelayExpired': False,
                         'IsClean': False,
                         'IsLogDBClean': False,
                         'MCPileup': [],
                         'OutputDatasets': [
                             u'/Cosmics/Integ_Test-CosmicSP-StepChain_InclParents_HG2004_Val_Privv12-v11/RAW-RECO'],
                         'ParentDataset': [],
                         'ParentageResolved': False,
                         'PlineMarkers': None,
                         'RequestName': u'amaltaro_StepChain_InclParents_April2020_Val_200414_120713_81',
                         'RequestStatus': u'completed',
                         'RequestTransition': [{u'DN': u'',
                                                u'Status': u'new',
                                                u'UpdateTime': 1586858833},
                                               {u'DN': u'', u'Status': u'assignment-approved',
                                                u'UpdateTime': 1586858834},
                                               {u'DN': u'', u'Status': u'assigned', u'UpdateTime': 1586858835},
                                               {u'DN': u'', u'Status': u'staging', u'UpdateTime': 1586859358},
                                               {u'DN': u'', u'Status': u'staged', u'UpdateTime': 1586859733},
                                               {u'DN': u'', u'Status': u'acquired', u'UpdateTime': 1586860322},
                                               {u'DN': u'', u'Status': u'running-open', u'UpdateTime': 1586860927},
                                               {u'DN': u'', u'Status': u'running-closed', u'UpdateTime': 1586861535},
                                               {u'DN': u'', u'Status': u'completed', u'UpdateTime': 1586863942}],
                         'RequestType': u'StepChain',
                         'SubRequestType': u'',
                         'RulesToClean': {},
                         'TargetStatus': None,
                         'TransferDone': False,
                         'TransferTape': False}
        self.assertDictEqual(wflow, expectedWflow)

    def testMultiPU(self):
        # Test workflow with multiple pileups::
        wflow = MSRuleCleanerWflow(self.multiPUReq)
        expectedWflow = {'CleanupStatus': {},
                         'DataPileup': [],
                         'ForceArchive': False,
                         'IncludeParents': False,
                         'InputDataset': None,
                         'IsArchivalDelayExpired': False,
                         'IsClean': False,
                         'IsLogDBClean': False,
                         'MCPileup': [u'/MinBias_TuneCUETP8M1_13TeV-pythia8/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM',
                                      u'/Neutrino_E-10_gun/RunIISpring15PrePremix-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v2-v2/GEN-SIM-DIGI-RAW'],
                         'OutputDatasets': [
                             u'/DYJetsToLL_Pt-50To100_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/DMWM_Test-SC_MultiPU_HG2002_Val_Todor_v13-v20/GEN-SIM',
                             u'/DYJetsToLL_Pt-50To100_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/DMWM_Test-SC_MultiPU_HG2002_Val_Todor_v13-v20/LHE',
                             u'/DYJetsToLL_Pt-50To100_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/DMWM_Test-SC_MultiPU_HG2002_Val_Todor_v13-v20/GEN-SIM-RAW'],
                         'ParentDataset': [],
                         'ParentageResolved': False,
                         'PlineMarkers': None,
                         'RequestName': u'tivanov_SC_MultiPU_HG2002_Val_200121_043659_4925',
                         'RequestStatus': u'completed',
                         'RequestTransition': [{u'DN': u'',
                                                u'Status': u'new',
                                                u'UpdateTime': 1579577819},
                                               {u'DN': u'', u'Status': u'assignment-approved',
                                                u'UpdateTime': 1579577822},
                                               {u'DN': u'', u'Status': u'assigned', u'UpdateTime': 1579577822},
                                               {u'DN': u'', u'Status': u'staging', u'UpdateTime': 1579577895},
                                               {u'DN': u'', u'Status': u'staged', u'UpdateTime': 1579578050},
                                               {u'DN': u'', u'Status': u'acquired', u'UpdateTime': 1579578770},
                                               {u'DN': u'', u'Status': u'running-open', u'UpdateTime': 1579578770},
                                               {u'DN': u'', u'Status': u'running-closed', u'UpdateTime': 1579579378},
                                               {u'DN': u'', u'Status': u'completed', u'UpdateTime': 1579587203}],
                         'RequestType': u'StepChain',
                         'SubRequestType': u'',
                         'RulesToClean': {},
                         'TargetStatus': None,
                         'TransferDone': False,
                         'TransferTape': False}
        self.assertDictEqual(wflow, expectedWflow)


if __name__ == '__main__':
    unittest.main()
