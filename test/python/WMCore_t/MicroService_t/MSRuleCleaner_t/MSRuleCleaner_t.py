"""
Unit tests for Unified/MSRuleCleaner.py module

"""
# pylint: disable=W0212

from __future__ import division, print_function

import json
# system modules
import os
import unittest

# WMCore modules
from WMCore.MicroService.MSRuleCleaner.MSRuleCleaner import MSRuleCleaner, MSRuleCleanerArchivalSkip
from WMCore.MicroService.MSRuleCleaner.MSRuleCleanerWflow import MSRuleCleanerWflow
from WMCore.Services.Rucio import Rucio


def getTestFile(partialPath):
    """
    Returns the absolute path for the test json file
    """
    normPath = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))
    return os.path.join(normPath, partialPath)


# class MSRuleCleanerTest(EmulatedUnitTestCase):
class MSRuleCleanerTest(unittest.TestCase):
    "Unit test for MSruleCleaner module"

    def setUp(self):
        "init test class"
        self.maxDiff = None
        self.msConfig = {"verbose": True,
                         "interval": 1 * 60,
                         "services": ['ruleCleaner'],
                         "rucioWmaAcct": 'wma_test',
                         "rucioAccount": 'wma_test',
                         'reqmgr2Url': 'https://cmsweb-testbed.cern.ch/reqmgr2',
                         'msOutputUrl': 'https://cmsweb-testbed.cern.ch/ms-output',
                         'reqmgrCacheUrl': 'https://cmsweb-testbed.cern.ch/couchdb/reqmgr_workload_cache',
                         'phedexUrl': 'https://cmsweb-testbed.cern.ch/phedex/datasvc/json/prod',
                         'dbsUrl': 'https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader',
                         'rucioUrl': 'http://cms-rucio-int.cern.ch',
                         'rucioAuthUrl': 'https://cms-rucio-auth-int.cern.ch',
                         "wmstatsUrl": "https://cmsweb-testbed.cern.ch/wmstatsserver",
                         "logDBUrl": "https://cmsweb-testbed.cern.ch/couchdb/wmstats_logdb",
                         'logDBReporter': 'reqmgr2ms_ruleCleaner',
                         'archiveDelayHours': 8,
                         'archiveAlarmHours': 24,
                         'enableRealMode': False}

        self.creds = {"client_cert": os.getenv("X509_USER_CERT", "Unknown"),
                      "client_key": os.getenv("X509_USER_KEY", "Unknown")}
        self.rucioConfigDict = {"rucio_host": self.msConfig['rucioUrl'],
                                "auth_host": self.msConfig['rucioAuthUrl'],
                                "auth_type": "x509",
                                "account": self.msConfig['rucioAccount'],
                                "ca_cert": False,
                                "timeout": 30,
                                "request_retries": 3,
                                "creds": self.creds}

        self.reqStatus = ['announced', 'aborted-completed', 'rejected']
        self.msRuleCleaner = MSRuleCleaner(self.msConfig)
        self.msRuleCleaner.resetCounters()
        self.msRuleCleaner.rucio = Rucio.Rucio(self.msConfig['rucioAccount'],
                                               hostUrl=self.rucioConfigDict['rucio_host'],
                                               authUrl=self.rucioConfigDict['auth_host'],
                                               configDict=self.rucioConfigDict)

        self.taskChainFile = getTestFile('data/ReqMgr/requests/Static/TaskChainRequestDump.json')
        self.stepChainFile = getTestFile('data/ReqMgr/requests/Static/StepChainRequestDump.json')
        self.reqRecordsFile = getTestFile('data/ReqMgr/requests/Static/BatchRequestsDump.json')
        with open(self.reqRecordsFile, encoding="utf-8") as fd:
            self.reqRecords = json.load(fd)
        with open(self.taskChainFile, encoding="utf-8") as fd:
            self.taskChainReq = json.load(fd)
        with open(self.stepChainFile, encoding="utf-8") as fd:
            self.stepChainReq = json.load(fd)
        super(MSRuleCleanerTest, self).setUp()

    def testGetLastStatusTransitionTime(self):
        wflow = MSRuleCleanerWflow(self.taskChainReq)
        lastStatusTransition = self.msRuleCleaner._getLastStatusTransitionTime(wflow)
        self.assertEqual(lastStatusTransition, 1607359514)

    def testIsStatusAdvanceExpired(self):
        wflow = MSRuleCleanerWflow(self.taskChainReq)
        self.assertTrue(self.msRuleCleaner._checkStatusAdvanceExpired(wflow))

    def testPipelineAgentBlock(self):
        # Test plineAgentBlock:
        wflow = MSRuleCleanerWflow(self.taskChainReq)
        self.msRuleCleaner.plineAgentBlock.run(wflow)
        expectedWflow = {'CleanupStatus': {'plineAgentBlock': True},
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
                         'PlineMarkers': ['plineAgentBlock'],
                         'RequestName': 'TaskChain_LumiMask_multiRun_HG2011_Val_201029_112735_5891',
                         'RequestStatus': 'announced',
                         'RequestTransition': [{'DN': '', 'Status': 'new', 'UpdateTime': 1606723304},
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
                         'RulesToClean': {'plineAgentBlock': []},
                         'TargetStatus': None,
                         'TransferDone': False,
                         'TransferDisk': False,
                         'TransferTape': False,
                         'TapeRulesStatus': [],
                         'WmcOutputRulesMap': {},
                         'StatusAdvanceExpiredMsg': ""}
        self.assertDictEqual(wflow, expectedWflow)

    def testPipelineAgentCont(self):
        # Test plineAgentCont
        wflow = MSRuleCleanerWflow(self.taskChainReq)
        self.msRuleCleaner.plineAgentCont.run(wflow)
        expectedWflow = {'CleanupStatus': {'plineAgentCont': True},
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
                         'PlineMarkers': ['plineAgentCont'],
                         'RequestName': 'TaskChain_LumiMask_multiRun_HG2011_Val_201029_112735_5891',
                         'RequestStatus': 'announced',
                         'RequestTransition': [{'DN': '', 'Status': 'new', 'UpdateTime': 1606723304},
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
                         'RulesToClean': {'plineAgentCont': []},
                         'TargetStatus': None,
                         'TransferDone': False,
                         'TransferDisk': False,
                         'TransferTape': False,
                         'TapeRulesStatus': [],
                         'WmcOutputRulesMap': {},
                         'StatusAdvanceExpiredMsg': ""}
        self.assertDictEqual(wflow, expectedWflow)

    def testPipelineMSTrBlock(self):
        # Test plineAgentCont
        wflow = MSRuleCleanerWflow(self.taskChainReq)
        self.msRuleCleaner.plineMSTrBlock.run(wflow)
        expectedWflow = {'CleanupStatus': {'plineMSTrBlock': True},
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
                         'PlineMarkers': ['plineMSTrBlock'],
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
                         'RulesToClean': {'plineMSTrBlock': []},
                         'TargetStatus': None,
                         'TransferDone': False,
                         'TransferDisk': False,
                         'TransferTape': False,
                         'TapeRulesStatus': [],
                         'WmcOutputRulesMap': {},
                         'StatusAdvanceExpiredMsg': ""}
        self.assertDictEqual(wflow, expectedWflow)

    def testPipelineMSTrCont(self):
        # Test plineAgentCont
        wflow = MSRuleCleanerWflow(self.taskChainReq)
        self.msRuleCleaner.plineMSTrCont.run(wflow)
        expectedWflow = {'CleanupStatus': {'plineMSTrCont': True},
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
                         'PlineMarkers': ['plineMSTrCont'],
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
                         'RulesToClean': {'plineMSTrCont': []},
                         'TargetStatus': None,
                         'TransferDone': False,
                         'TransferDisk': False,
                         'TransferTape': False,
                         'TapeRulesStatus': [],
                         'WmcOutputRulesMap': {},
                         'StatusAdvanceExpiredMsg': ""}
        self.assertDictEqual(wflow, expectedWflow)

    def testPipelineArchive(self):
        # Test plineAgentCont
        wflow = MSRuleCleanerWflow(self.taskChainReq)

        # Try archival of a skipped workflow:
        with self.assertRaises(MSRuleCleanerArchivalSkip):
            self.msRuleCleaner.plineArchive.run(wflow)
        self.msRuleCleaner.plineAgentBlock.run(wflow)
        self.msRuleCleaner.plineAgentCont.run(wflow)

        # Try archival of a cleaned workflow:
        # NOTE: We should always expect an MSRuleCleanerArchivalSkip exception
        #       here because the 'enableRealRunMode' flag is set to False
        with self.assertRaises(MSRuleCleanerArchivalSkip):
            self.msRuleCleaner.plineArchive.run(wflow)
        expectedWflow = {'CleanupStatus': {'plineAgentBlock': True, 'plineAgentCont': True},
                         'ForceArchive': False,
                         'IncludeParents': False,
                         'InputDataset': '/JetHT/Run2012C-v1/RAW',
                         'IsArchivalDelayExpired': True,
                         'IsClean': True,
                         'IsLogDBClean': True,
                         'OutputDatasets': [
                             '/JetHT/CMSSW_7_2_0-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/RECO',
                             '/JetHT/CMSSW_7_2_0-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/DQMIO',
                             '/JetHT/CMSSW_7_2_0-SiStripCalMinBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO',
                             '/JetHT/CMSSW_7_2_0-SiStripCalZeroBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO',
                             '/JetHT/CMSSW_7_2_0-TkAlMinBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO'],
                         'ParentDataset': [],
                         'ParentageResolved': True,
                         'PlineMarkers': ['plineArchive',
                                          'plineAgentBlock',
                                          'plineAgentCont',
                                          'plineArchive'],
                         'RequestName': 'TaskChain_LumiMask_multiRun_HG2011_Val_201029_112735_5891',
                         'RequestStatus': 'announced',
                         'RequestTransition': [{'DN': '', 'Status': 'new', 'UpdateTime': 1606723304},
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
                         'RulesToClean': {'plineAgentBlock': [], 'plineAgentCont': []},
                         'TargetStatus': 'normal-archived',
                         'TransferDone': False,
                         'TransferDisk': False,
                         'TransferTape': False,
                         'TapeRulesStatus': [],
                         'WmcOutputRulesMap': {},
                         'StatusAdvanceExpiredMsg': "Not properly cleaned workflow: TaskChain_LumiMask_multiRun_HG2011_Val_201029_112735_5891"}
        self.assertDictEqual(wflow, expectedWflow)

        # Try archival of an uncleaned workflow
        wflow['CleanupStatus']['plineAgentBlock'] = False
        with self.assertRaises(MSRuleCleanerArchivalSkip):
            self.msRuleCleaner.plineArchive.run(wflow)

    def testPipelineArchiveStepChain(self):
        # Test plineAgentCont
        wflow = MSRuleCleanerWflow(self.stepChainReq)

        # Try archival of a skipped workflow:
        with self.assertRaises(MSRuleCleanerArchivalSkip):
            self.msRuleCleaner.plineArchive.run(wflow)
        self.msRuleCleaner.plineAgentBlock.run(wflow)
        self.msRuleCleaner.plineAgentCont.run(wflow)

        # Try archival of a cleaned workflow:
        # NOTE: We should always expect an MSRuleCleanerArchivalSkip exception
        #       here because the 'enableRealRunMode' flag is set to False
        with self.assertRaises(MSRuleCleanerArchivalSkip):
            self.msRuleCleaner.plineArchive.run(wflow)
        expectedWflow = {'CleanupStatus': {'plineAgentBlock': True, 'plineAgentCont': True},
                         'ForceArchive': False,
                         'IncludeParents': False,
                         'InputDataset': None,
                         'IsArchivalDelayExpired': True,
                         'IsClean': True,
                         'IsLogDBClean': True,
                        'OutputDatasets': [
                            '/DYJetsToLL_Pt-50To100_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/Integ_TestStep1-GENSIM_StepChain_Tasks_HG2011_Val_Todor_v1-v20/GEN-SIM',
                            '/DYJetsToLL_Pt-50To100_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/Integ_TestStep1-GENSIM_StepChain_Tasks_HG2011_Val_Todor_v1-v20/LHE',
                            '/DYJetsToLL_Pt-50To100_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/Integ_TestStep2-DIGI_StepChain_Tasks_HG2011_Val_Todor_v1-v20/GEN-SIM-RAW',
                            '/DYJetsToLL_Pt-50To100_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/Integ_TestStep3-RECO_StepChain_Tasks_HG2011_Val_Todor_v1-v20/AODSIM'],
                         'ParentDataset': [],
                         'ParentageResolved': False,
                         'PlineMarkers': ['plineArchive',
                                          'plineAgentBlock',
                                          'plineAgentCont',
                                          'plineArchive'],
                         'RequestName': 'StepChain_Tasks_HG2011_Val_201029_112731_6371',
                         'RequestStatus': 'aborted-completed',
                         'RequestTransition': [{'DN': '', 'Status': 'new', 'UpdateTime': 1603967251},
                                               {'DN': '', 'Status': 'assignment-approved', 'UpdateTime': 1603967253},
                                               {'DN': '', 'Status': 'assigned', 'UpdateTime': 1603967254},
                                               {'DN': '', 'Status': 'aborted', 'UpdateTime': 1604931587},
                                               {'DN': '', 'Status': 'aborted-completed', 'UpdateTime': 1604931737}],
                         'RequestType': 'StepChain',
                         'SubRequestType': '',
                         'RulesToClean': {'plineAgentBlock': [], 'plineAgentCont': []},
                         'TargetStatus': 'aborted-archived',
                         'TransferDone': False,
                         'TransferDisk': False,
                         'TransferTape': False,
                         'TapeRulesStatus': [],
                         'WmcOutputRulesMap': {},
                         'StatusAdvanceExpiredMsg': ("Not properly cleaned workflow: StepChain_Tasks_HG2011_Val_201029_112731_6371"
                                                     " - 'ParentageResolved' flag set to false.\n"
                                                     "Not properly cleaned workflow: StepChain_Tasks_HG2011_Val_201029_112731_6371\n"
                                                     "Not properly cleaned workflow: StepChain_Tasks_HG2011_Val_201029_112731_6371"
                                                     " - 'ParentageResolved' flag set to false.")}
        self.assertDictEqual(wflow, expectedWflow)

        # Try archival of an uncleaned workflow
        wflow['CleanupStatus']['plineAgentBlock'] = False
        with self.assertRaises(MSRuleCleanerArchivalSkip):
            self.msRuleCleaner.plineArchive.run(wflow)

    def testRunning(self):
        result = self.msRuleCleaner._execute(self.reqRecords)
        self.assertEqual(result, (3, 2, 0, 0))

    def testCheckClean(self):
        # NOTE: All of the bellow checks are well visualized at:
        #       https://github.com/dmwm/WMCore/pull/10023#discussion_r520070925

        # 1. MaskList shorter than FlagList
        wflowFlags = {'CleanupStatus': {'plineAgentBlock': True, 'plineAgentCont': True, 'plineMStrCont': False},
                      'PlineMarkers': ['plineAgentBlock', 'plineAgentCont']}
        self.assertTrue(self.msRuleCleaner._checkClean(wflowFlags))

        wflowFlags = {'CleanupStatus': {'plineAgentBlock': False, 'plineAgentCont': True, 'plineMStrCont': True},
                      'PlineMarkers': ['plineAgentBlock', 'plineAgentCont']}
        self.assertFalse(self.msRuleCleaner._checkClean(wflowFlags))

        # 2. MaskList Empty
        wflowFlags = {'CleanupStatus': {'plineAgentBlock': True, 'plineAgentCont': True},
                      'PlineMarkers': []}
        self.assertFalse(self.msRuleCleaner._checkClean(wflowFlags))

        # 3. MaskList longer than FlagList
        wflowFlags = {'CleanupStatus': {'plineAgentBlock': True, 'plineAgentCont': True},
                      'PlineMarkers': ['plineAgentBlock', 'plineAgentCont', 'plineMStrCont', 'plineArchive']}
        self.assertTrue(self.msRuleCleaner._checkClean(wflowFlags))

        wflowFlags = {'CleanupStatus': {'plineAgentBlock': True, 'plineAgentCont': False},
                      'PlineMarkers': ['plineAgentBlock', 'plineAgentCont', 'plineMStrCont', 'plineArchive']}
        self.assertFalse(self.msRuleCleaner._checkClean(wflowFlags))

        # 4. FlagList Empty
        wflowFlags = {'CleanupStatus': {},
                      'PlineMarkers': ['plineAgentBlock', 'plineAgentCont']}
        self.assertFalse(self.msRuleCleaner._checkClean(wflowFlags))

    # -----------------------------------------------------------------------
    # Tests for cleanOutputBlockRules — block-by-block wma_prod rule expiry
    # -----------------------------------------------------------------------

    def _makeWflow(self, outputDatasets, wmcOutputRulesMap):
        """Build a minimal wflow dict for cleanOutputBlockRules tests."""
        return {
            'OutputDatasets': outputDatasets,
            'WmcOutputRulesMap': wmcOutputRulesMap,
        }

    def _setupRucioMock(self, getOkFilesMap, blocksInContainer, blockFiles, blockRules):
        """
        Configure self.msRuleCleaner.rucio mock for cleanOutputBlockRules.

        :param getOkFilesMap: dict {ruleId: set of OK file names}
        :param blocksInContainer: dict {container: [block names]}
        :param blockFiles: dict {block: [{'name': filename}, ...]}
        :param blockRules: dict {block: [{'id': ruleId}, ...]}
        """
        self.msRuleCleaner._getOkFilesFromRule = lambda ruleId: getOkFilesMap.get(ruleId, set())
        self.msRuleCleaner.rucio.getBlocksInContainer.side_effect = \
            lambda container: blocksInContainer.get(container, [])
        self.msRuleCleaner.rucio.listContent.side_effect = \
            lambda block: blockFiles.get(block, [])
        self.msRuleCleaner.rucio.listDataRules.side_effect = \
            lambda block, account: blockRules.get(block, [])

    def testCleanOutputBlockRulesAllFilesOk(self):
        """
        Block rule is expired when all files are OK in all wmcore_output rules.
        Scenario: disk rule and tape rule both have file1 + file2 OK.
        Block contains exactly file1 + file2 → fully covered → updateRule called.
        """
        self.msRuleCleaner.msConfig['enableRealMode'] = True
        container = '/Dataset/Processed/TIER'
        block = container + '#block1'

        self._setupRucioMock(
            getOkFilesMap={
                'disk-rule-1': {'file1', 'file2'},
                'tape-rule-1': {'file1', 'file2'},
            },
            blocksInContainer={container: [block]},
            blockFiles={block: [{'name': 'file1'}, {'name': 'file2'}]},
            blockRules={block: [{'id': 'wma-block-rule-1'}]},
        )

        wflow = self._makeWflow(
            outputDatasets=[container],
            wmcOutputRulesMap={container: ['disk-rule-1', 'tape-rule-1']},
        )
        self.msRuleCleaner.cleanOutputBlockRules(wflow)

        self.msRuleCleaner.rucio.updateRule.assert_called_once_with(
            'wma-block-rule-1', {'lifetime': 0}
        )

    def testCleanOutputBlockRulesPartialBlockOk(self):
        """
        Block rule is NOT expired when only some files are OK.
        Scenario: block has file1 + file2 + file3, but only file1 + file2 are OK.
        """
        self.msRuleCleaner.msConfig['enableRealMode'] = True
        container = '/Dataset/Processed/TIER'
        block = container + '#block1'

        self._setupRucioMock(
            getOkFilesMap={'disk-rule-1': {'file1', 'file2'}},
            blocksInContainer={container: [block]},
            blockFiles={block: [{'name': 'file1'}, {'name': 'file2'}, {'name': 'file3'}]},
            blockRules={block: [{'id': 'wma-block-rule-1'}]},
        )

        wflow = self._makeWflow(
            outputDatasets=[container],
            wmcOutputRulesMap={container: ['disk-rule-1']},
        )
        self.msRuleCleaner.cleanOutputBlockRules(wflow)

        self.msRuleCleaner.rucio.updateRule.assert_not_called()

    def testCleanOutputBlockRulesNoCommonOkFiles(self):
        """
        Nothing is cleaned when the intersection of OK files across rules is empty.
        Scenario: disk rule has file1 + file2 OK, tape rule has no files OK yet.
        """
        self.msRuleCleaner.msConfig['enableRealMode'] = True
        container = '/Dataset/Processed/TIER'
        block = container + '#block1'

        self._setupRucioMock(
            getOkFilesMap={
                'disk-rule-1': {'file1', 'file2'},
                'tape-rule-1': set(),          # tape not done yet
            },
            blocksInContainer={container: [block]},
            blockFiles={block: [{'name': 'file1'}, {'name': 'file2'}]},
            blockRules={block: [{'id': 'wma-block-rule-1'}]},
        )

        wflow = self._makeWflow(
            outputDatasets=[container],
            wmcOutputRulesMap={container: ['disk-rule-1', 'tape-rule-1']},
        )
        self.msRuleCleaner.cleanOutputBlockRules(wflow)

        self.msRuleCleaner.rucio.updateRule.assert_not_called()

    def testCleanOutputBlockRulesMultipleBlocks(self):
        """
        Only fully-OK blocks are cleaned; partially-OK blocks are skipped.
        Scenario: block1 fully OK → expired. block2 partial → skipped.
        """
        self.msRuleCleaner.msConfig['enableRealMode'] = True
        container = '/Dataset/Processed/TIER'
        block1 = container + '#block1'
        block2 = container + '#block2'

        self._setupRucioMock(
            getOkFilesMap={'disk-rule-1': {'file1', 'file2', 'file3'}},
            blocksInContainer={container: [block1, block2]},
            blockFiles={
                block1: [{'name': 'file1'}, {'name': 'file2'}],       # fully OK
                block2: [{'name': 'file2'}, {'name': 'file3'}, {'name': 'file4'}],  # file4 missing
            },
            blockRules={
                block1: [{'id': 'wma-block-rule-1'}],
                block2: [{'id': 'wma-block-rule-2'}],
            },
        )

        wflow = self._makeWflow(
            outputDatasets=[container],
            wmcOutputRulesMap={container: ['disk-rule-1']},
        )
        self.msRuleCleaner.cleanOutputBlockRules(wflow)

        self.msRuleCleaner.rucio.updateRule.assert_called_once_with(
            'wma-block-rule-1', {'lifetime': 0}
        )

    def testCleanOutputBlockRulesDryRun(self):
        """
        updateRule is NOT called when enableRealMode=False, even if block is fully OK.
        """
        self.msRuleCleaner.msConfig['enableRealMode'] = False
        container = '/Dataset/Processed/TIER'
        block = container + '#block1'

        self._setupRucioMock(
            getOkFilesMap={'disk-rule-1': {'file1', 'file2'}},
            blocksInContainer={container: [block]},
            blockFiles={block: [{'name': 'file1'}, {'name': 'file2'}]},
            blockRules={block: [{'id': 'wma-block-rule-1'}]},
        )

        wflow = self._makeWflow(
            outputDatasets=[container],
            wmcOutputRulesMap={container: ['disk-rule-1']},
        )
        self.msRuleCleaner.cleanOutputBlockRules(wflow)

        self.msRuleCleaner.rucio.updateRule.assert_not_called()

    def testCleanOutputBlockRulesContainerNotInRucio(self):
        """
        WMRucioDIDNotFoundException is caught gracefully — no crash, no updateRule.
        """
        from WMCore.Services.Rucio.Rucio import WMRucioDIDNotFoundException
        self.msRuleCleaner.msConfig['enableRealMode'] = True
        container = '/Dataset/Processed/TIER'

        self.msRuleCleaner._getOkFilesFromRule = lambda ruleId: {'file1'}
        self.msRuleCleaner.rucio.getBlocksInContainer.side_effect = \
            WMRucioDIDNotFoundException("container not found")

        wflow = self._makeWflow(
            outputDatasets=[container],
            wmcOutputRulesMap={container: ['disk-rule-1']},
        )
        self.msRuleCleaner.cleanOutputBlockRules(wflow)  # must not raise

        self.msRuleCleaner.rucio.updateRule.assert_not_called()

    def testCleanOutputBlockRulesNoRulesForContainer(self):
        """
        Container missing from WmcOutputRulesMap is silently skipped.
        """
        self.msRuleCleaner.msConfig['enableRealMode'] = True
        container = '/Dataset/Processed/TIER'

        wflow = self._makeWflow(
            outputDatasets=[container],
            wmcOutputRulesMap={},   # no entry for this container
        )
        self.msRuleCleaner.cleanOutputBlockRules(wflow)  # must not raise

        self.msRuleCleaner.rucio.getBlocksInContainer.assert_not_called()
        self.msRuleCleaner.rucio.updateRule.assert_not_called()


if __name__ == '__main__':
    unittest.main()
