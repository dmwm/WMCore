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
        self.assertTrue(self.msRuleCleaner._isStatusAdvanceExpired(wflow))

    def testPipelineAgentBlock(self):
        # Test plineAgentBlock:
        wflow = MSRuleCleanerWflow(self.taskChainReq)
        self.msRuleCleaner.plineAgentBlock.run(wflow)
        expectedWflow = {'CleanupStatus': {'plineAgentBlock': True},
                         'DataPileup': [],
                         'ForceArchive': False,
                         'IncludeParents': False,
                         'InputDataset': '/JetHT/Run2012C-v1/RAW',
                         'IsArchivalDelayExpired': False,
                         'IsClean': False,
                         'IsLogDBClean': False,
                         'MCPileup': [],
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
                         'TransferTape': False}
        self.assertDictEqual(wflow, expectedWflow)

    def testPipelineAgentCont(self):
        # Test plineAgentCont
        wflow = MSRuleCleanerWflow(self.taskChainReq)
        self.msRuleCleaner.plineAgentCont.run(wflow)
        expectedWflow = {'CleanupStatus': {'plineAgentCont': True},
                         'DataPileup': [],
                         'ForceArchive': False,
                         'IncludeParents': False,
                         'InputDataset': '/JetHT/Run2012C-v1/RAW',
                         'IsArchivalDelayExpired': False,
                         'IsClean': False,
                         'IsLogDBClean': False,
                         'MCPileup': [],
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
                         'TransferTape': False}
        self.assertDictEqual(wflow, expectedWflow)

    def testPipelineMSTrBlock(self):
        # Test plineAgentCont
        wflow = MSRuleCleanerWflow(self.taskChainReq)
        self.msRuleCleaner.plineMSTrBlock.run(wflow)
        expectedWflow = {'CleanupStatus': {'plineMSTrBlock': True},
                         'DataPileup': [],
                         'ForceArchive': False,
                         'IncludeParents': False,
                         'InputDataset': '/JetHT/Run2012C-v1/RAW',
                         'IsArchivalDelayExpired': False,
                         'IsClean': False,
                         'IsLogDBClean': False,
                         'MCPileup': [],
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
                         'TransferTape': False}
        self.assertDictEqual(wflow, expectedWflow)

    def testPipelineMSTrCont(self):
        # Test plineAgentCont
        wflow = MSRuleCleanerWflow(self.taskChainReq)
        self.msRuleCleaner.plineMSTrCont.run(wflow)
        expectedWflow = {'CleanupStatus': {'plineMSTrCont': True},
                         'DataPileup': [],
                         'ForceArchive': False,
                         'IncludeParents': False,
                         'InputDataset': '/JetHT/Run2012C-v1/RAW',
                         'IsArchivalDelayExpired': False,
                         'IsClean': False,
                         'IsLogDBClean': False,
                         'MCPileup': [],
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
                         'TransferTape': False}
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
                         'DataPileup': [],
                         'ForceArchive': False,
                         'IncludeParents': False,
                         'InputDataset': '/JetHT/Run2012C-v1/RAW',
                         'IsArchivalDelayExpired': True,
                         'IsClean': True,
                         'IsLogDBClean': True,
                         'MCPileup': [],
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
                         'TransferTape': False}
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


if __name__ == '__main__':
    unittest.main()
