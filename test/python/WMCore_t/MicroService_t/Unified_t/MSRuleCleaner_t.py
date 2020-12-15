"""
Unit tests for Unified/MSRuleCleaner.py module

"""
from __future__ import division, print_function

import json
# system modules
import os
import unittest


# WMCore modules
from WMCore.MicroService.Unified.MSRuleCleaner import MSRuleCleaner, MSRuleCleanerArchivalError, MSRuleCleanerArchivalSkip
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMCore.Services.Rucio import Rucio
from WMCore.MicroService.DataStructs.MSRuleCleanerWflow import MSRuleCleanerWflow


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
                         "interval": 1 *60,
                         "services": ['ruleCleaner'],
                         "rucioWmaAcct": 'wma_test',
                         "rucioAccount": 'wma_test',
                         'reqmgr2Url': 'https://cmsweb-testbed.cern.ch/reqmgr2',
                         'msOutputUrl': 'https://cmsweb-testbed.cern.ch/ms-output',
                         'reqmgrCacheUrl': 'https://cmsweb-testbed.cern.ch/couchdb/reqmgr_workload_cache',
                         'phedexUrl': 'https://cmsweb-testbed.cern.ch/phedex/datasvc/json/prod',
                         'dbsUrl': 'https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader',
                         'rucioUrl': 'http://cmsrucio-int.cern.ch',
                         'rucioAuthUrl': 'https://cmsrucio-auth-int.cern.ch',
                         "wmstatsUrl": "https://cmsweb-testbed.cern.ch/wmstatsserver",
                         "logDBUrl": "https://cmsweb-testbed.cern.ch/couchdb/wmstats_logdb",
                         'logDBReporter': 'reqmgr2ms_ruleCleaner',
                         'archiveDelayHours': 8,
                         'enableRealMode': False}

        self.creds = {"client_cert": os.getenv("X509_USER_CERT", "Unknown"),
                      "client_key": os.getenv("X509_USER_KEY", "Unknown")}
        self.rucioConfigDict =  {"rucio_host": self.msConfig['rucioUrl'],
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
        with open(self.reqRecordsFile) as fd:
            self.reqRecords = json.load(fd)
        with open(self.taskChainFile) as fd:
            self.taskChainReq = json.load(fd)
        with open(self.stepChainFile) as fd:
            self.stepChainReq = json.load(fd)
        super(MSRuleCleanerTest, self).setUp()

    def testPipelineAgentBlock(self):
        # Test plineAgentBlock:
        wflow = MSRuleCleanerWflow(self.taskChainReq)
        self.msRuleCleaner.plineAgentBlock.run(wflow)
        expectedWflow = {'CleanupStatus': {'plineAgentBlock': True},
                         'ForceArchive': False,
                         'IsArchivalDelayExpired': False,
                         'IsClean': False,
                         'IsLogDBClean': False,
                         'OutputDatasets': [u'/JetHT/CMSSW_7_2_0-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/RECO',
                                            u'/JetHT/CMSSW_7_2_0-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/DQMIO',
                                            u'/JetHT/CMSSW_7_2_0-SiStripCalMinBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO',
                                            u'/JetHT/CMSSW_7_2_0-SiStripCalZeroBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO',
                                            u'/JetHT/CMSSW_7_2_0-TkAlMinBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO'],
                         'ParentageResolved': True,
                         'PlineMarkers': ['plineAgentBlock'],
                         'RequestName': u'TaskChain_LumiMask_multiRun_HG2011_Val_201029_112735_5891',
                         'RequestStatus': u'announced',
                         "RequestTransition": [{"Status": "new",
                                                "DN": "",
                                                "UpdateTime": 1606723304},
                                               {"DN": "",
                                                "Status": "assignment-approved",
                                                "UpdateTime": 1606723305},
                                               {"DN": "",
                                                "Status": "assigned",
                                                "UpdateTime": 1606723306},
                                               {"DN": "",
                                                "Status": "staging",
                                                "UpdateTime": 1606723461},
                                               {"DN": "",
                                                "Status": "staged",
                                                "UpdateTime": 1606723590},
                                               {"DN": "",
                                                "Status": "acquired",
                                                "UpdateTime": 1606723968},
                                               {"DN": "",
                                                "Status": "running-open",
                                                "UpdateTime": 1606724572},
                                               {"DN": "",
                                                "Status": "running-closed",
                                                "UpdateTime": 1606724573},
                                               {"DN": "",
                                                "Status": "completed",
                                                "UpdateTime": 1607018413},
                                               {"DN": "",
                                                "Status": "closed-out",
                                                "UpdateTime": 1607347706},
                                               {"DN": "",
                                                "Status": "announced",
                                                "UpdateTime": 1607359514}],
                         'RequestType': u'TaskChain',
                         'RulesToClean': {'plineAgentBlock': []},
                         'TargetStatus': None,
                         'TransferDone': False}
        self.assertDictEqual(wflow, expectedWflow)

    def testPipelineAgentCont(self):
        # Test plineAgentCont
        wflow = MSRuleCleanerWflow(self.taskChainReq)
        self.msRuleCleaner.plineAgentCont.run(wflow)
        expectedWflow = {'CleanupStatus': {'plineAgentCont': True},
                         'ForceArchive': False,
                         'IsArchivalDelayExpired': False,
                         'IsClean': False,
                         'IsLogDBClean': False,
                         'OutputDatasets': [u'/JetHT/CMSSW_7_2_0-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/RECO',
                                            u'/JetHT/CMSSW_7_2_0-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/DQMIO',
                                            u'/JetHT/CMSSW_7_2_0-SiStripCalMinBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO',
                                            u'/JetHT/CMSSW_7_2_0-SiStripCalZeroBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO',
                                            u'/JetHT/CMSSW_7_2_0-TkAlMinBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO'],
                         'ParentageResolved': True,
                         'PlineMarkers': ['plineAgentCont'],
                         'RequestName': u'TaskChain_LumiMask_multiRun_HG2011_Val_201029_112735_5891',
                         'RequestStatus': u'announced',
                         "RequestTransition": [{"Status": "new",
                                                "DN": "",
                                                "UpdateTime": 1606723304},
                                               {"DN": "",
                                                "Status": "assignment-approved",
                                                "UpdateTime": 1606723305},
                                               {"DN": "",
                                                "Status": "assigned",
                                                "UpdateTime": 1606723306},
                                               {"DN": "",
                                                "Status": "staging",
                                                "UpdateTime": 1606723461},
                                               {"DN": "",
                                                "Status": "staged",
                                                "UpdateTime": 1606723590},
                                               {"DN": "",
                                                "Status": "acquired",
                                                "UpdateTime": 1606723968},
                                               {"DN": "",
                                                "Status": "running-open",
                                                "UpdateTime": 1606724572},
                                               {"DN": "",
                                                "Status": "running-closed",
                                                "UpdateTime": 1606724573},
                                               {"DN": "",
                                                "Status": "completed",
                                                "UpdateTime": 1607018413},
                                               {"DN": "",
                                                "Status": "closed-out",
                                                "UpdateTime": 1607347706},
                                               {"DN": "",
                                                "Status": "announced",
                                                "UpdateTime": 1607359514}],
                         'RequestType': u'TaskChain',
                         'RulesToClean': {'plineAgentCont': []},
                         'TargetStatus': None,
                         'TransferDone': False}
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
                         'IsArchivalDelayExpired': True,
                         'IsClean': True,
                         'IsLogDBClean': True,
                         'OutputDatasets': [u'/JetHT/CMSSW_7_2_0-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/RECO',
                                            u'/JetHT/CMSSW_7_2_0-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/DQMIO',
                                            u'/JetHT/CMSSW_7_2_0-SiStripCalMinBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO',
                                            u'/JetHT/CMSSW_7_2_0-SiStripCalZeroBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO',
                                            u'/JetHT/CMSSW_7_2_0-TkAlMinBias-RECODreHLT_TaskChain_LumiMask_multiRun_HG2011_Val_Todor_v1-v11/ALCARECO'],
                         'ParentageResolved': True,
                         'PlineMarkers': ['plineArchive',
                                          'plineAgentBlock',
                                          'plineAgentCont',
                                          'plineArchive'],
                         'RequestName': u'TaskChain_LumiMask_multiRun_HG2011_Val_201029_112735_5891',
                         'RequestStatus': u'announced',
                         "RequestTransition": [{"Status": "new",
                                                "DN": "",
                                                "UpdateTime": 1606723304},
                                               {"DN": "",
                                                "Status": "assignment-approved",
                                                "UpdateTime": 1606723305},
                                               {"DN": "",
                                                "Status": "assigned",
                                                "UpdateTime": 1606723306},
                                               {"DN": "",
                                                "Status": "staging",
                                                "UpdateTime": 1606723461},
                                               {"DN": "",
                                                "Status": "staged",
                                                "UpdateTime": 1606723590},
                                               {"DN": "",
                                                "Status": "acquired",
                                                "UpdateTime": 1606723968},
                                               {"DN": "",
                                                "Status": "running-open",
                                                "UpdateTime": 1606724572},
                                               {"DN": "",
                                                "Status": "running-closed",
                                                "UpdateTime": 1606724573},
                                               {"DN": "",
                                                "Status": "completed",
                                                "UpdateTime": 1607018413},
                                               {"DN": "",
                                                "Status": "closed-out",
                                                "UpdateTime": 1607347706},
                                               {"DN": "",
                                                "Status": "announced",
                                                "UpdateTime": 1607359514}],
                         'RequestType': u'TaskChain',
                         'RulesToClean': {'plineAgentBlock': [], 'plineAgentCont': []},
                         'TargetStatus': 'normal-archived',
                         'TransferDone': False}
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
