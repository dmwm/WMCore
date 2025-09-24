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
from rucio.common.exception import RuleNotFound

from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase

from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMQuality.Emulators.WMSpecGenerator.WMSpecGenerator import WMSpecGenerator
from WMCore.WorkQueue.WorkQueue import globalQueue
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue as WorkQueueDS


from WMCore.WMSpec.StdSpecs.ReReco  import ReRecoWorkloadFactory

def getTestFile(partialPath):
    """
    Returns the absolute path for the test json file
    """
    normPath = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))
    return os.path.join(normPath, partialPath)


class MSRuleCleanerTest(EmulatedUnitTestCase):
#class MSRuleCleanerTest(unittest.TestCase):
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
        
        self.specGenerator = WMSpecGenerator("WMSpecs")
        self.schema = []
        self.couchApps = ["WorkQueue"]
        self.testInit = TestInitCouchApp('WorkQueueServiceTest')
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules=self.schema,
                                useDefault=False)
        self.testInit.setupCouch('workqueue_t', *self.couchApps)
        self.testInit.setupCouch('workqueue_t_inbox', *self.couchApps)
        self.testInit.setupCouch('local_workqueue_t', *self.couchApps)
        self.testInit.setupCouch('local_workqueue_t_inbox', *self.couchApps)
        self.testInit.generateWorkDir()

        self.msConfig.update({'QueueURL':self.testInit.couchUrl})
        print("msConfig: ", json.dumps(self.msConfig, indent=2))
        
        
        self.msRuleCleaner = MSRuleCleaner(self.msConfig)
        self.msRuleCleaner.resetCounters()
        self.msRuleCleaner.rucio = Rucio.Rucio(self.msConfig['rucioAccount'],
                                               hostUrl=self.rucioConfigDict['rucio_host'],
                                               authUrl=self.rucioConfigDict['auth_host'],
                                               configDict=self.rucioConfigDict)
        
        

        self.queueParams = {}
        self.queueParams['log_reporter'] = "Services_WorkQueue_Unittest"
        self.queueParams['rucioAccount'] = "wma_test"
        self.queueParams['rucioAuthUrl'] = "http://cms-rucio-int.cern.ch"
        self.queueParams['rucioUrl'] = "https://cms-rucio-auth-int.cern.ch"

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
                         'TransferTape': False,
                         'TapeRulesStatus': [],
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
                         'TransferTape': False,
                         'TapeRulesStatus': [],
                         'StatusAdvanceExpiredMsg': ""}
        self.assertDictEqual(wflow, expectedWflow)

    def testPipelineMSTrBlock(self):
        # Test plineAgentCont
        wflow = MSRuleCleanerWflow(self.taskChainReq)
        print(wflow)
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
                         'TransferTape': False,
                         'TapeRulesStatus': [],
                         'StatusAdvanceExpiredMsg': ""}
        
        self.assertDictEqual(wflow, expectedWflow)
     
    def testPipelineMSTrBlockGlobalQueue(self):
        
        #turn of tests of pipelineMSTrBlockGlobalQueue
        self.skipTest("Skipping testPipelineMSTrBlockGlobalQueue. This is placeholder for future test of pipelineMSTrBlockGlobalQueue if needed")

        #Get workflow description. ReRecoWorkloadFactory.getTestArguments() is used in createReRecoSpec below, 
        #so the workflow description here and the one used in creating workqueue is the same
        specName = "RerecoSpec"
        inputdataset = {"InputDataset": "/JetHT/Run2012C-v1/RAW"}
        workflowDescription = ReRecoWorkloadFactory.getTestArguments()
        workflowDescription['RequestName'] = specName
        workflowDescription['InputDataset'] = inputdataset["InputDataset"]
        
        wflow = MSRuleCleanerWflow(workflowDescription)
        
        #Create ReRecoSpec as stored in GlobalQueue       
        specUrl = self.specGenerator.createReRecoSpec(specName, "file",
                                                      assignKwargs={'SiteWhitelist':["T2_XX_SiteA"]},InputDataset=inputdataset["InputDataset"])
        #Make GlobalQueue
        globalQ = globalQueue(DbName='workqueue_t',
                              QueueURL=self.testInit.couchUrl,
                              UnittestFlag=True, **self.queueParams)
        globalQ.queueWork(specUrl, specName, "teamA")
        
        #Let try to modify the element in GlobalQueue to have PercentComplete and PercentSuccess set to 100
        wqService = WorkQueueDS(self.testInit.couchUrl, 'workqueue_t')
        #Use this instead of wqService.getWQElementsByWorkflow(workflowName) to have the element'id'
        data = wqService.db.loadView('WorkQueue', 'elementsDetailByWorkflowAndStatus',
                                 {'startkey': [specName], 'endkey': [specName, {}],
                                  'reduce': False})
        
        print("Elements in GlobalQueue:")
        elements = data.get('rows', [])
        print(json.dumps(elements, indent=2))
            
        #let update the PercentComplete and PercentSuccess of the first elements
        element_id = [elements[0]['id']]  # Get the first element's ID
        print("Updating element:", element_id)
        wqService.updateElements(*element_id, PercentComplete=100, PercentSuccess=100)
        # Re-fetch the elements to see the update
        data = wqService.db.loadView('WorkQueue', 'elementsDetailByWorkflowAndStatus',
                                 {'startkey': [specName], 'endkey': [specName, {}],
                                  'reduce': False})
        elements = data.get('rows', [])
        #elements=wqService.getWQElementsByWorkflow(specName)
        print("Updated Elements in GlobalQueue:")
        for e in elements:
            print(e["id"], e['value']['Status'], e['value']["PercentComplete"], e['value']["PercentSuccess"])
            #print(e["id"], e['Status'], e["PercentComplete"], e["PercentSuccess"])
        
        #now let try to create Rucio rule for the block
        #create a rule and inject it in wma_test account
        blockNames = list(elements[0]['value']['Inputs'].keys())  # Get the block name from the first element
        rule_id = self.msRuleCleaner.rucio.createReplicationRule(
            names=blockNames[0],
            rseExpression="T2_US_Nebraska",
            copies=1,
            grouping="DATASET",
            lifetime=360,
            account="wma_test",
            ask_approval=False,
            activity="Production Input",
            comment="WMCore test block rule creation"
        )

        print("Created Rucio rule with ID:", rule_id)
        rule_info = self.msRuleCleaner.rucio.getRule(rule_id[0])
        print(rule_info)
        
        self.msRuleCleaner.plineMSTrBlockGlobalQueue.run(wflow)
        print("Workflow after plineMSTrBlockGlobalQueue:")
        print(json.dumps(wflow, indent=2))
        
        #now make sure the rule is cleaned
        try:
            rule_info = self.msRuleCleaner.rucio.getRule(rule_id[0])
            #print("Rule exists:", json.dumps(rule_info, indent=2))
            #now delete it
            self.msRuleCleaner.rucio.deleteRule(rule_id[0])
            print("Deleted Rucio rule with ID:", rule_id)
        except RuleNotFound:
            print("Rule not found.")
        except Exception as e:
            print("Error checking rule:", e)

        print("Cleanup status: ", wflow['CleanupStatus']['plineMSTrBlockGlobalQueue'])
        print("Rules to clean: ", wflow['RulesToClean']['plineMSTrBlockGlobalQueue'], rule_id)
        assert((wflow['CleanupStatus']['plineMSTrBlockGlobalQueue'] is True))
        assert((wflow['RulesToClean']['plineMSTrBlockGlobalQueue'] == rule_id))

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
                         'TransferTape': False,
                         'TapeRulesStatus': [],
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
                         'TransferTape': False,
                         'TapeRulesStatus': [],
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
                         'TransferTape': False,
                         'TapeRulesStatus': [],
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


if __name__ == '__main__':
    unittest.main()
