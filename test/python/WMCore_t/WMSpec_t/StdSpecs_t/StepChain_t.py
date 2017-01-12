#!/usr/bin/env python

"""
_StepChain_t_
"""

import os
import unittest
from copy import deepcopy
from WMCore.WMSpec.StdSpecs.StepChain import StepChainWorkloadFactory
from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.Database.CMSCouch import CouchServer, Document
from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMCore.WMSpec.WMSpecErrors import WMSpecFactoryException

REQUEST = {
    "AcquisitionEra": "AcquisitionEra_StepChain",
    "Campaign": "TaskForceUnitTest",
    "CouchURL": os.environ["COUCHURL"],
    "CouchDBName": "stepchain_t",
    "GlobalTag": "MainGlobalTag",
    "Memory": 3500,
    "PrepID": "PREP-StepChain",
    "PrimaryDataset": "PrimaryDataset-StepChain",
    "ProcessingString": "ProcessingString_StepChain",
    "RequestType": "StepChain",
    "Requestor": "amaltaro",
    "Step1": {
        "CMSSWVersion": "CMSSW_7_1_25_patch2",
        "ConfigCacheID": "OVERRIDE",
        "EventsPerLumi": 100,
        "GlobalTag": "GT-Step1",
        "PrepID": "PREP-Step1",
        "RequestNumEvents": 20000,
        "ScramArch": "slc6_amd64_gcc481",
        "Seeding": "AutomaticSeeding",
        "SplittingAlgo": "EventBased",
        "StepName": "GENSIM"
    },
    "Step2": {
        "CMSSWVersion": "CMSSW_8_0_21",
        "ConfigCacheID": "OVERRIDE",
        "GlobalTag": "GT-Step2",
        "InputFromOutputModule": "RAWSIMoutput",
        "InputStep": "GENSIM",
        "MCPileup": "/MinBias_TuneCUETP8M1_13TeV-pythia8/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM",
        "PrepID": "PREP-Step2",
        "ScramArch": "slc6_amd64_gcc530",
        "SplittingAlgo": "EventAwareLumiBased",
        "StepName": "DIGI"
    },
    "Step3": {
        "CMSSWVersion": "CMSSW_8_0_21",
        "ConfigCacheID": "OVERRIDE",
        "GlobalTag": "GT-Step3",
        "InputFromOutputModule": "PREMIXRAWoutput",
        "InputStep": "DIGI",
        "KeepOutput": True,
        "PrepID": "PREP-Step3",
        "ScramArch": "slc6_amd64_gcc530",
        "SplittingAlgo": "EventAwareLumiBased",
        "StepName": "RECO"
    },
    "StepChain": 3,
    "TimePerEvent": 144
}


def injectStepChainConfigSingle(couchDatabase):
    """
    _injectStepChainConfigSingle_

    Create a single config
    """
    miniConfig = Document()
    miniConfig["info"] = None
    miniConfig["config"] = None
    miniConfig["md5hash"] = "9bdc3d7b2fc90e0f4ca24e270a467ac3"
    miniConfig["pset_hash"] = "7c856ad35f9f544839d8525ca10876a7"
    miniConfig["owner"] = {"group": "DATAOPS", "user": "amaltaro"}
    miniConfig["pset_tweak_details"] = {
        "process": {"outputModules_": ["MINIAODSIMoutput"],
                    "MINIAODSIMoutput": {"dataset": {"filterName": "", "dataTier": "MINIAODSIM"}}
                   }
    }
    result = couchDatabase.commitOne(miniConfig)
    return result[0]["id"]


def injectStepChainConfigMC(couchDatabase):
    """
    _injectStepChainConfigMC_

    Create a few configs in couch for a 3 step MC workflow, basically:
    GEN-SIM -> DIGI -> RECO

    then return a map of config names to IDs
    """
    genConfig = Document()
    genConfig["info"] = None
    genConfig["config"] = None
    genConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e234f"
    genConfig["pset_hash"] = "7c856ad35f9f544839d8525ca10876a7"
    genConfig["owner"] = {"group": "DATAOPS", "user": "amaltaro"}
    genConfig["pset_tweak_details"] = {
        "process": {"outputModules_": ["RAWSIMoutput"],
                    "RAWSIMoutput": {"dataset": {"filterName": "FilterA", "dataTier": "GEN-SIM"}}
                   }
    }

    digiConfig = Document()
    digiConfig["info"] = None
    digiConfig["config"] = None
    digiConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e736f"
    digiConfig["pset_hash"] = "7c856ad35f9f544839d8525ca11765a7"
    digiConfig["owner"] = {"group": "DATAOPS", "user": "amaltaro"}
    digiConfig["pset_tweak_details"] = {
        "process": {"outputModules_": ["RAWSIMoutput"],
                    "RAWSIMoutput": {"dataset": {"filterName": "FilterB", "dataTier": "GEN-SIM-RAW"}}
                   }
    }

    recoConfig = Document()
    recoConfig["info"] = None
    recoConfig["config"] = None
    recoConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5cab278a"
    recoConfig["pset_hash"] = "7c856ad35f9f544839d8524ca53728a6"
    recoConfig["owner"] = {"group": "DATAOPS", "user": "amaltaro"}
    recoConfig["pset_tweak_details"] = {
        "process": {"outputModules_": ["RECOSIMoutput", "AODSIMoutput"],
                    "RECOSIMoutput": {"dataset": {"filterName": "FilterC", "dataTier": "GEN-SIM-RECO"}},
                    "AODSIMoutput": {"dataset": {"filterName": "FilterD", "dataTier": "AODSIM"}}
                   }
    }

    digi2Config = Document()
    digi2Config["info"] = None
    digi2Config["config"] = None
    digi2Config["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e736a"
    digi2Config["pset_hash"] = "7c856ad35f9f544839d8525ca11765aa"
    digi2Config["owner"] = {"group": "DATAOPS", "user": "amaltaro"}
    digi2Config["pset_tweak_details"] = {
        "process": {"outputModules_": ["RAWSIMoutput"],
                    "RAWSIMoutput": {"dataset": {"filterName": "", "dataTier": "GEN-SIM-RAW"}}
                   }
    }

    couchDatabase.queue(genConfig)
    couchDatabase.queue(digiConfig)
    couchDatabase.queue(recoConfig)
    couchDatabase.queue(digi2Config)
    result = couchDatabase.commit()

    docMap = {"Step1": result[0][u'id'],
              "Step2": result[1][u'id'],
              "Step3": result[2][u'id'],
              "Step4": result[3][u'id']}

    return docMap


def getSingleStepOverride():
    " Return StepChain-specific dict for a single step "
    args = {
        "CouchURL": os.environ["COUCHURL"],
        "CouchDBName": "stepchain_t",
        "Step1": {
            "GlobalTag": "PHYS14_25_V44",
            "InputDataset": "/RSGravToGG_kMpl-01_M-5000_TuneCUEP8M1_13TeV-pythia8/RunIISpring15DR74-Asympt50ns_MCRUN2_74_V9A-v1/AODSIM",
            "StepName": "StepOne"},
        "StepChain": 1
    }
    return args


def getThreeStepsOverride():
    " Return StepChain-specific dict for a single step "
    args = {
        "CouchURL": os.environ["COUCHURL"],
        "CouchDBName": "stepchain_t",
        "Step1": {
            "GlobalTag": "PHYS14_25_V44",
            "InputDataset": "/RSGravToGG_kMpl-01_M-5000_TuneCUEP8M1_13TeV-pythia8/RunIISpring15DR74-Asympt50ns_MCRUN2_74_V9A-v1/AODSIM",
            "StepName": "StepOne"},
        "Step2": {
            "GlobalTag": "PHYS14_25_V44",
            "InputFromOutputModule": "RAWSIMoutput",
            "InputStep": "StepOne",
            "StepName": "StepTwo"},
        "Step3": {
            "GlobalTag": "PHYS14_25_V44",
            "InputFromOutputModule": "RAWSIMoutput",
            "InputStep": "StepTwo",
            "StepName": "StepThree"},
        "StepChain": 3
    }
    return args


class StepChainTests(unittest.TestCase):
    """
    _StepChainTests_

    Tests the StepChain spec file
    """

    def setUp(self):
        """
        _setUp_

        Initialize the database and couch.
        """
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("stepchain_t", "ConfigCache")
        self.testInit.setSchema(customModules=["WMCore.WMBS"], useDefault=False)

        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("stepchain_t")
        self.testInit.generateWorkDir()
        self.workload = None

        return

    def tearDown(self):
        """
        _tearDown_

        Clear out the database.
        """
        self.testInit.tearDownCouch()
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()
        return

    def testStepChainSingleStep(self):
        """
        Build a StepChain with a single step and no input dataset
        """
        testArguments = StepChainWorkloadFactory.getTestArguments()
        testArguments.update(REQUEST)
        testArguments.pop("Step2")
        testArguments.pop("Step3")
        testArguments['StepChain'] = 1
        testArguments['Step1']['ConfigCacheID'] = injectStepChainConfigSingle(self.configDatabase)

        factory = StepChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        testWMBSHelper = WMBSHelper(testWorkload, "GENSIM", "GravWhatever", cachepath=self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        # test workload properties
        self.assertEqual(testWorkload.getDashboardActivity(), "production")
        self.assertEqual(testWorkload.getCampaign(), "TaskForceUnitTest")
        self.assertEqual(testWorkload.getAcquisitionEra(), "AcquisitionEra_StepChain")
        self.assertEqual(testWorkload.getProcessingString(), "ProcessingString_StepChain")
        self.assertEqual(testWorkload.getProcessingVersion(), 1)
        self.assertEqual(testWorkload.getPrepID(), "PREP-StepChain")
        self.assertEqual(sorted(testWorkload.getCMSSWVersions()), ['CMSSW_7_1_25_patch2'])
        self.assertEqual(testWorkload.data.policies.start.policyName, "MonteCarlo")
        # test workload tasks and steps
        tasks = testWorkload.listAllTaskNames()
        self.assertEqual(len(tasks), 4)
        self.assertTrue('GENSIMMergeMINIAODSIMoutput' in tasks)

        task = testWorkload.getTask(tasks[0])
        self.assertEqual(task.taskType(), "Production", "Wrong task type")
        splitParams = task.jobSplittingParameters()
        self.assertEqual(splitParams['algorithm'], "EventBased", "Wrong job splitting algo")
        self.assertEqual(splitParams['events_per_job'], 200)
        self.assertEqual(splitParams['events_per_lumi'], 100)
        self.assertTrue(splitParams['performance']['timePerEvent'] >= 144)
        self.assertTrue(splitParams['performance']['sizePerEvent'] >= 512)
        self.assertTrue(splitParams['performance']['memoryRequirement'] == 3500)
        # test workload step stuff
        self.assertEqual(sorted(task.listAllStepNames()), ['cmsRun1', 'logArch1', 'stageOut1'])
        self.assertEqual(task.getTopStepName(), 'cmsRun1')
        self.assertEqual(task.getStep("cmsRun1").stepType(), "CMSSW")
        self.assertFalse(task.getInputStep(), "Wrong input step")
        outModsAndDsets = task.listOutputDatasetsAndModules()[0]
        self.assertEqual(outModsAndDsets['outputModule'], 'MINIAODSIMoutput')
        self.assertEqual(outModsAndDsets['outputDataset'],
                         '/PrimaryDataset-StepChain/AcquisitionEra_StepChain-ProcessingString_StepChain-v1/MINIAODSIM')
        self.assertEqual(task.getSwVersion(), 'CMSSW_7_1_25_patch2')
        self.assertEqual(task.getScramArch(), 'slc6_amd64_gcc481')
        step = task.getStep("cmsRun1")
        self.assertEqual(step.data.application.configuration.arguments.globalTag, 'GT-Step1')

        return

    def testStepChainMC(self):
        """
        Build a StepChain workload starting from scratch
        """
        testArguments = StepChainWorkloadFactory.getTestArguments()
        testArguments.update(REQUEST)

        configDocs = injectStepChainConfigMC(self.configDatabase)
        for s in ['Step1', 'Step2', 'Step3']:
            testArguments[s]['ConfigCacheID'] = configDocs[s]
            testArguments[s]['KeepOutput'] = True

        factory = StepChainWorkloadFactory()

        # test that we cannot stage out different samples with the same output module
        self.assertRaises(WMSpecFactoryException, factory.factoryWorkloadConstruction,
                          "TestWorkload", testArguments)

        testArguments['Step2']['KeepOutput'] = False
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        testWMBSHelper = WMBSHelper(testWorkload, "GENSIM", "MCFakeBlock", cachepath=self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        # test workload properties
        self.assertEqual(testWorkload.getDashboardActivity(), "production")
        self.assertEqual(testWorkload.getCampaign(), "TaskForceUnitTest")
        self.assertEqual(testWorkload.getAcquisitionEra(), "AcquisitionEra_StepChain")
        self.assertEqual(testWorkload.getProcessingString(), "ProcessingString_StepChain")
        self.assertEqual(testWorkload.getProcessingVersion(), 1)
        self.assertEqual(testWorkload.getPrepID(), "PREP-StepChain")
        self.assertEqual(sorted(testWorkload.getCMSSWVersions()), ['CMSSW_7_1_25_patch2', 'CMSSW_8_0_21'])
        self.assertFalse(testWorkload.getLumiList(), "Wrong lumiList")
        self.assertEqual(testWorkload.data.policies.start.policyName, "MonteCarlo")

        # test workload tasks and steps
        tasks = testWorkload.listAllTaskNames()
        self.assertEqual(len(tasks), 10)
        for t in ['GENSIM', 'GENSIMMergeRAWSIMoutput', 'RECOMergeAODSIMoutput', 'RECOMergeRECOSIMoutput',
                  'GENSIMRAWSIMoutputMergeLogCollect', 'RECOAODSIMoutputMergeLogCollect',
                  'RECORECOSIMoutputMergeLogCollect']:
            self.assertTrue(t in tasks, "Wrong task name")
        self.assertFalse('ProdMinBiasMergeAODSIMoutput' in tasks, "Wrong task name")

        task = testWorkload.getTask(tasks[0])
        self.assertEqual(task.name(), "GENSIM")
        self.assertEqual(task.getPathName(), "/TestWorkload/GENSIM")
        self.assertEqual(task.taskType(), "Production", "Wrong task type")

        splitParams = task.jobSplittingParameters()
        self.assertEqual(splitParams['algorithm'], "EventBased", "Wrong job splitting algo")
        self.assertEqual(splitParams['events_per_job'], 200)
        self.assertEqual(splitParams['events_per_lumi'], 100)
        self.assertFalse(splitParams['lheInputFiles'], "Wrong LHE flag")
        self.assertTrue(splitParams['performance']['timePerEvent'] >= 144)
        self.assertTrue(splitParams['performance']['sizePerEvent'] >= 512)
        self.assertTrue(splitParams['performance']['memoryRequirement'] == 3500)

        self.assertFalse(task.getTrustSitelists().get('trustlists'), "Wrong input location flag")
        self.assertFalse(task.inputRunWhitelist(), "Wrong run white list")

        # test workload step stuff
        self.assertEqual(sorted(task.listAllStepNames()), ['cmsRun1', 'cmsRun2', 'cmsRun3', 'logArch1', 'stageOut1'])
        self.assertEqual(task.getTopStepName(), 'cmsRun1')
        self.assertEqual(task.getStep("cmsRun1").stepType(), "CMSSW")
        self.assertFalse(task.getInputStep(), "Wrong input step")
        outModsAndDsets = task.listOutputDatasetsAndModules()
        outMods = set([elem['outputModule'] for elem in outModsAndDsets])
        outDsets = [elem['outputDataset'] for elem in outModsAndDsets]
        self.assertEqual(outMods, set(['RAWSIMoutput', 'AODSIMoutput', 'RECOSIMoutput']), "Wrong output modules")
        self.assertTrue(
            '/PrimaryDataset-StepChain/AcquisitionEra_StepChain-FilterA-ProcessingString_StepChain-v1/GEN-SIM' in outDsets)
        self.assertTrue(
            '/PrimaryDataset-StepChain/AcquisitionEra_StepChain-FilterD-ProcessingString_StepChain-v1/AODSIM' in outDsets)
        self.assertTrue(
            '/PrimaryDataset-StepChain/AcquisitionEra_StepChain-FilterC-ProcessingString_StepChain-v1/GEN-SIM-RECO' in outDsets)
        self.assertEqual(task.getSwVersion(), testArguments['Step1']["CMSSWVersion"])
        self.assertEqual(task.getScramArch(), testArguments['Step1']["ScramArch"])

        step = task.getStep("cmsRun1")
        self.assertFalse(step.data.tree.parent)
        self.assertFalse(getattr(step.data.input, 'inputStepName', None))
        self.assertFalse(getattr(step.data.input, 'inputOutputModule', None))
        self.assertEqual(step.data.output.modules.RAWSIMoutput.filterName, 'FilterA')
        self.assertEqual(step.data.output.modules.RAWSIMoutput.dataTier, 'GEN-SIM')
        self.assertTrue(step.data.output.keep)
        self.assertEqual(sorted(step.data.tree.childNames), ['cmsRun2', 'logArch1', 'stageOut1'])
        self.assertEqual(step.data.application.setup.cmsswVersion, testArguments['Step1']["CMSSWVersion"])
        self.assertEqual(step.data.application.setup.scramArch, testArguments['Step1']["ScramArch"])
        self.assertEqual(step.data.application.configuration.arguments.globalTag, testArguments['Step1']["GlobalTag"])

        step = task.getStep("cmsRun2")
        self.assertEqual(step.data.tree.parent, "cmsRun1")
        self.assertEqual(step.data.input.inputStepName, 'cmsRun1')
        self.assertEqual(step.data.input.inputOutputModule, 'RAWSIMoutput')
        self.assertEqual(step.data.output.modules.RAWSIMoutput.filterName, 'FilterB')
        self.assertEqual(step.data.output.modules.RAWSIMoutput.dataTier, 'GEN-SIM-RAW')
        self.assertFalse(step.data.output.keep)
        self.assertEqual(step.data.tree.childNames, ["cmsRun3"])
        self.assertEqual(step.data.application.setup.cmsswVersion, testArguments['Step2']["CMSSWVersion"])
        self.assertEqual(step.data.application.setup.scramArch, testArguments['Step2']["ScramArch"])
        self.assertEqual(step.data.application.configuration.arguments.globalTag, testArguments['Step2']["GlobalTag"])

        step = task.getStep("cmsRun3")
        self.assertEqual(step.data.tree.parent, "cmsRun2")
        self.assertEqual(step.data.input.inputStepName, 'cmsRun2')
        self.assertEqual(step.data.input.inputOutputModule, 'PREMIXRAWoutput')
        self.assertEqual(step.data.output.modules.RECOSIMoutput.filterName, 'FilterC')
        self.assertEqual(step.data.output.modules.AODSIMoutput.filterName, 'FilterD')
        self.assertEqual(step.data.output.modules.RECOSIMoutput.dataTier, 'GEN-SIM-RECO')
        self.assertEqual(step.data.output.modules.AODSIMoutput.dataTier, 'AODSIM')
        self.assertTrue(step.data.output.keep)
        self.assertFalse(step.data.tree.childNames)
        self.assertEqual(step.data.application.setup.cmsswVersion, testArguments['Step3']["CMSSWVersion"])
        self.assertEqual(step.data.application.setup.scramArch, testArguments['Step3']["ScramArch"])
        self.assertEqual(step.data.application.configuration.arguments.globalTag, testArguments['Step3']["GlobalTag"])

        return

    def testStepChainReDigi(self):
        """
        Build a StepChain workload with input dataset in the first step
        """
        testArguments = StepChainWorkloadFactory.getTestArguments()
        testArguments.update(REQUEST)

        configDocs = injectStepChainConfigMC(self.configDatabase)
        for s in ['Step1', 'Step2', 'Step3']:
            testArguments[s]['ConfigCacheID'] = configDocs[s]

        testArguments['Step1'] = deepcopy(testArguments.pop('Step2'))
        testArguments['Step2'] = deepcopy(testArguments.pop('Step3'))
        testArguments['StepChain'] = 2

        testArguments['Step1'].pop('InputFromOutputModule')
        testArguments['Step1'].pop('InputStep')
        testArguments['Step1'].update({
            'KeepOutput': False,
            'InputDataset': '/BprimeJetToBZ_M800GeV_Tune4C_13TeV-madgraph-tauola/Fall13-POSTLS162_V1-v1/GEN-SIM',
            'BlockWhitelist': ["/BprimeJetToBZ_M800GeV_Tune4C_13TeV-madgraph-tauola/Fall13-POSTLS162_V1-v1/GEN-SIM#3a09df90-5593-11e4-bd05-003048f0e3f4","/BprimeJetToBZ_M800GeV_Tune4C_13TeV-madgraph-tauola/Fall13-POSTLS162_V1-v1/GEN-SIM#3a8b15e6-54e0-11e4-afc7-003048f0e3f4"]
        })
        factory = StepChainWorkloadFactory()

        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        testWMBSHelper = WMBSHelper(testWorkload, "DIGI", "Block", cachepath=self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        # test workload properties
        self.assertEqual(testWorkload.getDashboardActivity(), "processing")
        self.assertEqual(testWorkload.getCampaign(), "TaskForceUnitTest")
        self.assertEqual(testWorkload.getAcquisitionEra(), "AcquisitionEra_StepChain")
        self.assertEqual(testWorkload.getProcessingString(), "ProcessingString_StepChain")
        self.assertEqual(testWorkload.getProcessingVersion(), 1)
        self.assertEqual(testWorkload.getPrepID(), "PREP-StepChain")
        self.assertEqual(sorted(testWorkload.getCMSSWVersions()), ['CMSSW_8_0_21'])
        self.assertFalse(testWorkload.getLumiList(), "Wrong lumiList")
        self.assertEqual(testWorkload.data.policies.start.policyName, "Block")

        # test workload tasks and steps
        tasks = testWorkload.listAllTaskNames()
        self.assertEqual(len(tasks), 7)
        for t in ['DIGI', 'RECOMergeAODSIMoutput', 'RECOMergeRECOSIMoutput',
                  'RECOAODSIMoutputMergeLogCollect', 'RECORECOSIMoutputMergeLogCollect',
                  'RECOCleanupUnmergedAODSIMoutput', 'RECOCleanupUnmergedRECOSIMoutput']:
            self.assertTrue(t in tasks, "Wrong task name")
        self.assertFalse('ProdMinBiasMergeAODSIMoutput' in tasks, "Wrong task name")

        task = testWorkload.getTask(tasks[0])
        self.assertEqual(task.name(), "DIGI")
        self.assertEqual(task.getPathName(), "/TestWorkload/DIGI")
        self.assertEqual(task.taskType(), "Processing", "Wrong task type")

        splitParams = task.jobSplittingParameters()
        self.assertEqual(splitParams['algorithm'], "EventAwareLumiBased", "Wrong job splitting algo")
        self.assertEqual(splitParams['events_per_job'], 200)
        self.assertTrue(splitParams['performance']['timePerEvent'] >= 144)
        self.assertTrue(splitParams['performance']['sizePerEvent'] >= 512)
        self.assertTrue(splitParams['performance']['memoryRequirement'] == 3500)

        self.assertEqual(task.getInputDatasetPath(), '/BprimeJetToBZ_M800GeV_Tune4C_13TeV-madgraph-tauola/Fall13-POSTLS162_V1-v1/GEN-SIM')
        self.assertTrue('/BprimeJetToBZ_M800GeV_Tune4C_13TeV-madgraph-tauola/Fall13-POSTLS162_V1-v1/GEN-SIM#3a09df90-5593-11e4-bd05-003048f0e3f4' in task.inputBlockWhitelist())
        self.assertTrue('/BprimeJetToBZ_M800GeV_Tune4C_13TeV-madgraph-tauola/Fall13-POSTLS162_V1-v1/GEN-SIM#3a8b15e6-54e0-11e4-afc7-003048f0e3f4' in task.inputBlockWhitelist())

        # test workload step stuff
        self.assertEqual(sorted(task.listAllStepNames()), ['cmsRun1', 'cmsRun2', 'logArch1', 'stageOut1'])
        self.assertEqual(task.getTopStepName(), 'cmsRun1')
        self.assertEqual(task.getStep("cmsRun1").stepType(), "CMSSW")
        self.assertFalse(task.getInputStep(), "Wrong input step")
        outModsAndDsets = task.listOutputDatasetsAndModules()
        outMods = set([elem['outputModule'] for elem in outModsAndDsets])
        outDsets = [elem['outputDataset'] for elem in outModsAndDsets]
        self.assertEqual(outMods, set(['AODSIMoutput', 'RECOSIMoutput']), "Wrong output modules")
        self.assertTrue(
            '/PrimaryDataset-StepChain/AcquisitionEra_StepChain-FilterD-ProcessingString_StepChain-v1/AODSIM' in outDsets)
        self.assertTrue(
            '/PrimaryDataset-StepChain/AcquisitionEra_StepChain-FilterC-ProcessingString_StepChain-v1/GEN-SIM-RECO' in outDsets)
        return

    def testStepMapping(self):
        """
        Build a mapping of steps, input and output modules
        """
        testArguments = StepChainWorkloadFactory.getTestArguments()
        testArguments.update(REQUEST)
        testArguments['StepChain'] = 1
        testArguments['Step1']['ConfigCacheID'] = injectStepChainConfigSingle(self.configDatabase)
        testArguments.update({
            "CMSSWVersion": "CMSSW_8_0_17",
            "ScramArch": "slc6_amd64_gcc530",
            "StepChain": 4
        })

        # Create a new DIGI step in Step3 and shift Step3 to Step4
        testArguments['Step4'] = deepcopy(testArguments['Step3'])
        testArguments['Step3'] = {"GlobalTag": "GT-Step3",
                                  "InputFromOutputModule": "RAWSIMoutput",
                                  "InputStep": "GENSIM",
                                  "StepName": "DIGI2"}

        configDocs = injectStepChainConfigMC(self.configDatabase)
        for s in ['Step1', 'Step2', 'Step3', 'Step4']:
            testArguments[s]['ConfigCacheID'] = configDocs[s]
            testArguments[s]['KeepOutput'] = False
        # these are the inverse...
        testArguments['Step3']['ConfigCacheID'] = configDocs['Step4']
        testArguments['Step4']['ConfigCacheID'] = configDocs['Step3']

        expectedTasks = set(['GENSIM', 'RECOMergeAODSIMoutput', 'RECOMergeRECOSIMoutput',
                             'RECOAODSIMoutputMergeLogCollect', 'RECORECOSIMoutputMergeLogCollect',
                             'RECOCleanupUnmergedAODSIMoutput', 'RECOCleanupUnmergedRECOSIMoutput'])
        expectedSteps = set(['cmsRun1', 'cmsRun2', 'cmsRun3', 'cmsRun4', 'stageOut1', 'logArch1'])

        factory = StepChainWorkloadFactory()
        # Raise exception because the last Step cannot have KeepOutput=False
        self.assertRaises(WMSpecFactoryException, factory.factoryWorkloadConstruction,
                          "TestWorkload", testArguments)
        testArguments['Step4']['KeepOutput'] = True

        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)
        self.assertEqual(len(testWorkload.listAllTaskNames()), len(expectedTasks))
        self.assertEqual(set(testWorkload.listAllTaskNames()), expectedTasks)
        task = testWorkload.getTask('GENSIM')
        self.assertEqual(set(task.listAllStepNames()), expectedSteps)

        step1 = task.getStep('cmsRun1')
        stepInputSection = step1.data.input.dictionary_()
        self.assertFalse('inputStepName' in stepInputSection)
        self.assertEqual(set(step1.data.output.modules.dictionary_().keys()), set(['RAWSIMoutput']))
        self.assertEqual(step1.data.output.modules.RAWSIMoutput.dictionary_()['dataTier'], 'GEN-SIM')

        step2 = task.getStep('cmsRun2')
        stepInputSection = step2.data.input.dictionary_()
        self.assertTrue(set(stepInputSection['inputStepName']), 'cmsRun1')
        self.assertTrue(set(stepInputSection['inputOutputModule']), 'RAWSIMoutput')
        self.assertEqual(set(step2.data.output.modules.dictionary_().keys()), set(['RAWSIMoutput']))
        self.assertEqual(step2.data.output.modules.RAWSIMoutput.dictionary_()['dataTier'], 'GEN-SIM-RAW')

        step3 = task.getStep('cmsRun3')
        stepInputSection = step3.data.input.dictionary_()
        self.assertTrue(set(stepInputSection['inputStepName']), 'cmsRun1')
        self.assertTrue(set(stepInputSection['inputOutputModule']), 'RAWSIMoutput')
        self.assertEqual(set(step3.data.output.modules.dictionary_().keys()), set(['RAWSIMoutput']))
        self.assertEqual(step3.data.output.modules.RAWSIMoutput.dictionary_()['dataTier'], 'GEN-SIM-RAW')

        step4 = task.getStep('cmsRun4')
        stepInputSection = step4.data.input.dictionary_()
        self.assertTrue(set(stepInputSection['inputStepName']), 'cmsRun2')
        self.assertTrue(set(stepInputSection['inputOutputModule']), 'RAWSIMoutput')
        self.assertEqual(set(step4.data.output.modules.dictionary_().keys()), set(['AODSIMoutput', 'RECOSIMoutput']))
        self.assertEqual(step4.data.output.modules.AODSIMoutput.dictionary_()['dataTier'], 'AODSIM')

    def test1StepMemCoresSettings(self):
        """
        _test1StepMemCoresSettings_

        Make sure the multicore and memory setings are properly propagated to
        all steps. Single step in a task.
        """
        testArguments = StepChainWorkloadFactory.getTestArguments()
        testArguments.update(getSingleStepOverride())
        testArguments['Step1']['ConfigCacheID'] = injectStepChainConfigSingle(self.configDatabase),
        if isinstance(testArguments['Step1']['ConfigCacheID'], tuple):
            testArguments['Step1']['ConfigCacheID'] = testArguments['Step1']['ConfigCacheID'][0]

        factory = StepChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        prodTask = testWorkload.getTask('StepOne')
        for step in ('cmsRun1', 'stageOut1', 'logArch1'):
            stepHelper = prodTask.getStepHelper(step)
            self.assertEqual(stepHelper.getNumberOfCores(), 1)
        # then test Memory requirements
        perfParams = prodTask.jobSplittingParameters()['performance']
        self.assertEqual(perfParams['memoryRequirement'], 2300.0)

        testArguments["Multicore"] = 6
        testArguments["Memory"] = 4600.0
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload2", testArguments)
        prodTask = testWorkload.getTask('StepOne')
        for step in ('cmsRun1', 'stageOut1', 'logArch1'):
            stepHelper = prodTask.getStepHelper(step)
            if step in ('stageOut1', 'logArch1'):
                self.assertEqual(stepHelper.getNumberOfCores(), 1, "%s should have 1 core" % step)
            else:
                self.assertEqual(stepHelper.getNumberOfCores(), testArguments["Multicore"])
        perfParams = prodTask.jobSplittingParameters()['performance']
        self.assertEqual(perfParams['memoryRequirement'], testArguments["Memory"])

        return

    def test3StepsMemCoresSettingsA(self):
        """
        _test3StepsMemCoresSettingsA_

        Make sure the multicore and memory setings are properly propagated to
        all steps. Three steps in the task.
        """
        testArguments = StepChainWorkloadFactory.getTestArguments()
        configDocs = injectStepChainConfigMC(self.configDatabase)
        testArguments.update(getThreeStepsOverride())
        for s in ['Step1', 'Step2', 'Step3']:
            testArguments[s]['ConfigCacheID'] = configDocs[s]
        testArguments['Step2']['KeepOutput'] = False

        factory = StepChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        prodTask = testWorkload.getTask('StepOne')
        for step in ('cmsRun1', 'cmsRun2', 'cmsRun3', 'stageOut1', 'logArch1'):
            stepHelper = prodTask.getStepHelper(step)
            self.assertEqual(stepHelper.getNumberOfCores(), 1)
        # then test Memory requirements
        perfParams = prodTask.jobSplittingParameters()['performance']
        self.assertEqual(perfParams['memoryRequirement'], 2300.0)

        # Test Multicore/Memory settings at TOP level **only**
        testArguments["Multicore"] = 6
        testArguments["Memory"] = 4600.0
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload2", testArguments)
        prodTask = testWorkload.getTask('StepOne')
        for step in ('cmsRun1', 'cmsRun2', 'cmsRun3', 'stageOut1', 'logArch1'):
            stepHelper = prodTask.getStepHelper(step)
            if step in ('stageOut1', 'logArch1'):
                self.assertEqual(stepHelper.getNumberOfCores(), 1, "%s should have 1 core" % step)
            else:
                self.assertEqual(stepHelper.getNumberOfCores(), testArguments["Multicore"])
        perfParams = prodTask.jobSplittingParameters()['performance']
        self.assertEqual(perfParams['memoryRequirement'], testArguments["Memory"])

    def test3StepsMemCoresSettingsB(self):
        """
        _test3StepsMemCoresSettingsB_

        Mix Multicore settings at both step and request level and make sure they
        are properly propagated to each step. Three steps in the task.
        """
        testArguments = StepChainWorkloadFactory.getTestArguments()
        configDocs = injectStepChainConfigMC(self.configDatabase)
        testArguments.update(getThreeStepsOverride())
        for s in ['Step1', 'Step2', 'Step3']:
            testArguments[s]['ConfigCacheID'] = configDocs[s]
        testArguments['Step2']['KeepOutput'] = False

        # Test Multicore/Memory settings at step level **only**
        testArguments['Step1']["Multicore"] = 2
        testArguments['Step3']["Multicore"] = 4
        testArguments['Step1']["Memory"] = 2200.0
        testArguments['Step3']["Memory"] = 4400.0
        factory = StepChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        prodTask = testWorkload.getTask('StepOne')
        for step in ('cmsRun2', 'stageOut1', 'logArch1'):
            stepHelper = prodTask.getStepHelper(step)
            self.assertEqual(stepHelper.getNumberOfCores(), 1, "%s should have 1 core" % step)
        self.assertEqual(prodTask.getStepHelper('cmsRun1').getNumberOfCores(), testArguments['Step1']["Multicore"])
        self.assertEqual(prodTask.getStepHelper('cmsRun3').getNumberOfCores(), testArguments['Step3']["Multicore"])

        perfParams = prodTask.jobSplittingParameters()['performance']
        self.assertEqual(perfParams['memoryRequirement'], testArguments["Memory"])

        # Test mix of Multicore/Memory settings at both top and step level
        testArguments["Multicore"] = 3
        testArguments['Step1']["Multicore"] = 2
        testArguments['Step3']["Multicore"] = 4
        testArguments["Memory"] = 3300.0
        testArguments['Step1']["Memory"] = 2200.0
        testArguments['Step3']["Memory"] = 4400.0
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload2", testArguments)
        prodTask = testWorkload.getTask('StepOne')
        for step in ('stageOut1', 'logArch1'):
            stepHelper = prodTask.getStepHelper(step)
            self.assertEqual(stepHelper.getNumberOfCores(), 1, "%s should have 1 core" % step)
        self.assertEqual(prodTask.getStepHelper('cmsRun1').getNumberOfCores(), testArguments['Step1']["Multicore"])
        self.assertEqual(prodTask.getStepHelper('cmsRun2').getNumberOfCores(), testArguments["Multicore"])
        self.assertEqual(prodTask.getStepHelper('cmsRun3').getNumberOfCores(), testArguments['Step3']["Multicore"])

        perfParams = prodTask.jobSplittingParameters()['performance']
        self.assertEqual(perfParams['memoryRequirement'], testArguments["Memory"])

        return


if __name__ == '__main__':
    unittest.main()
