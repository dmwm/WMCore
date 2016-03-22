#!/usr/bin/env python

"""
_StepChain_t_
"""

import json
import os
import unittest
from copy import copy
from WMCore.WMSpec.StdSpecs.StepChain import StepChainWorkloadFactory
from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.Database.CMSCouch import CouchServer, Document
from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMCore.WMSpec.WMSpecErrors import WMSpecFactoryException


def getTestFile(partialPath):
    """
    Returns the absolute path for the test json file
    """
    normPath = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))
    return os.path.join(normPath, partialPath)


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
        self.jsonTemplate = getTestFile('data/ReqMgr/requests/DMWM/StepChain_MC.json')

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
        Build a StepChain single step, reading AODSIM and producing MINIAODSIM
        """
        testArguments = StepChainWorkloadFactory.getTestArguments()
        request = {
            "Campaign": "TaskForceUnitTest",
            "CMSSWVersion": "CMSSW_7_5_0",
            "ScramArch": "slc6_amd64_gcc491",
            "DbsUrl": "https://cmsweb.cern.ch/dbs/prod/global/DBSReader",
            "GlobalTag": "PHYS14_25_V3",
            "AcquisitionEra": "SingleStep",
            "ProcessingString": "UnitTest_StepChain",
            "ProcessingVersion": 3,
            "PrepID": "MainStep",
            "CouchURL": os.environ["COUCHURL"],
            "CouchDBName": "stepchain_t",
            "Memory": 3500,
            "SizePerEvent": 2600,
            "TimePerEvent": 26.5,
            "Step1": {
                "ConfigCacheID": injectStepChainConfigSingle(self.configDatabase),
                "GlobalTag": "PHYS14_25_V44",
                "InputDataset": "/RSGravToGG_kMpl-01_M-5000_TuneCUEP8M1_13TeV-pythia8/RunIISpring15DR74-Asympt50ns_MCRUN2_74_V9A-v1/AODSIM",
                "SplittingAlgo": "EventAwareLumiBased",
                "EventsPerJob": 500,
                "StepName": "StepMini"},
            "StepChain": 1
        }
        testArguments.update(request)

        factory = StepChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        testWMBSHelper = WMBSHelper(testWorkload, "StepMini", "GravWhatever", cachepath=self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        # test workload properties
        self.assertEqual(testWorkload.getDashboardActivity(), "processing")
        self.assertEqual(testWorkload.getCampaign(), "TaskForceUnitTest")
        self.assertEqual(testWorkload.getAcquisitionEra(), "SingleStep")
        self.assertEqual(testWorkload.getProcessingString(), "UnitTest_StepChain")
        self.assertEqual(testWorkload.getProcessingVersion(), 3)
        self.assertEqual(testWorkload.getPrepID(), "MainStep")
        self.assertEqual(sorted(testWorkload.getCMSSWVersions()), ['CMSSW_7_5_0'])
        self.assertEqual(testWorkload.data.policies.start.policyName, "Block")
        # test workload tasks and steps
        tasks = testWorkload.listAllTaskNames()
        self.assertEqual(len(tasks), 4)
        self.assertTrue('StepMiniMergeMINIAODSIMoutput' in tasks)

        task = testWorkload.getTask(tasks[0])
        self.assertEqual(task.taskType(), "Processing", "Wrong task type")
        splitParams = task.jobSplittingParameters()
        self.assertEqual(splitParams['algorithm'], "EventAwareLumiBased", "Wrong job splitting algo")
        self.assertEqual(splitParams['events_per_job'], 500)
        self.assertTrue(splitParams['performance']['timePerEvent'] > 26.4)
        self.assertTrue(splitParams['performance']['sizePerEvent'] > 2599)
        self.assertTrue(splitParams['performance']['memoryRequirement'] == 3500)
        # test workload step stuff
        self.assertEqual(sorted(task.listAllStepNames()), ['cmsRun1', 'logArch1', 'stageOut1'])
        self.assertEqual(task.getTopStepName(), 'cmsRun1')
        self.assertEqual(task.getStep("cmsRun1").stepType(), "CMSSW")
        self.assertFalse(task.getInputStep(), "Wrong input step")
        outModsAndDsets = task.listOutputDatasetsAndModules()[0]
        self.assertEqual(outModsAndDsets['outputModule'], 'MINIAODSIMoutput')
        self.assertEqual(outModsAndDsets['outputDataset'],
                         '/RSGravToGG_kMpl-01_M-5000_TuneCUEP8M1_13TeV-pythia8/SingleStep-UnitTest_StepChain-v3/MINIAODSIM')
        self.assertEqual(task.getSwVersion(), 'CMSSW_7_5_0')
        self.assertEqual(task.getScramArch(), 'slc6_amd64_gcc491')
        step = task.getStep("cmsRun1")
        self.assertEqual(step.data.application.configuration.arguments.globalTag, 'PHYS14_25_V44')

        return

    def testStepChainMC(self):
        """
        Build a StepChain workload starting from scratch
        """
        # Read in the request
        request = json.load(open(self.jsonTemplate))
        testArguments = request['createRequest']
        testArguments.update({
            "CouchURL": os.environ["COUCHURL"],
            "ConfigCacheUrl": os.environ["COUCHURL"],
            "CouchDBName": "stepchain_t"
        })
        configDocs = injectStepChainConfigMC(self.configDatabase)
        for s in ['Step1', 'Step2', 'Step3']:
            testArguments[s]['ConfigCacheID'] = configDocs[s]

        factory = StepChainWorkloadFactory()

        # test that we cannot stage out different samples with the same output module
        self.assertRaises(WMSpecFactoryException, factory.factoryWorkloadConstruction,
                          "TestWorkload", testArguments)

        testArguments['Step2']['KeepOutput'] = False
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        testWMBSHelper = WMBSHelper(testWorkload, "ProdMinBias", "MCFakeBlock", cachepath=self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        # test workload properties
        self.assertEqual(testWorkload.getDashboardActivity(), "production")
        self.assertEqual(testWorkload.getCampaign(), "Campaign-OVERRIDE-ME")
        self.assertEqual(testWorkload.getAcquisitionEra(), "CMSSW_7_0_0_pre11")
        self.assertEqual(testWorkload.getProcessingString(), "START70_V4")
        self.assertEqual(testWorkload.getProcessingVersion(), 1)
        self.assertEqual(testWorkload.getPrepID(), "Step-00")
        self.assertEqual(sorted(testWorkload.getCMSSWVersions()), ['CMSSW_7_0_0_pre11', 'CMSSW_7_0_0_pre12'])

        # test workload attributes
        self.assertEqual(testWorkload.processingString, "START70_V4")
        self.assertEqual(testWorkload.acquisitionEra, "CMSSW_7_0_0_pre11")
        self.assertEqual(testWorkload.processingVersion, 1)
        self.assertFalse(testWorkload.lumiList, "Wrong lumiList")
        self.assertEqual(testWorkload.data.policies.start.policyName, "MonteCarlo")

        # test workload tasks and steps
        tasks = testWorkload.listAllTaskNames()
        self.assertEqual(len(tasks), 10)
        for t in ['ProdMinBias', 'ProdMinBiasMergeRAWSIMoutput',
                  'RECOPROD1MergeAODSIMoutput', 'RECOPROD1MergeRECOSIMoutput']:
            self.assertTrue(t in tasks, "Wrong task name")
        self.assertFalse('ProdMinBiasMergeAODSIMoutput' in tasks, "Wrong task name")

        task = testWorkload.getTask(tasks[0])
        self.assertEqual(task.name(), "ProdMinBias")
        self.assertEqual(task.getPathName(), "/TestWorkload/ProdMinBias")
        self.assertEqual(task.taskType(), "Production", "Wrong task type")

        splitParams = task.jobSplittingParameters()
        self.assertEqual(splitParams['algorithm'], "EventBased", "Wrong job splitting algo")
        self.assertEqual(splitParams['events_per_job'], 150)
        self.assertEqual(splitParams['events_per_lumi'], 50)
        self.assertFalse(splitParams['lheInputFiles'], "Wrong LHE flag")
        self.assertTrue(splitParams['performance']['timePerEvent'] > 4.75)
        self.assertTrue(splitParams['performance']['sizePerEvent'] > 1233)
        self.assertTrue(splitParams['performance']['memoryRequirement'] == 2400)

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
        self.assertTrue('/RelValProdMinBias/CMSSW_7_0_0_pre11-FilterA-START70_V4-v1/GEN-SIM' in outDsets)
        self.assertTrue('/RelValProdMinBias/CMSSW_7_0_0_pre11-FilterD-START70_V4-v1/AODSIM' in outDsets)
        self.assertTrue('/RelValProdMinBias/CMSSW_7_0_0_pre11-FilterC-START70_V4-v1/GEN-SIM-RECO' in outDsets)
        self.assertEqual(task.getSwVersion(), 'CMSSW_7_0_0_pre12')
        self.assertEqual(task.getScramArch(), 'slc5_amd64_gcc481')

        step = task.getStep("cmsRun1")
        self.assertFalse(step.data.tree.parent)
        self.assertFalse(getattr(step.data.input, 'inputStepName', None))
        self.assertFalse(getattr(step.data.input, 'inputOutputModule', None))
        self.assertEqual(step.data.output.modules.RAWSIMoutput.filterName, 'FilterA')
        self.assertEqual(step.data.output.modules.RAWSIMoutput.dataTier, 'GEN-SIM')
        self.assertTrue(step.data.output.keep)
        self.assertEqual(sorted(step.data.tree.childNames), ['cmsRun2', 'logArch1', 'stageOut1'])
        self.assertEqual(step.data.application.setup.cmsswVersion, 'CMSSW_7_0_0_pre12')
        self.assertEqual(step.data.application.setup.scramArch, 'slc5_amd64_gcc481')
        self.assertEqual(step.data.application.configuration.arguments.globalTag, 'START70_V4::All')

        step = task.getStep("cmsRun2")
        self.assertEqual(step.data.tree.parent, "cmsRun1")
        self.assertEqual(step.data.input.inputStepName, 'cmsRun1')
        self.assertEqual(step.data.input.inputOutputModule, 'RAWSIMoutput')
        self.assertEqual(step.data.output.modules.RAWSIMoutput.filterName, 'FilterB')
        self.assertEqual(step.data.output.modules.RAWSIMoutput.dataTier, 'GEN-SIM-RAW')
        self.assertFalse(step.data.output.keep)
        self.assertEqual(step.data.tree.childNames, ["cmsRun3"])
        self.assertEqual(step.data.application.setup.cmsswVersion, 'CMSSW_7_0_0_pre11')
        self.assertEqual(step.data.application.setup.scramArch, 'slc5_amd64_gcc481')
        self.assertEqual(step.data.application.configuration.arguments.globalTag, 'START70_V4::All')

        step = task.getStep("cmsRun3")
        self.assertEqual(step.data.tree.parent, "cmsRun2")
        self.assertEqual(step.data.input.inputStepName, 'cmsRun2')
        self.assertEqual(step.data.input.inputOutputModule, 'RAWSIMoutput')
        self.assertEqual(step.data.output.modules.RECOSIMoutput.filterName, 'FilterC')
        self.assertEqual(step.data.output.modules.AODSIMoutput.filterName, 'FilterD')
        self.assertEqual(step.data.output.modules.RECOSIMoutput.dataTier, 'GEN-SIM-RECO')
        self.assertEqual(step.data.output.modules.AODSIMoutput.dataTier, 'AODSIM')
        self.assertTrue(step.data.output.keep)
        self.assertFalse(step.data.tree.childNames)
        self.assertEqual(step.data.application.setup.cmsswVersion, 'CMSSW_7_0_0_pre11')
        self.assertEqual(step.data.application.setup.scramArch, 'slc5_amd64_gcc481')
        self.assertEqual(step.data.application.configuration.arguments.globalTag, 'START70_V4::All')

        return

    def testStepMapping(self):
        """
        Build a mapping of steps, input and output modules
        """
        factory = StepChainWorkloadFactory()
        request = json.load(open(self.jsonTemplate))
        testArguments = request['createRequest']
        # Create a new DIGI step in Step3 and shift Step3 to Step4
        testArguments['Step4'] = copy(testArguments['Step3'])
        testArguments['Step3'] = {"GlobalTag": "START70_V4::All",
                                  "InputFromOutputModule": "RAWSIMoutput",
                                  "InputStep": "ProdMinBias",
                                  "StepName": "DIGIPROD2"}
        testArguments['StepChain'] = 4
        testArguments.update({"CouchURL": os.environ["COUCHURL"],
                              "ConfigCacheUrl": os.environ["COUCHURL"],
                              "CouchDBName": "stepchain_t"})
        configDocs = injectStepChainConfigMC(self.configDatabase)
        for s in ['Step1', 'Step2', 'Step3', 'Step4']:
            testArguments[s]['ConfigCacheID'] = configDocs[s]
            testArguments[s]['KeepOutput'] = False
        # docs are in the wrong order for this case
        testArguments['Step3']['ConfigCacheID'] = configDocs['Step4']
        testArguments['Step4']['ConfigCacheID'] = configDocs['Step3']

        expectedTasks = set(['ProdMinBias', 'RECOPROD1MergeAODSIMoutput', 'RECOPROD1MergeRECOSIMoutput',
                             'RECOPROD1AODSIMoutputMergeLogCollect', 'RECOPROD1RECOSIMoutputMergeLogCollect',
                             'RECOPROD1CleanupUnmergedAODSIMoutput', 'RECOPROD1CleanupUnmergedRECOSIMoutput'])
        expectedSteps = set(['cmsRun1', 'cmsRun2', 'cmsRun3', 'cmsRun4', 'stageOut1', 'logArch1'])

        self.assertRaises(WMSpecFactoryException, factory.factoryWorkloadConstruction,
                          "TestWorkload", testArguments)

        testArguments['Step4']['KeepOutput'] = True
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)
        self.assertEqual(len(testWorkload.listAllTaskNames()), len(expectedTasks))
        self.assertEqual(set(testWorkload.listAllTaskNames()), expectedTasks)
        task = testWorkload.getTask('ProdMinBias')
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


if __name__ == '__main__':
    unittest.main()
