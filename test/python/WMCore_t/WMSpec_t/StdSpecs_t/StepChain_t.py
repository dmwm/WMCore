#!/usr/bin/env python

"""
_StepChain_t_
"""
from __future__ import print_function

from future.utils import viewitems, listvalues

from builtins import range
from builtins import str, bytes


import os
import threading
import unittest
from copy import deepcopy
from hashlib import md5

from Utils.PythonVersion import PY3
from Utils.Utilities import encodeUnicodeToBytesConditional

from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.Mask import Mask
from WMCore.Database.CMSCouch import CouchServer, Document
from WMCore.WMSpec.StdSpecs.StepChain import StepChainWorkloadFactory
from WMCore.WMSpec.WMSpecErrors import WMSpecFactoryException
from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMQuality.TestInitCouchApp import TestInitCouchApp

REQUEST = {
    "AcquisitionEra": "AcquisitionEra_StepChain",
    "Campaign": "TaskForceUnitTest",
    "ConfigCacheUrl": os.environ["COUCHURL"],
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
        "MCPileup": "/GammaGammaToEE_Elastic_Pt15_8TeV-lpair/Summer12-START53_V7C-v1/GEN-SIM",
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
    genConfig["owner"] = {"group": "DATAOPS", "user": "amaltaro"}
    genConfig["pset_tweak_details"] = {
        "process": {"outputModules_": ["RAWSIMoutput"],
                    "RAWSIMoutput": {"dataset": {"filterName": "FilterA", "dataTier": "GEN-SIM"}}
                    }
    }

    digiConfig = Document()
    digiConfig["owner"] = {"group": "DATAOPS", "user": "amaltaro"}
    digiConfig["pset_tweak_details"] = {
        "process": {"outputModules_": ["RAWSIMoutput"],
                    "RAWSIMoutput": {"dataset": {"filterName": "FilterB", "dataTier": "GEN-SIM-RAW"}}
                    }
    }

    recoConfig = Document()
    recoConfig["owner"] = {"group": "DATAOPS", "user": "amaltaro"}
    recoConfig["pset_tweak_details"] = {
        "process": {"outputModules_": ["RECOSIMoutput", "AODSIMoutput", "DQMoutput"],
                    "RECOSIMoutput": {"dataset": {"filterName": "FilterC", "dataTier": "GEN-SIM-RECO"}},
                    "AODSIMoutput": {"dataset": {"filterName": "FilterD", "dataTier": "AODSIM"}},
                    "DQMoutput": {"dataset": {"filterName": "", "dataTier": "DQMIO"}}
                    }
    }

    digi2Config = Document()
    digi2Config["owner"] = {"group": "DATAOPS", "user": "amaltaro"}
    digi2Config["pset_tweak_details"] = {
        "process": {"outputModules_": ["RAWSIMoutput"],
                    "RAWSIMoutput": {"dataset": {"filterName": "", "dataTier": "GEN-SIM-RAW"}}
                    }
    }

    harvestConfig = Document()
    harvestConfig["owner"] = {"user": "amaltaro", "group": "DATAOPS"}
    harvestConfig["pset_tweak_details"] = {
        "process": {"outputModules_": []}
    }
    couchDatabase.queue(genConfig)
    couchDatabase.queue(digiConfig)
    couchDatabase.queue(recoConfig)
    couchDatabase.queue(digi2Config)
    couchDatabase.queue(harvestConfig)
    result = couchDatabase.commit()

    docMap = {"Step1": result[0][u'id'],
              "Step2": result[1][u'id'],
              "Step3": result[2][u'id'],
              "Step4": result[3][u'id'],
              "Harvest": result[4][u'id']}

    return docMap


def getSingleStepOverride():
    " Return StepChain-specific dict for a single step "
    args = {
        "ConfigCacheUrl": os.environ["COUCHURL"],
        "CouchDBName": "stepchain_t",
        "Step1": {
            "GlobalTag": "PHYS14_25_V44",
            "InputDataset": "/BprimeJetToBZ_M800GeV_Tune4C_13TeV-madgraph-tauola/Fall13-POSTLS162_V1-v1/GEN-SIM",
            "StepName": "StepOne"},
        "StepChain": 1
    }
    return args


def getThreeStepsOverride():
    " Return StepChain-specific dict for a single step "
    args = {
        "ConfigCacheUrl": os.environ["COUCHURL"],
        "CouchDBName": "stepchain_t",
        "Step1": {
            "GlobalTag": "PHYS14_25_V44",
            "InputDataset": "/BprimeJetToBZ_M800GeV_Tune4C_13TeV-madgraph-tauola/Fall13-POSTLS162_V1-v1/GEN-SIM",
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


class StepChainTests(EmulatedUnitTestCase):
    """
    _StepChainTests_

    Tests the StepChain spec file
    """

    def setUp(self):
        """
        _setUp_

        Initialize the database and couch.
        """
        super(StepChainTests, self).setUp()
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection(destroyAllDatabase=True)
        self.testInit.setupCouch("stepchain_t", "ConfigCache")
        self.testInit.setSchema(customModules=["WMCore.WMBS"], useDefault=False)

        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("stepchain_t")
        self.testInit.generateWorkDir()
        self.workload = None

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)
        self.listTasksByWorkflow = self.daoFactory(classname="Workflow.LoadFromName")
        self.listFilesets = self.daoFactory(classname="Fileset.List")
        self.listSubsMapping = self.daoFactory(classname="Subscriptions.ListSubsAndFilesetsFromWorkflow")
        if PY3:
            self.assertItemsEqual = self.assertCountEqual
        return

    def tearDown(self):
        """
        _tearDown_

        Clear out the database.
        """
        self.testInit.tearDownCouch()
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()
        super(StepChainTests, self).tearDown()
        return

    def testStepChainSingleStep(self):
        """
        Build a StepChain with a single step and no input dataset
        """
        testArguments = StepChainWorkloadFactory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))
        testArguments.pop("Step2")
        testArguments.pop("Step3")
        testArguments['StepChain'] = 1
        testArguments['Step1']['ConfigCacheID'] = injectStepChainConfigSingle(self.configDatabase)
        factory = StepChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        outMods = {'MINIAODSIMoutput': dict(dataTier='MINIAODSIM', filterName='', transient=True,
                                            primaryDataset=testArguments['PrimaryDataset'],
                                            processedDataset="AcquisitionEra_StepChain-ProcessingString_StepChain-v1",
                                            lfnBase='/store/unmerged/AcquisitionEra_StepChain/PrimaryDataset-StepChain/MINIAODSIM/ProcessingString_StepChain-v1',
                                            mergedLFNBase='/store/data/AcquisitionEra_StepChain/PrimaryDataset-StepChain/MINIAODSIM/ProcessingString_StepChain-v1', )}
        outDsets = ['/PrimaryDataset-StepChain/AcquisitionEra_StepChain-ProcessingString_StepChain-v1/MINIAODSIM']

        # workload level check
        self.assertEqual(testWorkload.getRequestType(), testArguments['RequestType'])
        self.assertEqual(testWorkload.getDashboardActivity(), "production")
        self.assertEqual(testWorkload.getCampaign(), testArguments['Campaign'])
        self.assertEqual(testWorkload.getAcquisitionEra(), testArguments['AcquisitionEra'])
        self.assertEqual(testWorkload.getProcessingString(), testArguments['ProcessingString'])
        self.assertEqual(testWorkload.getProcessingVersion(), testArguments.get('ProcessingVersion', 1))
        self.assertEqual(testWorkload.getPrepID(), testArguments['PrepID'])
        self.assertItemsEqual(testWorkload.getCMSSWVersions(), [testArguments['Step1']['CMSSWVersion']])
        self.assertEqual(testWorkload.getLumiList(), {})
        self.assertFalse(testWorkload.getAllowOpportunistic())
        self.assertEqual(testWorkload.getUnmergedLFNBase(), '/store/unmerged')
        self.assertEqual(testWorkload.getMergedLFNBase(), '/store/data')
        self.assertEqual(testWorkload.listInputDatasets(), [])

        tasksProducingOutput = ['/TestWorkload/GENSIM', '/TestWorkload/GENSIM/GENSIMMergeMINIAODSIMoutput']
        self.assertItemsEqual(testWorkload.listOutputProducingTasks(), tasksProducingOutput)

        # workqueue start policy check
        self.assertEqual(testWorkload.startPolicy(), "MonteCarlo")
        workqueueSplit = {'SliceSize': 200, 'SliceType': 'NumberOfEvents', 'SplittingAlgo': 'EventBased',
                          'SubSliceSize': 100, 'SubSliceType': 'NumberOfEventsPerLumi',
                          'policyName': 'MonteCarlo', 'OpenRunningTimeout': 0}
        self.assertDictEqual(testWorkload.startPolicyParameters(), workqueueSplit)
        # workload tasks check
        tasks = ['GENSIM', 'GENSIMMergeMINIAODSIMoutput',
                 'GENSIMMINIAODSIMoutputMergeLogCollect', 'GENSIMCleanupUnmergedMINIAODSIMoutput']
        self.assertItemsEqual(testWorkload.listAllTaskNames(), tasks)

        # workload splitting settings check
        splitArgs = testWorkload.listJobSplittingParametersByTask()
        step1Splitting = splitArgs['/TestWorkload/GENSIM']
        self.assertEqual(step1Splitting['type'], 'Production')
        self.assertEqual(step1Splitting['algorithm'], 'EventBased')
        self.assertEqual(step1Splitting['events_per_job'], 200)
        self.assertEqual(step1Splitting['events_per_lumi'], 100)
        self.assertFalse(step1Splitting['lheInputFiles'])
        self.assertFalse(step1Splitting['trustSitelists'])
        self.assertFalse(step1Splitting['trustPUSitelists'])
        self.assertDictEqual(step1Splitting['performance'], {'memoryRequirement': 3500.0,
                                                             'sizePerEvent': 512.0,
                                                             'timePerEvent': 144.0})

        # task level checks
        task = testWorkload.getTaskByName(testArguments['Step1']['StepName'])
        self.assertEqual(task.getPrepID(), testArguments['Step1'].get('PrepID', testArguments.get('PrepID')))
        self.assertEqual(task.getSwVersion(), testArguments['Step1']['CMSSWVersion'])
        self.assertItemsEqual(task.getScramArch(), testArguments['Step1']['ScramArch'])
        self.assertItemsEqual(task.listAllStepNames(), ['cmsRun1', 'logArch1', 'stageOut1'])
        self.assertEqual(task.getTopStepName(), 'cmsRun1')
        self.assertFalse(task.getInputStep(), "Wrong input step")
        # task level checks for output data
        outModDict = task.getOutputModulesForTask(cmsRunOnly=True)[0].dictionary_()  # only 1 cmsRun process
        self.assertItemsEqual(list(outModDict), list(outMods))
        for modName in outModDict:
            self._validateOutputModule(modName, outModDict[modName], outMods[modName])

        # step level checks
        step = task.getStepHelper(task.getTopStepName())
        self.assertEqual(task.getTopStepName(), 'cmsRun1')
        self.assertEqual(step.stepType(), "CMSSW")
        self.assertEqual(step.getCMSSWVersion(), testArguments['Step1']['CMSSWVersion'])
        self.assertItemsEqual(step.getScramArch(), testArguments['Step1']['ScramArch'])
        self.assertEqual(step.getGlobalTag(), testArguments['Step1'].get('GlobalTag', testArguments.get('GlobalTag')))

        outputDsets = [x['outputDataset'] for x in task.listOutputDatasetsAndModules()]
        self.assertItemsEqual(outputDsets, outDsets)

        print(testArguments)
        # test assignment with wrong Trust flags
        assignDict = {"SiteWhitelist": ["T2_US_Nebraska"], "Team": "The-A-Team",
                      "RequestStatus": "assigned",
                      "TrustSitelists": False, "TrustPUSitelists": True
                      }
        with self.assertRaises(RuntimeError):
            testWorkload.updateArguments(assignDict)
        # now with correct flags
        assignDict['TrustPUSitelists'] = False
        testWorkload.updateArguments(assignDict)

    def testStepChainMC(self):
        """
        Build a StepChain workload starting from scratch
        """
        testArguments = StepChainWorkloadFactory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))

        configDocs = injectStepChainConfigMC(self.configDatabase)
        for s in ['Step1', 'Step2', 'Step3']:
            testArguments[s]['ConfigCacheID'] = configDocs[s]
        testArguments['Step2']['KeepOutput'] = False

        factory = StepChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        # workload level check
        self.assertEqual(testWorkload.getRequestType(), testArguments['RequestType'])
        self.assertEqual(testWorkload.getDashboardActivity(), "production")
        self.assertEqual(testWorkload.getCampaign(), testArguments['Campaign'])
        self.assertEqual(testWorkload.getAcquisitionEra(), testArguments['AcquisitionEra'])
        self.assertEqual(testWorkload.getProcessingString(), testArguments['ProcessingString'])
        self.assertEqual(testWorkload.getProcessingVersion(), testArguments.get('ProcessingVersion', 1))
        self.assertEqual(testWorkload.getPrepID(), testArguments['PrepID'])
        self.assertItemsEqual(testWorkload.getCMSSWVersions(), ['CMSSW_7_1_25_patch2', 'CMSSW_8_0_21'])
        self.assertEqual(testWorkload.getLumiList(), {})
        self.assertFalse(testWorkload.getAllowOpportunistic())
        self.assertEqual(testWorkload.getUnmergedLFNBase(), '/store/unmerged')
        self.assertEqual(testWorkload.getMergedLFNBase(), '/store/data')
        self.assertEqual(testWorkload.listInputDatasets(), [])

        tasksProducingOutput = ['/TestWorkload/GENSIM', '/TestWorkload/GENSIM/GENSIMMergeRAWSIMoutput',
                                '/TestWorkload/GENSIM/RECOMergeAODSIMoutput',
                                '/TestWorkload/GENSIM/RECOMergeDQMoutput',
                                '/TestWorkload/GENSIM/RECOMergeRECOSIMoutput']
        self.assertItemsEqual(testWorkload.listOutputProducingTasks(), tasksProducingOutput)

        # workqueue start policy check
        self.assertEqual(testWorkload.startPolicy(), "MonteCarlo")
        workqueueSplit = {'SliceSize': 200, 'SliceType': 'NumberOfEvents', 'SplittingAlgo': 'EventBased',
                          'SubSliceSize': 100, 'SubSliceType': 'NumberOfEventsPerLumi',
                          'policyName': 'MonteCarlo', 'OpenRunningTimeout': 0}
        self.assertDictEqual(testWorkload.startPolicyParameters(), workqueueSplit)
        # workload tasks check
        tasks = ['GENSIM', 'GENSIMMergeRAWSIMoutput', 'RECOMergeAODSIMoutput', 'RECOMergeDQMoutput',
                 'RECOMergeRECOSIMoutput', 'GENSIMRAWSIMoutputMergeLogCollect', 'RECOAODSIMoutputMergeLogCollect',
                 'RECODQMoutputMergeLogCollect', 'RECORECOSIMoutputMergeLogCollect',
                 'GENSIMCleanupUnmergedRAWSIMoutput',
                 'RECOCleanupUnmergedAODSIMoutput', 'RECOCleanupUnmergedRECOSIMoutput', 'RECOCleanupUnmergedDQMoutput']
        self.assertItemsEqual(testWorkload.listAllTaskNames(), tasks)

        task = testWorkload.getTask(tasks[0])
        self.assertEqual(task.name(), "GENSIM")
        self.assertEqual(task.getPathName(), "/TestWorkload/GENSIM")
        self.assertEqual(task.taskType(), "Production", "Wrong task type")

        splitParams = task.jobSplittingParameters()
        self.assertEqual(splitParams['algorithm'], "EventBased", "Wrong job splitting algo")
        self.assertEqual(splitParams['events_per_job'], 200)
        self.assertEqual(splitParams['events_per_lumi'], 100)
        self.assertFalse(splitParams['lheInputFiles'], "Wrong LHE flag")
        self.assertFalse(splitParams['deterministicPileup'])
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
        self.assertEqual(outMods, {'RAWSIMoutput', 'AODSIMoutput', 'RECOSIMoutput', 'DQMoutput'},
                         "Wrong output modules")
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
        self.assertEqual(step.data.output.modules.DQMoutput.filterName, '')
        self.assertEqual(step.data.output.modules.RECOSIMoutput.dataTier, 'GEN-SIM-RECO')
        self.assertEqual(step.data.output.modules.AODSIMoutput.dataTier, 'AODSIM')
        self.assertEqual(step.data.output.modules.DQMoutput.dataTier, 'DQMIO')
        self.assertTrue(step.data.output.keep)
        self.assertFalse(step.data.tree.childNames)
        self.assertEqual(step.data.application.setup.cmsswVersion, testArguments['Step3']["CMSSWVersion"])
        self.assertEqual(step.data.application.setup.scramArch, testArguments['Step3']["ScramArch"])
        self.assertEqual(step.data.application.configuration.arguments.globalTag, testArguments['Step3']["GlobalTag"])

        # test merge stuff
        task = testWorkload.getTaskByPath('/TestWorkload/GENSIM/GENSIMMergeRAWSIMoutput')
        self.assertEqual(task.taskType(), "Merge")
        self.assertEqual(task.getSwVersion(), testArguments['Step1']["CMSSWVersion"])
        self.assertEqual(task.getScramArch(), testArguments['Step1']["ScramArch"])

        task = testWorkload.getTaskByPath('/TestWorkload/GENSIM/RECOMergeAODSIMoutput')
        self.assertEqual(task.taskType(), "Merge")
        self.assertEqual(task.getSwVersion(), testArguments['Step3']["CMSSWVersion"])
        self.assertEqual(task.getScramArch(), testArguments['Step3']["ScramArch"])

        # test logCollect stuff
        task = testWorkload.getTaskByPath(
                '/TestWorkload/GENSIM/GENSIMMergeRAWSIMoutput/GENSIMRAWSIMoutputMergeLogCollect')
        self.assertEqual(task.taskType(), "LogCollect")
        step = task.getStep("logCollect1")
        self.assertEqual(step.data.application.setup.cmsswVersion, testArguments['Step1']["CMSSWVersion"])
        self.assertEqual(step.data.application.setup.scramArch, testArguments['Step1']["ScramArch"])

        task = testWorkload.getTaskByPath('/TestWorkload/GENSIM/RECOMergeAODSIMoutput/RECOAODSIMoutputMergeLogCollect')
        self.assertEqual(task.taskType(), "LogCollect")
        step = task.getStep("logCollect1")
        self.assertEqual(step.data.application.setup.cmsswVersion, testArguments['Step3']["CMSSWVersion"])
        self.assertEqual(step.data.application.setup.scramArch, testArguments['Step3']["ScramArch"])

    def testPileupWithoutInputData(self):
        """
        Test whether we can properly setup the Pileup information even
        when the top level step has no input dataset (so pileup over a
        MC from scratch step)
        """
        testArguments = StepChainWorkloadFactory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))

        configId = injectStepChainConfigSingle(self.configDatabase)
        testArguments['Step1'].update(ConfigCacheID=configId,
                                      MCPileup=testArguments['Step2']['MCPileup'])
        testArguments.pop("Step2", None)
        testArguments.pop("Step3", None)
        testArguments['StepChain'] = 1

        factory = StepChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        # workload level check
        self.assertEqual(testWorkload.getDashboardActivity(), "production")
        self.assertEqual(testWorkload.listInputDatasets(), [])
        pileups = listvalues(testWorkload.listPileupDatasets())
        pileups = [item for puSet in pileups for item in puSet]
        self.assertItemsEqual(pileups, [testArguments['Step1']['MCPileup']])

        # task level check
        task = testWorkload.getTask(taskName=testArguments['Step1']['StepName'])
        self.assertItemsEqual([testArguments['Step1']['MCPileup']], task.getInputPileupDatasets())

        # step level check
        stepHelper = task.getStepHelper('cmsRun1')
        puConfig = stepHelper.getPileup()
        self.assertItemsEqual([testArguments['Step1']["MCPileup"]], puConfig.mc.dataset)

        # test assignment with wrong Trust flags
        assignDict = {"SiteWhitelist": ["T2_US_Nebraska"], "Team": "The-A-Team",
                      "RequestStatus": "assigned",
                      "TrustSitelists": True, "TrustPUSitelists": True
                      }
        with self.assertRaises(RuntimeError):
            testWorkload.updateArguments(assignDict)
        # now with correct flags
        assignDict['TrustSitelists'] = False
        testWorkload.updateArguments(assignDict)

    def testMultiplePileupDsets(self):
        """
        Test a StepChain which uses different Pileup datasets for different
        steps in the chain.
        """
        testArguments = StepChainWorkloadFactory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))

        configDocs = injectStepChainConfigMC(self.configDatabase)
        for s in ['Step1', 'Step2', 'Step3']:
            testArguments[s]['ConfigCacheID'] = configDocs[s]
        testArguments['Step2']['KeepOutput'] = False
        testArguments['Step3']['MCPileup'] = "/WJetsToLNu_1J_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIIAutumn18NanoAOD-102X_upgrade2018_realistic_v15-v1/NANOAODSIM"

        factory = StepChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        # workload level check
        pileups = listvalues(testWorkload.listPileupDatasets())
        pileups = [item for puSet in pileups for item in puSet]
        self.assertIn(testArguments['Step2']['MCPileup'], pileups)
        self.assertIn(testArguments['Step3']['MCPileup'], pileups)

        # task level check
        task = testWorkload.getTask(taskName=testArguments['Step1']['StepName'])
        pileups = [testArguments['Step2']['MCPileup'], testArguments['Step3']['MCPileup']]
        self.assertItemsEqual(pileups, task.getInputPileupDatasets())

        # step level check
        for idx in range(1, testArguments['StepChain'] + 1):
            stepName = "cmsRun%s" % idx
            stepNum = "Step%s" % idx
            stepHelper = task.getStepHelper(stepName)
            puConfig = stepHelper.getPileup()
            if puConfig:
                puConfig = puConfig.mc.dataset
            else:
                puConfig = [puConfig]  # let's call it a [None] then, for the sake of testing
            self.assertItemsEqual([testArguments[stepNum].get("MCPileup")], puConfig)

    def testStepChainIncludeParentsValidation(self):
        """
        Check that the test arguments pass basic validation,
        i.e. no exception should be raised.
        """
        testArguments = StepChainWorkloadFactory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))

        configDocs = injectStepChainConfigMC(self.configDatabase)
        for s in ['Step1', 'Step2', 'Step3']:
            testArguments[s]['ConfigCacheID'] = configDocs[s]

        testArguments['Step1'] = deepcopy(testArguments.pop('Step2'))
        testArguments['Step2'] = deepcopy(testArguments.pop('Step3'))
        testArguments['StepChain'] = 2

        testArguments['Step1'].pop('InputFromOutputModule')
        testArguments['Step1'].pop('InputStep')
        testArguments['Step1'].update({
            'IncludeParents': True,
            'KeepOutput': False,
            'InputDataset': '/Cosmics/ComissioningHI-v1/RAW'
        })

        factory = StepChainWorkloadFactory()
        self.assertRaises(WMSpecFactoryException, factory.factoryWorkloadConstruction,
                          "TestWorkload", testArguments)

        testArguments['Step1']["InputDataset"] = '/GammaGammaToEE_Elastic_Pt15_8TeV-lpair/Summer12-START53_V7C-v1/GEN-SIM'
        factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        testArguments['Step1']["IncludeParents"] = False
        factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        testArguments['Step1']["IncludeParents"] = False
        testArguments['Step1']["InputDataset"] = '/Cosmics/ComissioningHI-v1/RAW'
        factory.factoryWorkloadConstruction("TestWorkload", testArguments)
        return

    def testStepChainReDigi(self):
        """
        Build a StepChain workload with input dataset in the first step
        """
        testArguments = StepChainWorkloadFactory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))

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
            'BlockWhitelist': [
                "/BprimeJetToBZ_M800GeV_Tune4C_13TeV-madgraph-tauola/Fall13-POSTLS162_V1-v1/GEN-SIM#3a09df90-5593-11e4-bd05-003048f0e3f4",
                "/BprimeJetToBZ_M800GeV_Tune4C_13TeV-madgraph-tauola/Fall13-POSTLS162_V1-v1/GEN-SIM#3a8b15e6-54e0-11e4-afc7-003048f0e3f4"]
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
        self.assertEqual(len(tasks), 10)
        for t in ['DIGI', 'RECOMergeAODSIMoutput', 'RECOMergeRECOSIMoutput', 'RECOMergeDQMoutput',
                  'RECOAODSIMoutputMergeLogCollect', 'RECORECOSIMoutputMergeLogCollect', 'RECODQMoutputMergeLogCollect',
                  'RECOCleanupUnmergedAODSIMoutput', 'RECOCleanupUnmergedRECOSIMoutput',
                  'RECOCleanupUnmergedDQMoutput']:
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

        self.assertEqual(task.getInputDatasetPath(),
                         '/BprimeJetToBZ_M800GeV_Tune4C_13TeV-madgraph-tauola/Fall13-POSTLS162_V1-v1/GEN-SIM')
        self.assertTrue(
                '/BprimeJetToBZ_M800GeV_Tune4C_13TeV-madgraph-tauola/Fall13-POSTLS162_V1-v1/GEN-SIM#3a09df90-5593-11e4-bd05-003048f0e3f4' in task.inputBlockWhitelist())
        self.assertTrue(
                '/BprimeJetToBZ_M800GeV_Tune4C_13TeV-madgraph-tauola/Fall13-POSTLS162_V1-v1/GEN-SIM#3a8b15e6-54e0-11e4-afc7-003048f0e3f4' in task.inputBlockWhitelist())

        # test workload step stuff
        self.assertEqual(sorted(task.listAllStepNames()), ['cmsRun1', 'cmsRun2', 'logArch1', 'stageOut1'])
        self.assertEqual(task.getTopStepName(), 'cmsRun1')
        self.assertEqual(task.getStep("cmsRun1").stepType(), "CMSSW")
        self.assertFalse(task.getInputStep(), "Wrong input step")
        outModsAndDsets = task.listOutputDatasetsAndModules()
        outMods = set([elem['outputModule'] for elem in outModsAndDsets])
        outDsets = [elem['outputDataset'] for elem in outModsAndDsets]
        self.assertEqual(outMods, {'AODSIMoutput', 'RECOSIMoutput', 'DQMoutput'}, "Wrong output modules")
        self.assertTrue(
                '/PrimaryDataset-StepChain/AcquisitionEra_StepChain-FilterD-ProcessingString_StepChain-v1/AODSIM' in outDsets)
        self.assertTrue(
                '/PrimaryDataset-StepChain/AcquisitionEra_StepChain-ProcessingString_StepChain-v1/DQMIO' in outDsets)
        self.assertTrue(
                '/PrimaryDataset-StepChain/AcquisitionEra_StepChain-FilterC-ProcessingString_StepChain-v1/GEN-SIM-RECO' in outDsets)
        return

    def testSubscriptions(self):
        """
        Build a StepChain workload defining different processed dataset name among the steps
        """
        subscriptionInfo = {'CustodialSites': ['T1_US_FNAL_MSS'],
                            'DeleteFromSource': False,
                            'NonCustodialSites': ['T1_US_FNAL_Disk'],
                            'Priority': 'High',
                            'DatasetLifetime': None}
        assignDict = {"SiteWhitelist": ["T2_US_Nebraska", "T2_IT_Rome"], "Team": "The-A-Team",
                      "RequestStatus": "assigned", "RequestPriority": 111,
                      "CustodialSites": subscriptionInfo['CustodialSites'],
                      "NonCustodialSites": subscriptionInfo['NonCustodialSites'],
                      "SubscriptionPriority": subscriptionInfo['Priority'],
                      "DatasetLifetime": subscriptionInfo['DatasetLifetime']
                      }

        testArguments = StepChainWorkloadFactory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))
        testArguments['Step3']['InputFromOutputModule'] = testArguments['Step2']['InputFromOutputModule']

        configDocs = injectStepChainConfigMC(self.configDatabase)
        for s in ['Step1', 'Step2', 'Step3']:
            testArguments[s]['ConfigCacheID'] = configDocs[s]
        testArguments['Step2']['KeepOutput'] = False

        factory = StepChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        # test default request priority
        self.assertEqual(testWorkload.priority(), 8000)

        # and assign it
        testWorkload.updateArguments(assignDict)

        # test new priority in the spec
        self.assertEqual(testWorkload.priority(), assignDict["RequestPriority"])

        outputDsets = testWorkload.listOutputDatasets()
        self.assertEqual(len(outputDsets), 4)
        expectedInfo = {dset: deepcopy(subscriptionInfo) for dset in outputDsets}
        self.assertDictEqual(testWorkload.getSubscriptionInformation(), expectedInfo)

        ### and now test it with duplicated output module settings
        testArguments['Step2']['KeepOutput'] = True
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)
        testWorkload.updateArguments(assignDict)

        outputDsets = testWorkload.listOutputDatasets()
        self.assertEqual(len(outputDsets), 5)
        expectedInfo = {dset: deepcopy(subscriptionInfo) for dset in outputDsets}
        self.assertDictEqual(testWorkload.getSubscriptionInformation(), expectedInfo)

    def testOutputDataSettings(self):
        """
        Build a StepChain workload defining different processed dataset name among the steps
        """

        outDsets = ['/PrimaryDataset-StepChain/AcqEra_Step1-FilterA-ProcStr_Step1-v1/GEN-SIM',
                    '/PrimaryDataset-StepChain/AcqEra_Step3-FilterD-ProcStr_Step3-v3/AODSIM',
                    '/PrimaryDataset-StepChain/AcqEra_Step3-ProcStr_Step3-v3/DQMIO',
                    '/PrimaryDataset-StepChain/AcqEra_Step3-FilterC-ProcStr_Step3-v3/GEN-SIM-RECO']

        outputLFNBases = ['/store/unmerged/AcqEra_Step1/PrimaryDataset-StepChain/GEN-SIM/FilterA-ProcStr_Step1-v1',
                          '/store/data/AcqEra_Step1/PrimaryDataset-StepChain/GEN-SIM/FilterA-ProcStr_Step1-v1',
                          '/store/unmerged/AcqEra_Step2/PrimaryDataset-StepChain/GEN-SIM-RAW/FilterB-ProcStr_Step2-v2',
                          '/store/unmerged/AcqEra_Step3/PrimaryDataset-StepChain/GEN-SIM-RECO/FilterC-ProcStr_Step3-v3',
                          '/store/data/AcqEra_Step3/PrimaryDataset-StepChain/GEN-SIM-RECO/FilterC-ProcStr_Step3-v3',
                          '/store/unmerged/AcqEra_Step3/PrimaryDataset-StepChain/DQMIO/ProcStr_Step3-v3',
                          '/store/data/AcqEra_Step3/PrimaryDataset-StepChain/DQMIO/ProcStr_Step3-v3',
                          '/store/unmerged/AcqEra_Step3/PrimaryDataset-StepChain/AODSIM/FilterD-ProcStr_Step3-v3',
                          '/store/data/AcqEra_Step3/PrimaryDataset-StepChain/AODSIM/FilterD-ProcStr_Step3-v3']

        outMods = {'RAWSIMoutput': dict(dataTier='GEN-SIM', filterName='FilterA', transient=True,
                                        primaryDataset=REQUEST['PrimaryDataset'],
                                        processedDataset="AcqEra_Step1-FilterA-ProcStr_Step1-v1",
                                        lfnBase=outputLFNBases[0],
                                        mergedLFNBase=outputLFNBases[0 + 1]),
                   'RECOSIMoutput': dict(dataTier='GEN-SIM-RECO', filterName='FilterC', transient=True,
                                         primaryDataset=REQUEST['PrimaryDataset'],
                                         processedDataset="AcqEra_Step3-FilterC-ProcStr_Step3-v3",
                                         lfnBase=outputLFNBases[3],
                                         mergedLFNBase=outputLFNBases[3 + 1]),
                   'DQMoutput': dict(dataTier='DQMIO', filterName='', transient=True,
                                     primaryDataset=REQUEST['PrimaryDataset'],
                                     processedDataset="AcqEra_Step3-ProcStr_Step3-v3",
                                     lfnBase=outputLFNBases[5],
                                     mergedLFNBase=outputLFNBases[5 + 1]),
                   'AODSIMoutput': dict(dataTier='AODSIM', filterName='FilterD', transient=True,
                                        primaryDataset=REQUEST['PrimaryDataset'],
                                        processedDataset="AcqEra_Step3-FilterD-ProcStr_Step3-v3",
                                        lfnBase=outputLFNBases[7],
                                        mergedLFNBase=outputLFNBases[7 + 1])}

        mergedMods = deepcopy(outMods)
        mergedMods['RAWSIMoutput'].update({'transient': False, 'lfnBase': outputLFNBases[0 + 1]})
        mergedMods['RECOSIMoutput'].update({'transient': False, 'lfnBase': outputLFNBases[3 + 1]})
        mergedMods['DQMoutput'].update({'transient': False, 'lfnBase': outputLFNBases[5 + 1]})
        mergedMods['AODSIMoutput'].update({'transient': False, 'lfnBase': outputLFNBases[7 + 1]})

        transientMod = {'RAWSIMoutput': dict(dataTier='GEN-SIM-RAW', filterName='FilterB', transient=True,
                                             primaryDataset=REQUEST['PrimaryDataset'],
                                             processedDataset="AcqEra_Step2-FilterB-ProcStr_Step2-v2",
                                             lfnBase=outputLFNBases[2],
                                             mergedLFNBase=outputLFNBases[2].replace('unmerged', 'data'))}

        testArguments = StepChainWorkloadFactory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))

        configDocs = injectStepChainConfigMC(self.configDatabase)
        for s in ['Step1', 'Step2', 'Step3']:
            testArguments[s]['ConfigCacheID'] = configDocs[s]
            testArguments[s]['AcquisitionEra'] = 'AcqEra_' + s
            testArguments[s]['ProcessingString'] = 'ProcStr_' + s
            testArguments[s]['ProcessingVersion'] = int(s.replace('Step', ''))

        testArguments['Step2']['KeepOutput'] = False

        factory = StepChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        # Case 1: only workload creation
        lfnBases = ("/store/unmerged", "/store/data")
        self._checkThisOutputStuff(testWorkload, outDsets, outputLFNBases, outMods, lfnBases, mergedMods,
                                   step2Transient=transientMod)

        # Case 2: workload creation and assignment, with no output dataset override
        assignDict = {"SiteWhitelist": ["T2_US_Nebraska", "T2_IT_Rome"], "Team": "The-A-Team"}
        testWorkload.updateArguments(assignDict)
        self._checkThisOutputStuff(testWorkload, outDsets, outputLFNBases, outMods, lfnBases, mergedMods,
                                   step2Transient=transientMod)

        # Case 3: workload creation and assignment, output dataset overriden with the same values
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)
        assignDict = {"SiteWhitelist": ["T2_US_Nebraska", "T2_IT_Rome"], "Team": "The-A-Team",
                      "AcquisitionEra": {"GENSIM": "AcqEra_Step1", "DIGI": "AcqEra_Step2", "RECO": "AcqEra_Step3"},
                      "ProcessingString": {"GENSIM": "ProcStr_Step1", "DIGI": "ProcStr_Step2",
                                           "RECO": "ProcStr_Step3"},
                      "ProcessingVersion": {"GENSIM": 1, "DIGI": 2, "RECO": 3},
                      "MergedLFNBase": "/store/data",
                      "UnmergedLFNBase": "/store/unmerged"
                      }
        testWorkload.updateArguments(assignDict)
        self._checkThisOutputStuff(testWorkload, outDsets, outputLFNBases, outMods, lfnBases, mergedMods,
                                   step2Transient=transientMod)

        # Case 4: workload creation and assignment, output dataset overriden with new values
        lfnBases = ("/store/unmerged", "/store/mc")
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)
        assignDict = {"SiteWhitelist": ["T2_US_Nebraska", "T2_IT_Rome"], "Team": "The-A-Team",
                      "AcquisitionEra": {"GENSIM": "AcqEra_StepA", "DIGI": "AcqEra_StepB", "RECO": "AcqEra_StepC"},
                      "ProcessingString": {"GENSIM": "ProcStr_StepA", "DIGI": "ProcStr_StepB",
                                           "RECO": "ProcStr_StepC"},
                      "ProcessingVersion": {"GENSIM": 41, "DIGI": 42, "RECO": 43},
                      "MergedLFNBase": lfnBases[1],
                      "UnmergedLFNBase": lfnBases[0]
                      }
        testWorkload.updateArguments(assignDict)
        for tp in [("Step1", "StepA"), ("Step2", "StepB"), ("Step3", "StepC")]:
            outDsets = [dset.replace(tp[0], tp[1]) for dset in outDsets]
            outputLFNBases = [lfn.replace(tp[0], tp[1]) for lfn in outputLFNBases]
            for mod in outMods:
                outMods[mod] = {k: (v.replace(tp[0], tp[1]) if isinstance(v, (str, bytes)) else v)
                                for k, v in viewitems(outMods[mod])}
            transientMod['RAWSIMoutput'] = {k: (v.replace(tp[0], tp[1]) if isinstance(v, (str, bytes)) else v)
                                            for k, v in viewitems(transientMod['RAWSIMoutput'])}
        for tp in [("v1", "v41"), ("v2", "v42"), ("v3", "v43"), ("/store/data", "/store/mc")]:
            outDsets = [dset.replace(tp[0], tp[1]) for dset in outDsets]
            outputLFNBases = [lfn.replace(tp[0], tp[1]) for lfn in outputLFNBases]
            for mod in outMods:
                outMods[mod] = {k: (v.replace(tp[0], tp[1]) if isinstance(v, (str, bytes)) else v)
                                for k, v in viewitems(outMods[mod])}
            transientMod['RAWSIMoutput'] = {k: (v.replace(tp[0], tp[1]) if isinstance(v, (str, bytes)) else v)
                                            for k, v in viewitems(transientMod['RAWSIMoutput'])}

        mergedMods = deepcopy(outMods)
        mergedMods['RAWSIMoutput'].update({'transient': False, 'lfnBase': outputLFNBases[0 + 1]})
        mergedMods['RECOSIMoutput'].update({'transient': False, 'lfnBase': outputLFNBases[3 + 1]})
        mergedMods['DQMoutput'].update({'transient': False, 'lfnBase': outputLFNBases[5 + 1]})
        mergedMods['AODSIMoutput'].update({'transient': False, 'lfnBase': outputLFNBases[7 + 1]})

        self._checkThisOutputStuff(testWorkload, outDsets, outputLFNBases, outMods, lfnBases, mergedMods,
                                   step2Transient=transientMod)

        return

    def testDupOutputDataSettings(self):
        """
        Build a StepChain workload defining different processed dataset name among the steps
        and keeping the output of different steps using the same output module
        """
        outDsets = ['/PrimaryDataset-StepChain/AcqEra_Step1-FilterA-ProcStr_Step1-v1/GEN-SIM',
                    '/PrimaryDataset-StepChain/AcqEra_Step2-FilterB-ProcStr_Step2-v2/GEN-SIM-RAW',
                    '/PrimaryDataset-StepChain/AcqEra_Step3-FilterD-ProcStr_Step3-v3/AODSIM',
                    '/PrimaryDataset-StepChain/AcqEra_Step3-ProcStr_Step3-v3/DQMIO',
                    '/PrimaryDataset-StepChain/AcqEra_Step3-FilterC-ProcStr_Step3-v3/GEN-SIM-RECO']

        outputLFNBases = ['/store/unmerged/AcqEra_Step1/PrimaryDataset-StepChain/GEN-SIM/FilterA-ProcStr_Step1-v1',
                          '/store/data/AcqEra_Step1/PrimaryDataset-StepChain/GEN-SIM/FilterA-ProcStr_Step1-v1',
                          '/store/unmerged/AcqEra_Step2/PrimaryDataset-StepChain/GEN-SIM-RAW/FilterB-ProcStr_Step2-v2',
                          '/store/data/AcqEra_Step2/PrimaryDataset-StepChain/GEN-SIM-RAW/FilterB-ProcStr_Step2-v2',
                          '/store/unmerged/AcqEra_Step3/PrimaryDataset-StepChain/GEN-SIM-RECO/FilterC-ProcStr_Step3-v3',
                          '/store/data/AcqEra_Step3/PrimaryDataset-StepChain/GEN-SIM-RECO/FilterC-ProcStr_Step3-v3',
                          '/store/unmerged/AcqEra_Step3/PrimaryDataset-StepChain/AODSIM/FilterD-ProcStr_Step3-v3',
                          '/store/data/AcqEra_Step3/PrimaryDataset-StepChain/AODSIM/FilterD-ProcStr_Step3-v3',
                          '/store/unmerged/AcqEra_Step3/PrimaryDataset-StepChain/DQMIO/ProcStr_Step3-v3',
                          '/store/data/AcqEra_Step3/PrimaryDataset-StepChain/DQMIO/ProcStr_Step3-v3',
                          ]

        outMods = {'RAWSIMoutput': [dict(dataTier='GEN-SIM', filterName='FilterA', transient=True,
                                         primaryDataset=REQUEST['PrimaryDataset'],
                                         processedDataset="AcqEra_Step1-FilterA-ProcStr_Step1-v1",
                                         lfnBase=outputLFNBases[0],
                                         mergedLFNBase=outputLFNBases[0 + 1]),
                                    dict(dataTier='GEN-SIM-RAW', filterName='FilterB', transient=True,
                                         primaryDataset=REQUEST['PrimaryDataset'],
                                         processedDataset="AcqEra_Step2-FilterB-ProcStr_Step2-v2",
                                         lfnBase=outputLFNBases[2],
                                         mergedLFNBase=outputLFNBases[2 + 1])],
                   'RECOSIMoutput': dict(dataTier='GEN-SIM-RECO', filterName='FilterC', transient=True,
                                         primaryDataset=REQUEST['PrimaryDataset'],
                                         processedDataset="AcqEra_Step3-FilterC-ProcStr_Step3-v3",
                                         lfnBase=outputLFNBases[4],
                                         mergedLFNBase=outputLFNBases[4 + 1]),
                   'AODSIMoutput': dict(dataTier='AODSIM', filterName='FilterD', transient=True,
                                        primaryDataset=REQUEST['PrimaryDataset'],
                                        processedDataset="AcqEra_Step3-FilterD-ProcStr_Step3-v3",
                                        lfnBase=outputLFNBases[6],
                                        mergedLFNBase=outputLFNBases[6 + 1]),
                   'DQMoutput': dict(dataTier='DQMIO', filterName='', transient=True,
                                        primaryDataset=REQUEST['PrimaryDataset'],
                                        processedDataset="AcqEra_Step3-ProcStr_Step3-v3",
                                        lfnBase=outputLFNBases[8],
                                        mergedLFNBase=outputLFNBases[8 + 1])
                   }

        lfnBases = ("/store/unmerged", "/store/data")

        mergedMods = deepcopy(outMods)
        mergedMods['RAWSIMoutput'][0].update({'transient': False, 'lfnBase': outputLFNBases[0 + 1]})
        mergedMods['RAWSIMoutput'][1].update({'transient': False, 'lfnBase': outputLFNBases[2 + 1]})
        mergedMods['RECOSIMoutput'].update({'transient': False, 'lfnBase': outputLFNBases[4 + 1]})
        mergedMods['AODSIMoutput'].update({'transient': False, 'lfnBase': outputLFNBases[6 + 1]})
        mergedMods['DQMoutput'].update({'transient': False, 'lfnBase': outputLFNBases[8 + 1]})

        testArguments = StepChainWorkloadFactory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))

        configDocs = injectStepChainConfigMC(self.configDatabase)
        for s in ['Step1', 'Step2', 'Step3']:
            testArguments[s]['ConfigCacheID'] = configDocs[s]
            testArguments[s]['AcquisitionEra'] = 'AcqEra_' + s
            testArguments[s]['ProcessingString'] = 'ProcStr_' + s
            testArguments[s]['ProcessingVersion'] = int(s.replace('Step', ''))
        testArguments['Step3']['InputFromOutputModule'] = testArguments['Step2']['InputFromOutputModule']

        factory = StepChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        # creation only
        self._checkThisOutputStuff(testWorkload, outDsets, outputLFNBases, outMods, lfnBases, mergedMods)

        # now assign it, output dataset overriden with new values
        lfnBases = ("/store/unmerged", "/store/mc")
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)
        assignDict = {"SiteWhitelist": ["T2_US_Nebraska", "T2_IT_Rome"], "Team": "The-A-Team",
                      "AcquisitionEra": {"GENSIM": "AcqEra_StepA", "DIGI": "AcqEra_StepB", "RECO": "AcqEra_StepC"},
                      "ProcessingString": {"GENSIM": "ProcStr_StepA", "DIGI": "ProcStr_StepB",
                                           "RECO": "ProcStr_StepC"},
                      "ProcessingVersion": {"GENSIM": 41, "DIGI": 42, "RECO": 43},
                      "UnmergedLFNBase": lfnBases[0],
                      "MergedLFNBase": lfnBases[1],
                      }
        testWorkload.updateArguments(assignDict)
        for tp in [("Step1", "StepA"), ("Step2", "StepB"), ("Step3", "StepC")]:
            outDsets = [dset.replace(tp[0], tp[1]) for dset in outDsets]
            outputLFNBases = [lfn.replace(tp[0], tp[1]) for lfn in outputLFNBases]
            for mod in outMods:
                if isinstance(outMods[mod], dict):
                    outMods[mod] = {k: (v.replace(tp[0], tp[1]) if isinstance(v, (str, bytes)) else v)
                                    for k, v in viewitems(outMods[mod])}
                else:
                    outMods[mod] = [
                        {k: (v.replace(tp[0], tp[1]) if isinstance(v, (str, bytes)) else v) for k, v in viewitems(out)} for
                        out in outMods[mod]]
        for tp in [("v1", "v41"), ("v2", "v42"), ("v3", "v43"), ("/store/data", "/store/mc")]:
            outDsets = [dset.replace(tp[0], tp[1]) for dset in outDsets]
            outputLFNBases = [lfn.replace(tp[0], tp[1]) for lfn in outputLFNBases]
            for mod in outMods:
                if isinstance(outMods[mod], dict):
                    outMods[mod] = {k: (v.replace(tp[0], tp[1]) if isinstance(v, (str, bytes)) else v)
                                    for k, v in viewitems(outMods[mod])}
                else:
                    outMods[mod] = [
                        {k: (v.replace(tp[0], tp[1]) if isinstance(v, (str, bytes)) else v) for k, v in viewitems(out)} for
                        out in outMods[mod]]
        mergedMods = deepcopy(outMods)
        mergedMods['RAWSIMoutput'][0].update({'transient': False, 'lfnBase': outputLFNBases[0 + 1]})
        mergedMods['RAWSIMoutput'][1].update({'transient': False, 'lfnBase': outputLFNBases[2 + 1]})
        mergedMods['RECOSIMoutput'].update({'transient': False, 'lfnBase': outputLFNBases[4 + 1]})
        mergedMods['AODSIMoutput'].update({'transient': False, 'lfnBase': outputLFNBases[6 + 1]})
        mergedMods['DQMoutput'].update({'transient': False, 'lfnBase': outputLFNBases[8 + 1]})

        self._checkThisOutputStuff(testWorkload, outDsets, outputLFNBases, outMods, lfnBases, mergedMods)

        return

    def _checkThisOutputStuff(self, workload, outDsets, outputLFNBases, outMods, lfnBases, mergedMods, subscribed=False,
                              step2Transient=None):
        "Performs a bunch of tests for the output settings"
        self.assertItemsEqual(workload.listOutputDatasets(), outDsets)
        self.assertItemsEqual(workload.listAllOutputModulesLFNBases(onlyUnmerged=False), outputLFNBases)

        task = workload.getTaskByName('GENSIM')
        self._checkOutputDsetsAndMods(task, outMods, outDsets, lfnBases, step2Transient)

        # test merge tasks now
        for count, mergeTask in enumerate(['GENSIMMergeRAWSIMoutput', 'DIGIMergeRAWSIMoutput', 'RECOMergeRECOSIMoutput',
                                           'RECOMergeAODSIMoutput']):
            if step2Transient and mergeTask == 'DIGIMergeRAWSIMoutput':
                continue
            task = workload.getTaskByPath('/TestWorkload/GENSIM/%s' % mergeTask)
            step = task.getStepHelper("cmsRun1")
            modName = mergeTask.split('Merge')[-1]
            if step2Transient:
                if mergeTask == "DIGIMergeRAWSIMoutput":
                    self._validateOutputModule('Merged', step.getOutputModule('Merged'), step2Transient[modName])
                else:
                    self._validateOutputModule('Merged', step.getOutputModule('Merged'), mergedMods[modName])
            else:
                if mergeTask in ["GENSIMMergeRAWSIMoutput", "DIGIMergeRAWSIMoutput"]:
                    self._validateOutputModule('Merged', step.getOutputModule('Merged'), mergedMods[modName][count])
                else:
                    self._validateOutputModule('Merged', step.getOutputModule('Merged'), mergedMods[modName])

        return

    def _checkOutputDsetsAndMods(self, task, outMods, outDsets, lfnBases, step2Transient):
        """
        Validate data related to output dataset, output modules and subscriptions
        :param task: task object
        :param outMods: dictionary with the output module info for this task
        :param outDsets: dictionary with the output datasets info for this task
        """
        self.assertItemsEqual(task.getIgnoredOutputModulesForTask(), [])

        self.assertItemsEqual(task._getLFNBase(), lfnBases)
        outputDsets = [x['outputDataset'] for x in task.listOutputDatasetsAndModules()]
        self.assertItemsEqual(outputDsets, outDsets)
        outputMods = list(set([x['outputModule'] for x in task.listOutputDatasetsAndModules()]))
        self.assertItemsEqual(outputMods, outMods)

        self.assertDictEqual(task.getSubscriptionInformation(), {})

        # check output modules from both task and step level
        self.assertEqual(task.getTopStepName(), 'cmsRun1')
        for count, stepRun in enumerate(['cmsRun1', 'cmsRun2', 'cmsRun3']):
            outModDict = task.getOutputModulesForStep(stepRun).dictionary_()
            for modName in outModDict:
                if step2Transient:
                    if stepRun == "cmsRun2":
                        self._validateOutputModule(modName, outModDict[modName], step2Transient[modName])
                    else:
                        self._validateOutputModule(modName, outModDict[modName], outMods[modName])
                else:
                    if stepRun in ["cmsRun1", "cmsRun2"]:
                        self._validateOutputModule(modName, outModDict[modName], outMods[modName][count])
                    else:
                        self._validateOutputModule(modName, outModDict[modName], outMods[modName])

            step = task.getStepHelper(stepRun)
            for modName in step.listOutputModules():
                if step2Transient:
                    if stepRun == "cmsRun2":
                        self._validateOutputModule(modName, step.getOutputModule(modName), step2Transient[modName])
                    else:
                        self._validateOutputModule(modName, step.getOutputModule(modName), outMods[modName])
                else:
                    if stepRun in ["cmsRun1", "cmsRun2"]:
                        self._validateOutputModule(modName, step.getOutputModule(modName), outMods[modName][count])
                    else:
                        self._validateOutputModule(modName, step.getOutputModule(modName), outMods[modName])

        return

    def _validateOutputModule(self, outModName, outModObj, dictExp):
        """
        Make sure the task/step provided output module object contains
        the same values as the expected from the request json settings.
        :param outModName: output module name string
        :param outModObj: an output module object containing a ConfigSection object
        :param dictExp: dict with the same key/values as in the output module object
        """
        self.assertEqual(outModObj.dataTier, dictExp['dataTier'])
        self.assertEqual(outModObj.filterName, dictExp['filterName'])
        self.assertEqual(outModObj.mergedLFNBase, dictExp['mergedLFNBase'])
        self.assertEqual(outModObj.transient, dictExp['transient'])
        self.assertEqual(outModObj.processedDataset, dictExp['processedDataset'])
        self.assertEqual(outModObj.primaryDataset, dictExp['primaryDataset'])
        self.assertEqual(outModObj.lfnBase, dictExp['lfnBase'])

    def testStepMapping(self):
        """
        Build a mapping of steps, input and output modules
        """
        testArguments = StepChainWorkloadFactory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))
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

        expectedTasks = {'GENSIM', 'RECOMergeAODSIMoutput', 'RECOMergeRECOSIMoutput', 'RECOMergeDQMoutput',
                         'RECOAODSIMoutputMergeLogCollect', 'RECORECOSIMoutputMergeLogCollect',
                         'RECODQMoutputMergeLogCollect', 'RECOCleanupUnmergedAODSIMoutput',
                         'RECOCleanupUnmergedRECOSIMoutput', 'RECOCleanupUnmergedDQMoutput'}
        expectedSteps = {'cmsRun1', 'cmsRun2', 'cmsRun3', 'cmsRun4', 'stageOut1', 'logArch1'}

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
        self.assertItemsEqual(set(step4.data.output.modules.dictionary_()),
                              {'AODSIMoutput', 'DQMoutput', 'RECOSIMoutput'})
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
            self.assertEqual(stepHelper.getNumberOfStreams(), 0)
        # then test Memory requirements
        perfParams = prodTask.jobSplittingParameters()['performance']
        self.assertEqual(perfParams['memoryRequirement'], 2300.0)

        testArguments["Multicore"] = 6
        testArguments["Memory"] = 4600.0
        testArguments["EventStreams"] = 3
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload2", testArguments)
        prodTask = testWorkload.getTask('StepOne')
        for step in ('cmsRun1', 'stageOut1', 'logArch1'):
            stepHelper = prodTask.getStepHelper(step)
            if step in ('stageOut1', 'logArch1'):
                self.assertEqual(stepHelper.getNumberOfCores(), 1, "%s should have 1 core" % step)
                self.assertEqual(stepHelper.getNumberOfStreams(), 0)
            else:
                self.assertEqual(stepHelper.getNumberOfCores(), testArguments["Multicore"])
                self.assertEqual(stepHelper.getNumberOfStreams(), testArguments["EventStreams"])
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
            self.assertEqual(stepHelper.getNumberOfStreams(), 0)
        # then test Memory requirements
        perfParams = prodTask.jobSplittingParameters()['performance']
        self.assertEqual(perfParams['memoryRequirement'], 2300.0)

        # Test Multicore/Memory settings at TOP level **only**
        testArguments["Multicore"] = 6
        testArguments["Memory"] = 4600.0
        testArguments["EventStreams"] = 3
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload2", testArguments)
        prodTask = testWorkload.getTask('StepOne')
        for step in ('cmsRun1', 'cmsRun2', 'cmsRun3', 'stageOut1', 'logArch1'):
            stepHelper = prodTask.getStepHelper(step)
            if step in ('stageOut1', 'logArch1'):
                self.assertEqual(stepHelper.getNumberOfCores(), 1, "%s should have 1 core" % step)
                self.assertEqual(stepHelper.getNumberOfStreams(), 0)
            else:
                self.assertEqual(stepHelper.getNumberOfCores(), testArguments["Multicore"])
                self.assertEqual(stepHelper.getNumberOfStreams(), testArguments["EventStreams"])
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
        testArguments['Step1']["EventStreams"] = 2
        testArguments['Step3']["EventStreams"] = 4
        factory = StepChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        prodTask = testWorkload.getTask('StepOne')
        for step in ('cmsRun2', 'stageOut1', 'logArch1'):
            stepHelper = prodTask.getStepHelper(step)
            self.assertEqual(stepHelper.getNumberOfCores(), 1, "%s should have 1 core" % step)
        self.assertEqual(prodTask.getStepHelper('cmsRun1').getNumberOfCores(), testArguments['Step1']["Multicore"])
        self.assertEqual(prodTask.getStepHelper('cmsRun3').getNumberOfCores(), testArguments['Step3']["Multicore"])
        self.assertEqual(prodTask.getStepHelper('cmsRun1').getNumberOfStreams(), testArguments['Step1']["EventStreams"])
        self.assertEqual(prodTask.getStepHelper('cmsRun3').getNumberOfStreams(), testArguments['Step3']["EventStreams"])

        perfParams = prodTask.jobSplittingParameters()['performance']
        self.assertEqual(perfParams['memoryRequirement'], testArguments["Memory"])

        # Test mix of Multicore/Memory settings at both top and step level
        testArguments["Multicore"] = 3
        testArguments['Step1']["Multicore"] = 2
        testArguments['Step3']["Multicore"] = 4
        testArguments["Memory"] = 3300.0
        testArguments['Step1']["Memory"] = 2200.0
        testArguments['Step3']["Memory"] = 4400.0
        testArguments["EventStreams"] = 6
        testArguments['Step1']["EventStreams"] = 4
        testArguments['Step3']["EventStreams"] = 8
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload2", testArguments)
        prodTask = testWorkload.getTask('StepOne')
        for step in ('stageOut1', 'logArch1'):
            stepHelper = prodTask.getStepHelper(step)
            self.assertEqual(stepHelper.getNumberOfCores(), 1, "%s should have 1 core" % step)
            self.assertEqual(stepHelper.getNumberOfStreams(), 0)
        self.assertEqual(prodTask.getStepHelper('cmsRun1').getNumberOfCores(), testArguments['Step1']["Multicore"])
        self.assertEqual(prodTask.getStepHelper('cmsRun2').getNumberOfCores(), testArguments["Multicore"])
        self.assertEqual(prodTask.getStepHelper('cmsRun3').getNumberOfCores(), testArguments['Step3']["Multicore"])
        self.assertEqual(prodTask.getStepHelper('cmsRun1').getNumberOfStreams(), testArguments['Step1']["EventStreams"])
        self.assertEqual(prodTask.getStepHelper('cmsRun2').getNumberOfStreams(), testArguments["EventStreams"])
        self.assertEqual(prodTask.getStepHelper('cmsRun3').getNumberOfStreams(), testArguments['Step3']["EventStreams"])

        perfParams = prodTask.jobSplittingParameters()['performance']
        self.assertEqual(perfParams['memoryRequirement'], testArguments["Memory"])

        return

    def testPrepIDSettings(self):
        """
        Check whether each step carries the correct PrepID information
        """
        testArguments = StepChainWorkloadFactory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))

        configDocs = injectStepChainConfigMC(self.configDatabase)
        for s in ['Step1', 'Step2', 'Step3']:
            testArguments[s]['ConfigCacheID'] = configDocs[s]
        testArguments['Step2']['KeepOutput'] = False

        factory = StepChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        self.assertEqual(testWorkload.getPrepID(), testArguments['PrepID'])
        task = testWorkload.getTaskByName(testArguments['Step1']['StepName'])
        self.assertEqual(task.getPrepID(), testArguments['Step1']['PrepID'])
        # tricky business, there is only one Production/Processing task (Step1)
        # then PrepIDs are saved in the merge tasks
        task = testWorkload.getTaskByName('GENSIMMergeRAWSIMoutput')
        self.assertEqual(task.getPrepID(), testArguments['Step1']['PrepID'])
        task = testWorkload.getTaskByName('RECOMergeAODSIMoutput')
        self.assertEqual(task.getPrepID(), testArguments['Step3']['PrepID'])
        task = testWorkload.getTaskByName('RECOMergeRECOSIMoutput')
        self.assertEqual(task.getPrepID(), testArguments['Step3']['PrepID'])

        # Now we assign it just to make sure no changes will happen to the prepid
        assignDict = {"SiteWhitelist": ["T2_US_Nebraska", "T2_IT_Rome"], "Team": "The-A-Team"}
        testWorkload.updateArguments(assignDict)

        self.assertEqual(testWorkload.getPrepID(), testArguments['PrepID'])
        self.assertEqual(testWorkload.getPrepID(), testArguments['PrepID'])
        task = testWorkload.getTaskByName(testArguments['Step1']['StepName'])
        self.assertEqual(task.getPrepID(), testArguments['Step1']['PrepID'])

        task = testWorkload.getTaskByName('GENSIMMergeRAWSIMoutput')
        self.assertEqual(task.getPrepID(), testArguments['Step1']['PrepID'])
        task = testWorkload.getTaskByName('RECOMergeAODSIMoutput')
        self.assertEqual(task.getPrepID(), testArguments['Step3']['PrepID'])
        task = testWorkload.getTaskByName('RECOMergeRECOSIMoutput')
        self.assertEqual(task.getPrepID(), testArguments['Step3']['PrepID'])

        ### Test top level PrepID inheritance, creation only
        testArguments['Step2'].pop('PrepID')
        testArguments['Step3'].pop('PrepID')
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        self.assertEqual(testWorkload.getPrepID(), testArguments['PrepID'])
        self.assertEqual(testWorkload.getPrepID(), testArguments['PrepID'])
        task = testWorkload.getTaskByName(testArguments['Step1']['StepName'])
        self.assertEqual(task.getPrepID(), testArguments['Step1']['PrepID'])

        task = testWorkload.getTaskByName('GENSIMMergeRAWSIMoutput')
        self.assertEqual(task.getPrepID(), testArguments['Step1']['PrepID'])
        task = testWorkload.getTaskByName('RECOMergeAODSIMoutput')
        self.assertEqual(task.getPrepID(), testArguments['Step3'].get('PrepID', testArguments['PrepID']))
        task = testWorkload.getTaskByName('RECOMergeRECOSIMoutput')
        self.assertEqual(task.getPrepID(), testArguments['Step3'].get('PrepID', testArguments['PrepID']))

        # and we assign it too, better safe than sorry
        assignDict = {"SiteWhitelist": ["T2_US_Nebraska", "T2_IT_Rome"], "Team": "The-A-Team"}
        testWorkload.updateArguments(assignDict)
        self.assertEqual(testWorkload.getPrepID(), testArguments['PrepID'])
        self.assertEqual(testWorkload.getPrepID(), testArguments['PrepID'])
        task = testWorkload.getTaskByName(testArguments['Step1']['StepName'])
        self.assertEqual(task.getPrepID(), testArguments['Step1']['PrepID'])

        task = testWorkload.getTaskByName('GENSIMMergeRAWSIMoutput')
        self.assertEqual(task.getPrepID(), testArguments['Step1']['PrepID'])
        task = testWorkload.getTaskByName('RECOMergeAODSIMoutput')
        self.assertEqual(task.getPrepID(), testArguments['Step3'].get('PrepID', testArguments['PrepID']))
        task = testWorkload.getTaskByName('RECOMergeRECOSIMoutput')
        self.assertEqual(task.getPrepID(), testArguments['Step3'].get('PrepID', testArguments['PrepID']))

        return

    def testCMSSWSettings(self):
        """
        Build a StepChain workload starting from scratch
        """

        def _checkCMSSWScram(workload):
            "Validate CMSSW and ScramArch for this 3-steps template, including merge tasks"
            # workload level check
            self.assertItemsEqual(testWorkload.getCMSSWVersions(), ['CMSSW_7_1_25_patch2',
                                                                    'CMSSW_8_0_22', 'CMSSW_8_0_23'])
            # production/processing task level check
            task = testWorkload.getTaskByName(testArguments['Step1']['StepName'])
            self.assertEqual(task.getSwVersion(), testArguments['Step1']["CMSSWVersion"])
            self.assertEqual(task.getScramArch(), testArguments['Step1']["ScramArch"])

            # validate production/processing steps
            step = task.getStepHelper("cmsRun1")
            self.assertEqual(step.getCMSSWVersion(), testArguments['Step1']["CMSSWVersion"])
            self.assertItemsEqual(step.getScramArch(), testArguments['Step1']["ScramArch"])
            step = task.getStepHelper("cmsRun2")
            self.assertEqual(step.getCMSSWVersion(), testArguments['Step2']["CMSSWVersion"])
            self.assertItemsEqual(step.getScramArch(), testArguments['Step2']["ScramArch"])
            step = task.getStepHelper("cmsRun3")
            self.assertEqual(step.getCMSSWVersion(), testArguments['Step3']["CMSSWVersion"])
            self.assertItemsEqual(step.getScramArch(), testArguments['Step3']["ScramArch"])

            # then validate merge tasks
            print(testWorkload.listAllTaskNames())
            task = testWorkload.getTaskByName('GENSIMMergeRAWSIMoutput')
            self.assertEqual(task.getSwVersion(), testArguments['Step1']["CMSSWVersion"])
            self.assertEqual(task.getScramArch(), testArguments['Step1']["ScramArch"])
            task = testWorkload.getTaskByName('RECOMergeAODSIMoutput')
            self.assertEqual(task.getSwVersion(), testArguments['Step3']["CMSSWVersion"])
            self.assertEqual(task.getScramArch(), testArguments['Step3']["ScramArch"])
            task = testWorkload.getTaskByName('RECOMergeRECOSIMoutput')
            self.assertEqual(task.getSwVersion(), testArguments['Step3']["CMSSWVersion"])
            self.assertEqual(task.getScramArch(), testArguments['Step3']["ScramArch"])
            task = testWorkload.getTaskByName('RECOMergeDQMoutput')
            self.assertEqual(task.getSwVersion(), testArguments['Step3']["CMSSWVersion"])
            self.assertEqual(task.getScramArch(), testArguments['Step3']["ScramArch"])

            # finally, validate the harvesting task
            task = testWorkload.getTaskByName('RECOMergeDQMoutputEndOfRunDQMHarvestMerged')
            self.assertEqual(task.getSwVersion(), testArguments['Step3']["CMSSWVersion"])
            self.assertEqual(task.getScramArch(), testArguments['Step3']["ScramArch"])
            return

        testArguments = StepChainWorkloadFactory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))

        configDocs = injectStepChainConfigMC(self.configDatabase)
        for s in ['Step1', 'Step2', 'Step3']:
            testArguments[s]['ConfigCacheID'] = configDocs[s]
        testArguments['DQMConfigCacheID'] = configDocs['Harvest']
        testArguments['EnableHarvesting'] = True
        testArguments['Step2']['KeepOutput'] = False
        testArguments['Step2']['CMSSWVersion'] = "CMSSW_8_0_22"
        testArguments['Step2']['ScramArch'] = ["slc6_amd64_gcc530", "slc7_amd64_gcc530"]
        testArguments['Step3']['CMSSWVersion'] = "CMSSW_8_0_23"

        factory = StepChainWorkloadFactory()

        # Case 1: workflow creation only
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)
        _checkCMSSWScram(testWorkload)

        # Case 2: now we assign it just to make sure no changes will happen to the release values
        assignDict = {"SiteWhitelist": ["T2_US_Nebraska", "T2_IT_Rome"], "Team": "The-A-Team"}
        testWorkload.updateArguments(assignDict)
        _checkCMSSWScram(testWorkload)

        return

    def testInputDataSettings(self):
        """
        Test input data settings for a many-tasks TaskChain workload with specific
        settings for every single task.
        """
        childNames = ['GENSIMMergeRAWSIMoutput', 'RECOMergeAODSIMoutput', 'RECOMergeRECOSIMoutput',
                      'RECOMergeDQMoutput', 'GENSIMCleanupUnmergedRAWSIMoutput',
                      'RECOCleanupUnmergedAODSIMoutput', 'RECOCleanupUnmergedRECOSIMoutput',
                      'RECOCleanupUnmergedDQMoutput']

        def _checkInputData(workload, sitewhitelist=None):
            "Validate input data/block/run/step/PU for the 4-tasks request"
            sitewhitelist = sitewhitelist or []
            self.assertEqual(listvalues(workload.listPileupDatasets()), [{testArguments['Step2']['MCPileup']}])

            task = workload.getTaskByName(testArguments['Step1']['StepName'])
            self.assertEqual(task.taskType(), "Production")
            self.assertEqual(task.totalEvents(), testArguments['Step1']['RequestNumEvents'])
            self.assertItemsEqual(task.listChildNames(), childNames)
            self.assertEqual(task.getInputStep(), None)
            self.assertDictEqual(task.getLumiMask(), {})
            self.assertEqual(task.getFirstEvent(), testArguments['Step1'].get('FirstEvent', 1))
            self.assertEqual(task.getFirstLumi(), testArguments['Step1'].get('FirstLumi', 1))
            self.assertEqual(task.parentProcessingFlag(), testArguments['Step1'].get('IncludeParents', False))
            self.assertEqual(task.inputDataset(), testArguments['Step1'].get('InputDataset'))
            self.assertEqual(task.dbsUrl(), None)
            self.assertEqual(task.inputBlockWhitelist(), testArguments['Step1'].get('inputBlockWhitelist'))
            self.assertEqual(task.inputBlockBlacklist(), testArguments['Step1'].get('inputBlockBlacklist'))
            self.assertEqual(task.inputRunWhitelist(), testArguments['Step1'].get('inputRunWhitelist'))
            self.assertEqual(task.inputRunBlacklist(), testArguments['Step1'].get('inputRunBlacklist'))
            self.assertItemsEqual(task.siteWhitelist(), sitewhitelist)
            self.assertItemsEqual(task.siteBlacklist(), testArguments['Step1'].get('siteBlacklist', []))
            self.assertDictEqual(task.getTrustSitelists(), {'trustPUlists': False, 'trustlists': False})
            self.assertItemsEqual(task.getIgnoredOutputModulesForTask(),
                                  testArguments['Step1'].get('IgnoredOutputModules', []))
            splitParams = task.jobSplittingParameters()
            self.assertTrue(splitParams['deterministicPileup'])

            # step level checks
            task = workload.getTaskByName('GENSIMMergeRAWSIMoutput')
            self.assertEqual(task.getInputStep(), '/TestWorkload/GENSIM/cmsRun1')
            task = workload.getTaskByName('RECOMergeAODSIMoutput')
            self.assertEqual(task.getInputStep(), '/TestWorkload/GENSIM/cmsRun3')
            task = workload.getTaskByName('RECOMergeRECOSIMoutput')
            self.assertEqual(task.getInputStep(), '/TestWorkload/GENSIM/cmsRun3')
            task = workload.getTaskByName('RECOMergeDQMoutput')
            self.assertEqual(task.getInputStep(), '/TestWorkload/GENSIM/cmsRun3')
            return

        testArguments = StepChainWorkloadFactory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))
        testArguments['DeterministicPileup'] = True
        configDocs = injectStepChainConfigMC(self.configDatabase)
        for s in ['Step1', 'Step2', 'Step3']:
            testArguments[s]['ConfigCacheID'] = configDocs[s]
        testArguments['Step2']['KeepOutput'] = False

        factory = StepChainWorkloadFactory()

        # Case 1: workflow creation only
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)
        _checkInputData(testWorkload)

        # Case 2: workload assignment. Only the site whitelist is supposed to change
        assignDict = {"SiteWhitelist": ["T2_US_Nebraska", "T2_IT_Rome"], "Team": "The-A-Team"}
        testWorkload.updateArguments(assignDict)
        _checkInputData(testWorkload, sitewhitelist=["T2_US_Nebraska", "T2_IT_Rome"])

        return

    def testBadTrident(self):
        """
        Test a setup which is not supported. A request with the same output module AND
        datatier in different steps.
        Example would be a trident configuration where Step2 and Step3 read the output
        from Step1, producing the same datasets with a different CMSSW release (or GT).
        """
        testArguments = StepChainWorkloadFactory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))
        testArguments['Step3']['InputStep'] = testArguments['Step2']['InputStep']
        testArguments['Step3']['InputFromOutputModule'] = testArguments['Step2']['InputFromOutputModule']

        configDocs = injectStepChainConfigMC(self.configDatabase)
        testArguments['Step1']['ConfigCacheID'] = configDocs['Step1']
        testArguments['Step2']['ConfigCacheID'] = configDocs['Step3']
        testArguments['Step3']['ConfigCacheID'] = configDocs['Step3']

        factory = StepChainWorkloadFactory()

        self.assertRaises(WMSpecFactoryException, factory.factoryWorkloadConstruction,
                          "TestWorkload", testArguments)

    def testGoodTrident(self):
        """
        Test a trident request setup where steps don't define the same
        set of output module AND datatier.
        """
        testArguments = StepChainWorkloadFactory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))
        testArguments['Step3']['InputStep'] = testArguments['Step2']['InputStep']
        testArguments['Step3']['InputFromOutputModule'] = testArguments['Step2']['InputFromOutputModule']

        configDocs = injectStepChainConfigMC(self.configDatabase)
        testArguments['Step1']['ConfigCacheID'] = configDocs['Step1']
        testArguments['Step2']['ConfigCacheID'] = configDocs['Step2']
        testArguments['Step3']['ConfigCacheID'] = configDocs['Step3']

        factory = StepChainWorkloadFactory()
        factory.factoryWorkloadConstruction("TestWorkload", testArguments)

    def testMCFilesets(self):
        """
        Test workflow tasks, filesets and subscriptions creation
        """
        # expected tasks, filesets, subscriptions, etc
        expOutTasks = ['/TestWorkload/GENSIM',
                       '/TestWorkload/GENSIM/GENSIMMergeRAWSIMoutput',
                       '/TestWorkload/GENSIM/RECOMergeAODSIMoutput',
                       '/TestWorkload/GENSIM/RECOMergeDQMoutput',
                       '/TestWorkload/GENSIM/RECOMergeRECOSIMoutput']
        expWfTasks = ['/TestWorkload/GENSIM',
                      '/TestWorkload/GENSIM/GENSIMCleanupUnmergedRAWSIMoutput',
                      '/TestWorkload/GENSIM/GENSIMMergeRAWSIMoutput',
                      '/TestWorkload/GENSIM/GENSIMMergeRAWSIMoutput/GENSIMRAWSIMoutputMergeLogCollect',
                      '/TestWorkload/GENSIM/RECOCleanupUnmergedAODSIMoutput',
                      '/TestWorkload/GENSIM/RECOCleanupUnmergedDQMoutput',
                      '/TestWorkload/GENSIM/RECOCleanupUnmergedRECOSIMoutput',
                      '/TestWorkload/GENSIM/RECOMergeAODSIMoutput',
                      '/TestWorkload/GENSIM/RECOMergeAODSIMoutput/RECOAODSIMoutputMergeLogCollect',
                      '/TestWorkload/GENSIM/RECOMergeRECOSIMoutput',
                      '/TestWorkload/GENSIM/RECOMergeRECOSIMoutput/RECORECOSIMoutputMergeLogCollect',
                      '/TestWorkload/GENSIM/RECOMergeDQMoutput',
                      '/TestWorkload/GENSIM/RECOMergeDQMoutput/RECODQMoutputMergeLogCollect']
        expFsets = ['FILESET_DEFINED_DURING_RUNTIME',
                    '/TestWorkload/GENSIM/GENSIMMergeRAWSIMoutput/merged-logArchive',
                    '/TestWorkload/GENSIM/GENSIMMergeRAWSIMoutput/merged-MergedGEN-SIM',
                    '/TestWorkload/GENSIM/RECOMergeAODSIMoutput/merged-logArchive',
                    '/TestWorkload/GENSIM/RECOMergeAODSIMoutput/merged-MergedAODSIM',
                    '/TestWorkload/GENSIM/RECOMergeDQMoutput/merged-logArchive',
                    '/TestWorkload/GENSIM/RECOMergeDQMoutput/merged-MergedDQMIO',
                    '/TestWorkload/GENSIM/RECOMergeRECOSIMoutput/merged-logArchive',
                    '/TestWorkload/GENSIM/RECOMergeRECOSIMoutput/merged-MergedGEN-SIM-RECO',
                    '/TestWorkload/GENSIM/unmerged-AODSIMoutputAODSIM',
                    '/TestWorkload/GENSIM/unmerged-DQMoutputDQMIO',
                    '/TestWorkload/GENSIM/unmerged-RAWSIMoutputGEN-SIM',
                    '/TestWorkload/GENSIM/unmerged-RAWSIMoutputGEN-SIM-RAW',
                    '/TestWorkload/GENSIM/unmerged-RECOSIMoutputGEN-SIM-RECO',
                    '/TestWorkload/GENSIM/unmerged-logArchive']

        subMaps = ['REPLACE-ME: Fileset defined during runtime',
                   (4,
                     '/TestWorkload/GENSIM/GENSIMMergeRAWSIMoutput/merged-logArchive',
                     '/TestWorkload/GENSIM/GENSIMMergeRAWSIMoutput/GENSIMRAWSIMoutputMergeLogCollect',
                     'MinFileBased',
                     'LogCollect'),
                    (7,
                     '/TestWorkload/GENSIM/RECOMergeAODSIMoutput/merged-logArchive',
                     '/TestWorkload/GENSIM/RECOMergeAODSIMoutput/RECOAODSIMoutputMergeLogCollect',
                     'MinFileBased',
                     'LogCollect'),
                    (10,
                     '/TestWorkload/GENSIM/RECOMergeDQMoutput/merged-logArchive',
                     '/TestWorkload/GENSIM/RECOMergeDQMoutput/RECODQMoutputMergeLogCollect',
                     'MinFileBased',
                     'LogCollect'),
                    (13,
                     '/TestWorkload/GENSIM/RECOMergeRECOSIMoutput/merged-logArchive',
                     '/TestWorkload/GENSIM/RECOMergeRECOSIMoutput/RECORECOSIMoutputMergeLogCollect',
                     'MinFileBased',
                     'LogCollect'),
                    (5,
                     '/TestWorkload/GENSIM/unmerged-AODSIMoutputAODSIM',
                     '/TestWorkload/GENSIM/RECOCleanupUnmergedAODSIMoutput',
                     'SiblingProcessingBased',
                     'Cleanup'),
                    (6,
                     '/TestWorkload/GENSIM/unmerged-AODSIMoutputAODSIM',
                     '/TestWorkload/GENSIM/RECOMergeAODSIMoutput',
                     'ParentlessMergeBySize',
                     'Merge'),
                    (8,
                     '/TestWorkload/GENSIM/unmerged-DQMoutputDQMIO',
                     '/TestWorkload/GENSIM/RECOCleanupUnmergedDQMoutput',
                     'SiblingProcessingBased',
                     'Cleanup'),
                    (9,
                     '/TestWorkload/GENSIM/unmerged-DQMoutputDQMIO',
                     '/TestWorkload/GENSIM/RECOMergeDQMoutput',
                     'ParentlessMergeBySize',
                     'Merge'),
                    (2,
                     '/TestWorkload/GENSIM/unmerged-RAWSIMoutputGEN-SIM',
                     '/TestWorkload/GENSIM/GENSIMCleanupUnmergedRAWSIMoutput',
                     'SiblingProcessingBased',
                     'Cleanup'),
                    (3,
                     '/TestWorkload/GENSIM/unmerged-RAWSIMoutputGEN-SIM',
                     '/TestWorkload/GENSIM/GENSIMMergeRAWSIMoutput',
                     'ParentlessMergeBySize',
                     'Merge'),
                    (11,
                     '/TestWorkload/GENSIM/unmerged-RECOSIMoutputGEN-SIM-RECO',
                     '/TestWorkload/GENSIM/RECOCleanupUnmergedRECOSIMoutput',
                     'SiblingProcessingBased',
                     'Cleanup'),
                    (12,
                     '/TestWorkload/GENSIM/unmerged-RECOSIMoutputGEN-SIM-RECO',
                     '/TestWorkload/GENSIM/RECOMergeRECOSIMoutput',
                     'ParentlessMergeBySize',
                     'Merge')]

        testArguments = StepChainWorkloadFactory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))

        configDocs = injectStepChainConfigMC(self.configDatabase)
        for s in ['Step1', 'Step2', 'Step3']:
            testArguments[s]['ConfigCacheID'] = configDocs[s]
        testArguments['Step2']['KeepOutput'] = False

        factory = StepChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        myMask = Mask(FirstRun=1, FirstLumi=1, FirstEvent=1, LastRun=1, LastLumi=10, LastEvent=1000)
        testWMBSHelper = WMBSHelper(testWorkload, "GENSIM", mask=myMask,
                                    cachepath=self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        self.assertItemsEqual(testWorkload.listOutputProducingTasks(), expOutTasks)

        workflows = self.listTasksByWorkflow.execute(workflow="TestWorkload")
        self.assertItemsEqual([item['task'] for item in workflows], expWfTasks)

        # same function as in WMBSHelper, otherwise we cannot know which fileset name is
        maskString = ",".join(["%s=%s" % (x, myMask[x]) for x in sorted(myMask)])
        maskString = encodeUnicodeToBytesConditional(maskString, condition=PY3)
        topFilesetName = 'TestWorkload-GENSIM-%s' % md5(maskString).hexdigest()
        expFsets[0] = topFilesetName
        # returns a tuple of id, name, open and last_update
        filesets = self.listFilesets.execute()
        self.assertItemsEqual([item[1] for item in filesets], expFsets)

        subMaps[0] = (1, topFilesetName, '/TestWorkload/GENSIM', 'EventBased', 'Production')
        subscriptions = self.listSubsMapping.execute(workflow="TestWorkload", returnTuple=True)
        self.assertItemsEqual(subscriptions, subMaps)

        ### create another top level subscription
        myMask = Mask(FirstRun=1, FirstLumi=11, FirstEvent=1001, LastRun=1, LastLumi=20, LastEvent=2000)
        testWMBSHelper = WMBSHelper(testWorkload, "GENSIM", mask=myMask,
                                    cachepath=self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        workflows = self.listTasksByWorkflow.execute(workflow="TestWorkload")
        self.assertItemsEqual([item['task'] for item in workflows], expWfTasks)

        # same function as in WMBSHelper, otherwise we cannot know which fileset name is
        maskString = ",".join(["%s=%s" % (x, myMask[x]) for x in sorted(myMask)])
        maskString = encodeUnicodeToBytesConditional(maskString, condition=PY3)
        topFilesetName = 'TestWorkload-GENSIM-%s' % md5(maskString).hexdigest()
        expFsets.append(topFilesetName)
        # returns a tuple of id, name, open and last_update
        filesets = self.listFilesets.execute()
        self.assertItemsEqual([item[1] for item in filesets], expFsets)

        subMaps.append((14, topFilesetName, '/TestWorkload/GENSIM', 'EventBased', 'Production'))
        subscriptions = self.listSubsMapping.execute(workflow="TestWorkload", returnTuple=True)
        self.assertItemsEqual(subscriptions, subMaps)

    def testInputDataFilesets(self):
        """
        Test workflow tasks, filesets and subscriptions creation
        """
        # expected tasks, filesets, subscriptions, etc
        expOutTasks = ['/TestWorkload/StepOne',
                       '/TestWorkload/StepOne/StepOneMergeRAWSIMoutput',
                       '/TestWorkload/StepOne/StepThreeMergeAODSIMoutput',
                       '/TestWorkload/StepOne/StepThreeMergeDQMoutput',
                       '/TestWorkload/StepOne/StepThreeMergeRECOSIMoutput']
        expWfTasks = ['/TestWorkload/StepOne',
                      '/TestWorkload/StepOne/StepOneCleanupUnmergedRAWSIMoutput',
                      '/TestWorkload/StepOne/StepOneMergeRAWSIMoutput',
                      '/TestWorkload/StepOne/StepOneMergeRAWSIMoutput/StepOneRAWSIMoutputMergeLogCollect',
                      '/TestWorkload/StepOne/StepThreeCleanupUnmergedAODSIMoutput',
                      '/TestWorkload/StepOne/StepThreeCleanupUnmergedDQMoutput',
                      '/TestWorkload/StepOne/StepThreeCleanupUnmergedRECOSIMoutput',
                      '/TestWorkload/StepOne/StepThreeMergeDQMoutput',
                      '/TestWorkload/StepOne/StepThreeMergeDQMoutput/StepThreeDQMoutputMergeLogCollect',
                      '/TestWorkload/StepOne/StepThreeMergeAODSIMoutput',
                      '/TestWorkload/StepOne/StepThreeMergeAODSIMoutput/StepThreeAODSIMoutputMergeLogCollect',
                      '/TestWorkload/StepOne/StepThreeMergeRECOSIMoutput',
                      '/TestWorkload/StepOne/StepThreeMergeRECOSIMoutput/StepThreeRECOSIMoutputMergeLogCollect']
        expFsets = [
            'TestWorkload-StepOne-/BprimeJetToBZ_M800GeV_Tune4C_13TeV-madgraph-tauola/Fall13-POSTLS162_V1-v1/GEN-SIM#block1',
            '/TestWorkload/StepOne/StepOneMergeRAWSIMoutput/merged-logArchive',
            '/TestWorkload/StepOne/StepOneMergeRAWSIMoutput/merged-MergedGEN-SIM',
            '/TestWorkload/StepOne/StepThreeMergeAODSIMoutput/merged-logArchive',
            '/TestWorkload/StepOne/StepThreeMergeAODSIMoutput/merged-MergedAODSIM',
            '/TestWorkload/StepOne/StepThreeMergeDQMoutput/merged-logArchive',
            '/TestWorkload/StepOne/StepThreeMergeDQMoutput/merged-MergedDQMIO',
            '/TestWorkload/StepOne/unmerged-AODSIMoutputAODSIM',
            '/TestWorkload/StepOne/unmerged-DQMoutputDQMIO',
            '/TestWorkload/StepOne/unmerged-RAWSIMoutputGEN-SIM',
            '/TestWorkload/StepOne/unmerged-RAWSIMoutputGEN-SIM-RAW',
            '/TestWorkload/StepOne/StepThreeMergeRECOSIMoutput/merged-logArchive',
            '/TestWorkload/StepOne/StepThreeMergeRECOSIMoutput/merged-MergedGEN-SIM-RECO',
            '/TestWorkload/StepOne/unmerged-logArchive',
            '/TestWorkload/StepOne/unmerged-RECOSIMoutputGEN-SIM-RECO']
        subMaps = [(4,
                    '/TestWorkload/StepOne/StepOneMergeRAWSIMoutput/merged-logArchive',
                    '/TestWorkload/StepOne/StepOneMergeRAWSIMoutput/StepOneRAWSIMoutputMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (7,
                    '/TestWorkload/StepOne/StepThreeMergeAODSIMoutput/merged-logArchive',
                    '/TestWorkload/StepOne/StepThreeMergeAODSIMoutput/StepThreeAODSIMoutputMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (10,
                    '/TestWorkload/StepOne/StepThreeMergeDQMoutput/merged-logArchive',
                    '/TestWorkload/StepOne/StepThreeMergeDQMoutput/StepThreeDQMoutputMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (13,
                    '/TestWorkload/StepOne/StepThreeMergeRECOSIMoutput/merged-logArchive',
                    '/TestWorkload/StepOne/StepThreeMergeRECOSIMoutput/StepThreeRECOSIMoutputMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (5,
                    '/TestWorkload/StepOne/unmerged-AODSIMoutputAODSIM',
                    '/TestWorkload/StepOne/StepThreeCleanupUnmergedAODSIMoutput',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (6,
                    '/TestWorkload/StepOne/unmerged-AODSIMoutputAODSIM',
                    '/TestWorkload/StepOne/StepThreeMergeAODSIMoutput',
                    'ParentlessMergeBySize',
                    'Merge'),
                   (8,
                    '/TestWorkload/StepOne/unmerged-DQMoutputDQMIO',
                    '/TestWorkload/StepOne/StepThreeCleanupUnmergedDQMoutput',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (9,
                    '/TestWorkload/StepOne/unmerged-DQMoutputDQMIO',
                    '/TestWorkload/StepOne/StepThreeMergeDQMoutput',
                    'ParentlessMergeBySize',
                    'Merge'),
                   (2,
                    '/TestWorkload/StepOne/unmerged-RAWSIMoutputGEN-SIM',
                    '/TestWorkload/StepOne/StepOneCleanupUnmergedRAWSIMoutput',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (3,
                    '/TestWorkload/StepOne/unmerged-RAWSIMoutputGEN-SIM',
                    '/TestWorkload/StepOne/StepOneMergeRAWSIMoutput',
                    'ParentlessMergeBySize',
                    'Merge'),
                   (11,
                    '/TestWorkload/StepOne/unmerged-RECOSIMoutputGEN-SIM-RECO',
                    '/TestWorkload/StepOne/StepThreeCleanupUnmergedRECOSIMoutput',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (12,
                    '/TestWorkload/StepOne/unmerged-RECOSIMoutputGEN-SIM-RECO',
                    '/TestWorkload/StepOne/StepThreeMergeRECOSIMoutput',
                    'ParentlessMergeBySize',
                    'Merge'),
                   (1,
                    'TestWorkload-StepOne-/BprimeJetToBZ_M800GeV_Tune4C_13TeV-madgraph-tauola/Fall13-POSTLS162_V1-v1/GEN-SIM#block1',
                    '/TestWorkload/StepOne',
                    'EventAwareLumiBased',
                    'Processing')]

        testArguments = StepChainWorkloadFactory.getTestArguments()
        configDocs = injectStepChainConfigMC(self.configDatabase)
        testArguments.update(getThreeStepsOverride())
        for s in ['Step1', 'Step2', 'Step3']:
            testArguments[s]['ConfigCacheID'] = configDocs[s]
        testArguments['Step2']['KeepOutput'] = False

        factory = StepChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        testWMBSHelper = WMBSHelper(testWorkload, "StepOne",
                                    blockName=testArguments['Step1']['InputDataset'] + '#block1',
                                    cachepath=self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        self.assertItemsEqual(testWorkload.listOutputProducingTasks(), expOutTasks)

        workflows = self.listTasksByWorkflow.execute(workflow="TestWorkload")
        self.assertItemsEqual([item['task'] for item in workflows], expWfTasks)

        # returns a tuple of id, name, open and last_update
        filesets = self.listFilesets.execute()
        self.assertItemsEqual([item[1] for item in filesets], expFsets)

        subscriptions = self.listSubsMapping.execute(workflow="TestWorkload", returnTuple=True)
        self.assertItemsEqual(subscriptions, subMaps)

        ### create another top level subscription
        testWMBSHelper = WMBSHelper(testWorkload, "StepOne",
                                    blockName=testArguments['Step1']['InputDataset'] + '#block2',
                                    cachepath=self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        workflows = self.listTasksByWorkflow.execute(workflow="TestWorkload")
        self.assertItemsEqual([item['task'] for item in workflows], expWfTasks)

        # returns a tuple of id, name, open and last_update
        topFilesetName = 'TestWorkload-StepOne-/BprimeJetToBZ_M800GeV_Tune4C_13TeV-madgraph-tauola/Fall13-POSTLS162_V1-v1/GEN-SIM#block2'
        expFsets.append(topFilesetName)
        filesets = self.listFilesets.execute()
        self.assertItemsEqual([item[1] for item in filesets], expFsets)

        subMaps.append((14, topFilesetName, '/TestWorkload/StepOne', 'EventAwareLumiBased', 'Processing'))
        subscriptions = self.listSubsMapping.execute(workflow="TestWorkload", returnTuple=True)
        self.assertItemsEqual(subscriptions, subMaps)

    def testDupOutputModule(self):
        """
        Test a StepChain saving duplicate output module
        """
        expOutTasks = ['/TestWorkload/GENSIM',
                       '/TestWorkload/GENSIM/GENSIMMergeRAWSIMoutput',
                       '/TestWorkload/GENSIM/DIGIMergeRAWSIMoutput',
                       '/TestWorkload/GENSIM/RECOMergeAODSIMoutput',
                       '/TestWorkload/GENSIM/RECOMergeDQMoutput',
                       '/TestWorkload/GENSIM/RECOMergeRECOSIMoutput']
        expWfTasks = ['/TestWorkload/GENSIM',
                      '/TestWorkload/GENSIM/DIGICleanupUnmergedRAWSIMoutput',
                      '/TestWorkload/GENSIM/DIGIMergeRAWSIMoutput',
                      '/TestWorkload/GENSIM/DIGIMergeRAWSIMoutput/DIGIRAWSIMoutputMergeLogCollect',
                      '/TestWorkload/GENSIM/GENSIMCleanupUnmergedRAWSIMoutput',
                      '/TestWorkload/GENSIM/GENSIMMergeRAWSIMoutput',
                      '/TestWorkload/GENSIM/GENSIMMergeRAWSIMoutput/GENSIMRAWSIMoutputMergeLogCollect',
                      '/TestWorkload/GENSIM/RECOCleanupUnmergedAODSIMoutput',
                      '/TestWorkload/GENSIM/RECOCleanupUnmergedDQMoutput',
                      '/TestWorkload/GENSIM/RECOCleanupUnmergedRECOSIMoutput',
                      '/TestWorkload/GENSIM/RECOMergeDQMoutput',
                      '/TestWorkload/GENSIM/RECOMergeDQMoutput/RECODQMoutputMergeLogCollect',
                      '/TestWorkload/GENSIM/RECOMergeAODSIMoutput',
                      '/TestWorkload/GENSIM/RECOMergeAODSIMoutput/RECOAODSIMoutputMergeLogCollect',
                      '/TestWorkload/GENSIM/RECOMergeRECOSIMoutput',
                      '/TestWorkload/GENSIM/RECOMergeRECOSIMoutput/RECORECOSIMoutputMergeLogCollect']
        expFsets = ['FILESET_DEFINED_DURING_RUNTIME',
                    '/TestWorkload/GENSIM/DIGIMergeRAWSIMoutput/merged-MergedGEN-SIM-RAW',
                    '/TestWorkload/GENSIM/DIGIMergeRAWSIMoutput/merged-logArchive',
                    '/TestWorkload/GENSIM/GENSIMMergeRAWSIMoutput/merged-MergedGEN-SIM',
                    '/TestWorkload/GENSIM/GENSIMMergeRAWSIMoutput/merged-logArchive',
                    '/TestWorkload/GENSIM/RECOMergeAODSIMoutput/merged-MergedAODSIM',
                    '/TestWorkload/GENSIM/RECOMergeAODSIMoutput/merged-logArchive',
                    '/TestWorkload/GENSIM/RECOMergeDQMoutput/merged-MergedDQMIO',
                    '/TestWorkload/GENSIM/RECOMergeDQMoutput/merged-logArchive',
                    '/TestWorkload/GENSIM/RECOMergeRECOSIMoutput/merged-MergedGEN-SIM-RECO',
                    '/TestWorkload/GENSIM/RECOMergeRECOSIMoutput/merged-logArchive',
                    '/TestWorkload/GENSIM/unmerged-AODSIMoutputAODSIM',
                    '/TestWorkload/GENSIM/unmerged-DQMoutputDQMIO',
                    '/TestWorkload/GENSIM/unmerged-RAWSIMoutputGEN-SIM',
                    '/TestWorkload/GENSIM/unmerged-RAWSIMoutputGEN-SIM-RAW',
                    '/TestWorkload/GENSIM/unmerged-RECOSIMoutputGEN-SIM-RECO',
                    '/TestWorkload/GENSIM/unmerged-logArchive']
        # mapping of subscriptions to fileset and workflow task
        # subMaps = [
        subMaps = ['REPLACE-ME: Fileset defined during runtime',
                   (7,
                    '/TestWorkload/GENSIM/DIGIMergeRAWSIMoutput/merged-logArchive',
                    '/TestWorkload/GENSIM/DIGIMergeRAWSIMoutput/DIGIRAWSIMoutputMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (4,
                    '/TestWorkload/GENSIM/GENSIMMergeRAWSIMoutput/merged-logArchive',
                    '/TestWorkload/GENSIM/GENSIMMergeRAWSIMoutput/GENSIMRAWSIMoutputMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (10,
                    '/TestWorkload/GENSIM/RECOMergeAODSIMoutput/merged-logArchive',
                    '/TestWorkload/GENSIM/RECOMergeAODSIMoutput/RECOAODSIMoutputMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (13,
                    '/TestWorkload/GENSIM/RECOMergeDQMoutput/merged-logArchive',
                    '/TestWorkload/GENSIM/RECOMergeDQMoutput/RECODQMoutputMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (16,
                    '/TestWorkload/GENSIM/RECOMergeRECOSIMoutput/merged-logArchive',
                    '/TestWorkload/GENSIM/RECOMergeRECOSIMoutput/RECORECOSIMoutputMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (8,
                    '/TestWorkload/GENSIM/unmerged-AODSIMoutputAODSIM',
                    '/TestWorkload/GENSIM/RECOCleanupUnmergedAODSIMoutput',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (9,
                    '/TestWorkload/GENSIM/unmerged-AODSIMoutputAODSIM',
                    '/TestWorkload/GENSIM/RECOMergeAODSIMoutput',
                    'ParentlessMergeBySize',
                    'Merge'),
                   (11,
                    '/TestWorkload/GENSIM/unmerged-DQMoutputDQMIO',
                    '/TestWorkload/GENSIM/RECOCleanupUnmergedDQMoutput',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (12,
                    '/TestWorkload/GENSIM/unmerged-DQMoutputDQMIO',
                    '/TestWorkload/GENSIM/RECOMergeDQMoutput',
                    'ParentlessMergeBySize',
                    'Merge'),
                   (2,
                    '/TestWorkload/GENSIM/unmerged-RAWSIMoutputGEN-SIM',
                    '/TestWorkload/GENSIM/GENSIMCleanupUnmergedRAWSIMoutput',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (3,
                    '/TestWorkload/GENSIM/unmerged-RAWSIMoutputGEN-SIM',
                    '/TestWorkload/GENSIM/GENSIMMergeRAWSIMoutput',
                    'ParentlessMergeBySize',
                    'Merge'),
                   (5,
                    '/TestWorkload/GENSIM/unmerged-RAWSIMoutputGEN-SIM-RAW',
                    '/TestWorkload/GENSIM/DIGICleanupUnmergedRAWSIMoutput',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (6,
                    '/TestWorkload/GENSIM/unmerged-RAWSIMoutputGEN-SIM-RAW',
                    '/TestWorkload/GENSIM/DIGIMergeRAWSIMoutput',
                    'ParentlessMergeBySize',
                    'Merge'),
                   (14,
                    '/TestWorkload/GENSIM/unmerged-RECOSIMoutputGEN-SIM-RECO',
                    '/TestWorkload/GENSIM/RECOCleanupUnmergedRECOSIMoutput',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (15,
                    '/TestWorkload/GENSIM/unmerged-RECOSIMoutputGEN-SIM-RECO',
                    '/TestWorkload/GENSIM/RECOMergeRECOSIMoutput',
                    'ParentlessMergeBySize',
                    'Merge')]

        testArguments = StepChainWorkloadFactory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))

        configDocs = injectStepChainConfigMC(self.configDatabase)
        for s in ['Step1', 'Step2', 'Step3']:
            testArguments[s]['ConfigCacheID'] = configDocs[s]
        testArguments['Step3']['InputFromOutputModule'] = testArguments['Step2']['InputFromOutputModule']

        factory = StepChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        myMask = Mask(FirstRun=1, FirstLumi=1, FirstEvent=1, LastRun=1, LastLumi=10, LastEvent=1000)
        testWMBSHelper = WMBSHelper(testWorkload, "GENSIM", mask=myMask,
                                    cachepath=self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        self.assertItemsEqual(testWorkload.listOutputProducingTasks(), expOutTasks)

        workflows = self.listTasksByWorkflow.execute(workflow="TestWorkload")
        self.assertItemsEqual([item['task'] for item in workflows], expWfTasks)

        # same function as in WMBSHelper, otherwise we cannot know which fileset name is
        maskString = ",".join(["%s=%s" % (x, myMask[x]) for x in sorted(myMask)])
        maskString = encodeUnicodeToBytesConditional(maskString, condition=PY3)
        topFilesetName = 'TestWorkload-GENSIM-%s' % md5(maskString).hexdigest()
        expFsets[0] = topFilesetName
        # returns a tuple of id, name, open and last_update
        filesets = self.listFilesets.execute()
        self.assertItemsEqual([item[1] for item in filesets], expFsets)

        subMaps[0] = (1, topFilesetName, '/TestWorkload/GENSIM', 'EventBased', 'Production')
        subscriptions = self.listSubsMapping.execute(workflow="TestWorkload", returnTuple=True)
        self.assertItemsEqual(subscriptions, subMaps)

        ### create another top level subscription
        myMask = Mask(FirstRun=1, FirstLumi=11, FirstEvent=1001, LastRun=1, LastLumi=20, LastEvent=2000)
        testWMBSHelper = WMBSHelper(testWorkload, "GENSIM", mask=myMask,
                                    cachepath=self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        workflows = self.listTasksByWorkflow.execute(workflow="TestWorkload")
        self.assertItemsEqual([item['task'] for item in workflows], expWfTasks)

        # same function as in WMBSHelper, otherwise we cannot know which fileset name is
        maskString = ",".join(["%s=%s" % (x, myMask[x]) for x in sorted(myMask)])
        maskString = encodeUnicodeToBytesConditional(maskString, condition=PY3)
        topFilesetName = 'TestWorkload-GENSIM-%s' % md5(maskString).hexdigest()
        expFsets.append(topFilesetName)
        # returns a tuple of id, name, open and last_update
        filesets = self.listFilesets.execute()
        self.assertItemsEqual([item[1] for item in filesets], expFsets)

        subMaps.append((17, topFilesetName, '/TestWorkload/GENSIM', 'EventBased', 'Production'))
        subscriptions = self.listSubsMapping.execute(workflow="TestWorkload", returnTuple=True)
        self.assertItemsEqual(subscriptions, subMaps)

    def testStepParentageMapping1(self):
        """
        Build a 4-steps request
         *) with NO input dataset
         *) and only saving the output of the Step4
        and test the parentage mapping structure
        """
        outDsets = {
            "Step1": [],
            "Step2": [],
            "Step3": [],
            "Step4": ['/PrimaryDataset-StepChain/AcqEra_Step4-FilterD-ProcStr_Step4-v1/AODSIM',
                      '/PrimaryDataset-StepChain/AcqEra_Step4-ProcStr_Step4-v1/DQMIO',
                      '/PrimaryDataset-StepChain/AcqEra_Step4-FilterC-ProcStr_Step4-v1/GEN-SIM-RECO']
        }
        testArguments = StepChainWorkloadFactory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))
        testArguments['StepChain'] = 1
        testArguments['Step1']['ConfigCacheID'] = injectStepChainConfigSingle(self.configDatabase)
        testArguments['StepChain'] = 4

        # Create a new DIGI step in Step3 and shift Step3 to Step4
        testArguments['Step4'] = deepcopy(testArguments['Step3'])
        testArguments['Step3'] = {"InputFromOutputModule": "RAWSIMoutput",
                                  "InputStep": "GENSIM",
                                  "StepName": "DIGI2"}

        configDocs = injectStepChainConfigMC(self.configDatabase)
        for s in ['Step1', 'Step2', 'Step3', 'Step4']:
            testArguments[s]['ConfigCacheID'] = configDocs[s]
            testArguments[s]['KeepOutput'] = False
            testArguments[s]['AcquisitionEra'] = "AcqEra_" + s
            testArguments[s]['ProcessingString'] = "ProcStr_" + s
            testArguments[s]['GlobalTag'] = "GT-" + s
        testArguments['Step4']['KeepOutput'] = True

        # these are the inverse...
        testArguments['Step3']['ConfigCacheID'] = configDocs['Step4']
        testArguments['Step4']['ConfigCacheID'] = configDocs['Step3']

        factory = StepChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)
        parentageMapping = testWorkload.getStepParentageMapping()

        for i in range(1, testArguments['StepChain'] + 1):
            stepNumber = "Step%d" % i
            stepName = testArguments[stepNumber]['StepName']
            cmsRunNumber = "cmsRun%d" % i

            self.assertEqual(stepNumber, parentageMapping[stepName]['StepNumber'])
            self.assertEqual(cmsRunNumber, parentageMapping[stepName]['StepCmsRun'])
            self.assertEqual(testArguments[stepNumber].get('InputStep'), parentageMapping[stepName]['ParentStepName'])
            self.assertItemsEqual(outDsets[stepNumber], listvalues(parentageMapping[stepName]['OutputDatasetMap']))
            # request does not have InputDataset and we keep the output of the last step only
            self.assertEqual(None, parentageMapping[stepName]['ParentDataset'])

        self.assertItemsEqual(['AODSIMoutput', 'RECOSIMoutput', 'DQMoutput'],
                              list(parentageMapping['RECO']['OutputDatasetMap']))

    def testStepParentageMapping2(self):
        """
        Build a 4-steps request
         *) with input dataset
         *) and only saving the output of the Step4
        and test the parentage mapping structure
        """
        outDsets = {
            "Step1": [],
            "Step2": [],
            "Step3": [],
            "Step4": ['/PrimaryDataset-StepChain/AcqEra_Step4-FilterD-ProcStr_Step4-v1/AODSIM',
                      '/PrimaryDataset-StepChain/AcqEra_Step4-ProcStr_Step4-v1/DQMIO',
                      '/PrimaryDataset-StepChain/AcqEra_Step4-FilterC-ProcStr_Step4-v1/GEN-SIM-RECO']
        }
        testArguments = StepChainWorkloadFactory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))
        testArguments['StepChain'] = 1
        testArguments['Step1']['ConfigCacheID'] = injectStepChainConfigSingle(self.configDatabase)
        testArguments['Step1'][
            'InputDataset'] = "/BprimeJetToBZ_M800GeV_Tune4C_13TeV-madgraph-tauola/Fall13-POSTLS162_V1-v1/GEN-SIM"
        testArguments['StepChain'] = 4

        # Create a new DIGI step in Step3 and shift Step3 to Step4
        testArguments['Step4'] = deepcopy(testArguments['Step3'])
        testArguments['Step3'] = {"InputFromOutputModule": "RAWSIMoutput",
                                  "InputStep": "GENSIM",
                                  "StepName": "DIGI2"}

        configDocs = injectStepChainConfigMC(self.configDatabase)
        for s in ['Step1', 'Step2', 'Step3', 'Step4']:
            testArguments[s]['ConfigCacheID'] = configDocs[s]
            testArguments[s]['KeepOutput'] = False
            testArguments[s]['AcquisitionEra'] = "AcqEra_" + s
            testArguments[s]['ProcessingString'] = "ProcStr_" + s
            testArguments[s]['GlobalTag'] = "GT-" + s
        testArguments['Step4']['KeepOutput'] = True

        # these are the inverse...
        testArguments['Step3']['ConfigCacheID'] = configDocs['Step4']
        testArguments['Step4']['ConfigCacheID'] = configDocs['Step3']

        factory = StepChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)
        parentageMapping = testWorkload.getStepParentageMapping()

        for i in range(1, testArguments['StepChain'] + 1):
            stepNumber = "Step%d" % i
            stepName = testArguments[stepNumber]['StepName']
            cmsRunNumber = "cmsRun%d" % i

            self.assertEqual(stepNumber, parentageMapping[stepName]['StepNumber'])
            self.assertEqual(cmsRunNumber, parentageMapping[stepName]['StepCmsRun'])
            self.assertEqual(testArguments[stepNumber].get('InputStep'), parentageMapping[stepName]['ParentStepName'])
            self.assertItemsEqual(outDsets[stepNumber], listvalues(parentageMapping[stepName]['OutputDatasetMap']))
            self.assertEqual('/BprimeJetToBZ_M800GeV_Tune4C_13TeV-madgraph-tauola/Fall13-POSTLS162_V1-v1/GEN-SIM',
                             parentageMapping[stepName]['ParentDataset'])

        self.assertItemsEqual(['AODSIMoutput', 'RECOSIMoutput', 'DQMoutput'],
                              list(parentageMapping['RECO']['OutputDatasetMap']))

    def testStepParentageMapping3(self):
        """
        Build a 4-steps request
         *) with NO input dataset
         *) and saving the output of Step1 and Step3 and Step4
        and test the parentage mapping structure
        """
        outDsets = {
            "Step1": ['/PrimaryDataset-StepChain/AcqEra_Step1-FilterA-ProcStr_Step1-v1/GEN-SIM'],
            "Step2": [],
            "Step3": ['/PrimaryDataset-StepChain/AcqEra_Step3-ProcStr_Step3-v1/GEN-SIM-RAW'],
            "Step4": ['/PrimaryDataset-StepChain/AcqEra_Step4-FilterD-ProcStr_Step4-v1/AODSIM',
                      '/PrimaryDataset-StepChain/AcqEra_Step4-ProcStr_Step4-v1/DQMIO',
                      '/PrimaryDataset-StepChain/AcqEra_Step4-FilterC-ProcStr_Step4-v1/GEN-SIM-RECO']
        }
        testArguments = StepChainWorkloadFactory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))
        testArguments['StepChain'] = 1
        testArguments['Step1']['ConfigCacheID'] = injectStepChainConfigSingle(self.configDatabase)
        testArguments['StepChain'] = 4

        # Create a new DIGI step in Step3 and shift Step3 to Step4
        testArguments['Step4'] = deepcopy(testArguments['Step3'])
        testArguments['Step4']['InputFromOutputModule'] = 'RAWSIMoutput'
        testArguments['Step3'] = {"InputFromOutputModule": "RAWSIMoutput",
                                  "InputStep": "GENSIM",
                                  "StepName": "DIGI2"}

        configDocs = injectStepChainConfigMC(self.configDatabase)
        for s in ['Step1', 'Step2', 'Step3', 'Step4']:
            testArguments[s]['ConfigCacheID'] = configDocs[s]
            testArguments[s]['KeepOutput'] = True
            testArguments[s]['AcquisitionEra'] = "AcqEra_" + s
            testArguments[s]['ProcessingString'] = "ProcStr_" + s
            testArguments[s]['GlobalTag'] = "GT-" + s
        testArguments['Step2']['KeepOutput'] = False

        # these are the inverse...
        testArguments['Step3']['ConfigCacheID'] = configDocs['Step4']
        testArguments['Step4']['ConfigCacheID'] = configDocs['Step3']

        factory = StepChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)
        parentageMapping = testWorkload.getStepParentageMapping()

        for i in range(1, testArguments['StepChain'] + 1):
            stepNumber = "Step%d" % i
            stepName = testArguments[stepNumber]['StepName']
            cmsRunNumber = "cmsRun%d" % i
            self.assertEqual(stepNumber, parentageMapping[stepName]['StepNumber'])
            self.assertEqual(cmsRunNumber, parentageMapping[stepName]['StepCmsRun'])
            self.assertEqual(testArguments[stepNumber].get('InputStep'), parentageMapping[stepName]['ParentStepName'])
            self.assertItemsEqual(outDsets[stepNumber], listvalues(parentageMapping[stepName]['OutputDatasetMap']))

        # test parentage dataset
        self.assertEqual(None, parentageMapping['GENSIM']['ParentDataset'])
        parentDset = '/PrimaryDataset-StepChain/AcqEra_Step1-FilterA-ProcStr_Step1-v1/GEN-SIM'
        self.assertEqual(parentDset, parentageMapping['DIGI']['ParentDataset'])
        self.assertEqual(parentDset, parentageMapping['DIGI2']['ParentDataset'])
        self.assertEqual(parentDset, parentageMapping['RECO']['ParentDataset'])

        # test output modules, only Step2 not saving the output
        self.assertEqual(['RAWSIMoutput'], list(parentageMapping['GENSIM']['OutputDatasetMap']))
        self.assertEqual(['RAWSIMoutput'], list(parentageMapping['DIGI2']['OutputDatasetMap']))
        self.assertItemsEqual(['AODSIMoutput', 'RECOSIMoutput', 'DQMoutput'],
                              list(parentageMapping['RECO']['OutputDatasetMap']))

    def testStepParentageMapping4(self):
        """
        Same logic as testStepParentageMapping3, but also checking the mapping
        after the workflow gets assigned. Request is
         *) with NO input dataset
         *) and saving the output of Step1 and Step3 and Step4
        and test the parentage mapping structure
        """
        outDsets = {
            "Step1": ['/PrimaryDataset-StepChain/AcqEraNew_Step1-FilterA-ProcStrNew_Step1-v1/GEN-SIM'],
            "Step2": [],
            "Step3": ['/PrimaryDataset-StepChain/AcqEraNew_Step3-ProcStrNew_Step3-v1/GEN-SIM-RAW'],
            "Step4": ['/PrimaryDataset-StepChain/AcqEraNew_Step4-FilterD-ProcStrNew_Step4-v1/AODSIM',
                      '/PrimaryDataset-StepChain/AcqEraNew_Step4-ProcStrNew_Step4-v1/DQMIO',
                      '/PrimaryDataset-StepChain/AcqEraNew_Step4-FilterC-ProcStrNew_Step4-v1/GEN-SIM-RECO']
        }
        testArguments = StepChainWorkloadFactory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))
        testArguments['StepChain'] = 1
        testArguments['Step1']['ConfigCacheID'] = injectStepChainConfigSingle(self.configDatabase)
        testArguments['StepChain'] = 4

        # Create a new DIGI step in Step3 and shift Step3 to Step4
        testArguments['Step4'] = deepcopy(testArguments['Step3'])
        testArguments['Step4']['InputFromOutputModule'] = 'RAWSIMoutput'
        testArguments['Step3'] = {"InputFromOutputModule": "RAWSIMoutput",
                                  "InputStep": "GENSIM",
                                  "StepName": "DIGI2"}

        configDocs = injectStepChainConfigMC(self.configDatabase)
        for s in ['Step1', 'Step2', 'Step3', 'Step4']:
            testArguments[s]['ConfigCacheID'] = configDocs[s]
            testArguments[s]['KeepOutput'] = True
            testArguments[s]['AcquisitionEra'] = "AcqEra_" + s
            testArguments[s]['ProcessingString'] = "ProcStr_" + s
            testArguments[s]['GlobalTag'] = "GT-" + s
        testArguments['Step2']['KeepOutput'] = False

        # these are the inverse...
        testArguments['Step3']['ConfigCacheID'] = configDocs['Step4']
        testArguments['Step4']['ConfigCacheID'] = configDocs['Step3']

        factory = StepChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        assignDict = {"SiteWhitelist": ["T2_US_Nebraska", "T2_IT_Rome"], "Team": "The-A-Team",
                      "AcquisitionEra": {"GENSIM": "AcqEraNew_Step1", "DIGI": "AcqEraNew_Step2",
                                         "DIGI2": "AcqEraNew_Step3", "RECO": "AcqEraNew_Step4"},
                      "ProcessingString": {"GENSIM": "ProcStrNew_Step1", "DIGI": "ProcStrNew_Step2",
                                           "DIGI2": "ProcStrNew_Step3", "RECO": "ProcStrNew_Step4"},
                      "MergedLFNBase": "/store/data",
                      "UnmergedLFNBase": "/store/unmerged"
                      }
        testWorkload.updateArguments(assignDict)
        parentageMapping = testWorkload.getStepParentageMapping()

        for i in range(1, testArguments['StepChain'] + 1):
            stepNumber = "Step%d" % i
            stepName = testArguments[stepNumber]['StepName']
            cmsRunNumber = "cmsRun%d" % i
            self.assertEqual(stepNumber, parentageMapping[stepName]['StepNumber'])
            self.assertEqual(cmsRunNumber, parentageMapping[stepName]['StepCmsRun'])
            self.assertEqual(testArguments[stepNumber].get('InputStep'), parentageMapping[stepName]['ParentStepName'])
            self.assertItemsEqual(outDsets[stepNumber], listvalues(parentageMapping[stepName]['OutputDatasetMap']))

        # test parentage dataset
        self.assertEqual(None, parentageMapping['GENSIM']['ParentDataset'])
        parentDset = '/PrimaryDataset-StepChain/AcqEraNew_Step1-FilterA-ProcStrNew_Step1-v1/GEN-SIM'
        self.assertEqual(parentDset, parentageMapping['DIGI']['ParentDataset'])
        self.assertEqual(parentDset, parentageMapping['DIGI2']['ParentDataset'])
        self.assertEqual(parentDset, parentageMapping['RECO']['ParentDataset'])

        # test output modules, only Step2 not saving the output
        self.assertEqual(['RAWSIMoutput'], list(parentageMapping['GENSIM']['OutputDatasetMap']))
        self.assertEqual(['RAWSIMoutput'], list(parentageMapping['DIGI2']['OutputDatasetMap']))
        self.assertItemsEqual(['AODSIMoutput', 'RECOSIMoutput', 'DQMoutput'],
                              list(parentageMapping['RECO']['OutputDatasetMap']))

    def testTooManySteps(self):
        """
        Test that requests with more than 10 steps cannot be injected
        """
        factory = StepChainWorkloadFactory()

        testArguments = factory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))

        # now add 8 extra steps to the request
        for i in range(4, 12):
            stepNumber = "Step%s" % i
            testArguments[stepNumber] = deepcopy(testArguments['Step3'])
            testArguments['TaskName'] = "my%s" % stepNumber
        testArguments['StepChain'] = 11

        with self.assertRaises(WMSpecFactoryException):
            factory.factoryWorkloadConstruction("ElevenSteps", testArguments)

    def testWQStartPolicy(self):
        """
        Test workqueue start policy settings based on the input dataset
        """
        testArguments = StepChainWorkloadFactory.getTestArguments()
        testArguments.update(getSingleStepOverride())
        testArguments['Step1']['ConfigCacheID'] = injectStepChainConfigSingle(self.configDatabase),
        if isinstance(testArguments['Step1']['ConfigCacheID'], tuple):
            testArguments['Step1']['ConfigCacheID'] = testArguments['Step1']['ConfigCacheID'][0]

        testArguments['Step1']['InputDataset'] = "/MinBias_TuneCP5_13TeV-pythia8_pilot/RunIIFall18MiniAOD-pilot_102X_upgrade2018_realistic_v11-v1/MINIAODSIM"
        factory = StepChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        # MINIAODSIM datatier requires "Dataset" start policy
        self.assertEqual(testWorkload.startPolicyParameters()['policyName'], "Dataset")

        # MINIAOD and other datatiers require "Block" start policy
        testArguments['Step1']['InputDataset'] = "/JetHT/Run2022B-PromptReco-v1/MINIAOD"
        factory = StepChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)
        self.assertEqual(testWorkload.startPolicyParameters()['policyName'], "Block")

        # no input data, that means MonteCarlo policy
        testArguments['Step1'].pop('InputDataset', None)
        testArguments['Step1']['PrimaryDataset'] = "Test"
        testArguments['Step1']['RequestNumEvents'] = 123
        factory = StepChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)
        self.assertEqual(testWorkload.startPolicyParameters()['policyName'], "MonteCarlo")


if __name__ == '__main__':
    unittest.main()
