#!/usr/bin/env python

"""
_TaskChain_t_

Created by Dave Evans on 2011-06-21.
Copyright (c) 2011 Fermilab. All rights reserved.
"""

import json
import os
import unittest

from WMCore.WMSpec.StdSpecs.TaskChain import TaskChainWorkloadFactory
from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.Database.CMSCouch import CouchServer, Document
from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMSpec.WMSpecErrors import WMSpecFactoryException

def getTestFile(partialPath):
    """
    Returns the absolute path for the test json file
    """
    normPath = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))
    return os.path.join(normPath, partialPath)

def makeGeneratorConfig(couchDatabase):
    """
    _makeGeneratorConfig_

    Create a bogus config cache document for the montecarlo generation and
    inject it into couch.  Return the ID of the document.

    """
    newConfig = Document()
    newConfig["info"] = None
    newConfig["config"] = None
    newConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e580f"
    newConfig["pset_hash"] = "7c856ad35f9f544839d8525ca10259a7"
    newConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
    newConfig["pset_tweak_details"] = {"process": {"outputModules_": ["writeGENSIM"],
                                                  "writeGENSIM": {"dataset": {"filterName": "GenSimFilter",
                                                                          "dataTier": "GEN-SIM"}}}}
    result = couchDatabase.commitOne(newConfig)
    return result[0]["id"]



def makeProcessingConfigs(couchDatabase):
    """
    _makeProcessingConfigs_

    Make a bunch of processing configs in couch for a processing chain consisting of

    DigiHLT - Reco - ALCAReco - Skims

    returns a map of config names to IDs

    """
    rawConfig = Document()
    rawConfig["info"] = None
    rawConfig["config"] = None
    rawConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e234f"
    rawConfig["pset_hash"] = "7c856ad35f9f544839d8525ca10876a7"
    rawConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
    rawConfig["pset_tweak_details"] = {"process": {"outputModules_": ["writeRAWDIGI", "writeRAWDEBUGDIGI"],
                                                  "writeRAWDIGI": {"dataset": {"filterName": "RawDigiFilter",
                                                                          "dataTier": "RAW-DIGI"}},
                                                  "writeRAWDEBUGDIGI" : {"dataset": {"filterName": "RawDebugDigiFilter",
                                                                          "dataTier": "RAW-DEBUG-DIGI"}}}}
    recoConfig = Document()
    recoConfig["info"] = None
    recoConfig["config"] = None
    recoConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e736f"
    recoConfig["pset_hash"] = "7c856ad35f9f544839d8525ca11765a7"
    recoConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
    recoConfig["pset_tweak_details"] = {
       "process": {"outputModules_": ["writeRECO", "writeAOD", "writeALCA"],
                   "writeRECO": {"dataset": {"dataTier": "RECO", "filterName" : "reco"}},
                   "writeAOD":  {"dataset": {"dataTier": "AOD", "filterName" : "AOD"}},
                   "writeALCA":  {"dataset": {"dataTier": "ALCARECO", "filterName" : "alca"}},
                }
        }
    alcaConfig = Document()
    alcaConfig["info"] = None
    alcaConfig["config"] = None
    alcaConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e826a"
    alcaConfig["pset_hash"] = "7c856ad35f9f544839d8525ca53628a7"
    alcaConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
    alcaConfig["pset_tweak_details"] = {
       "process": {"outputModules_": ["writeALCA1", "writeALCA2", "writeALCA3", "writeALCA4"],
                   "writeALCA1": {"dataset": {"dataTier": "ALCARECO", "filterName": "alca1"}},
                   "writeALCA2":  {"dataset": {"dataTier": "ALCARECO", "filterName": "alca2"}},
                   "writeALCA3":  {"dataset": {"dataTier": "ALCARECO", "filterName": "alca3"}},
                   "writeALCA4":  {"dataset": {"dataTier": "ALCARECO", "filterName": "alca4"}},
                }
        }

    skimsConfig = Document()
    skimsConfig["info"] = None
    skimsConfig["config"] = None
    skimsConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5cab278a"
    skimsConfig["pset_hash"] = "7c856ad35f9f544839d8524ca53728a6"
    skimsConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
    skimsConfig["pset_tweak_details"] = {
       "process": {"outputModules_": ["writeSkim1", "writeSkim2", "writeSkim3", "writeSkim4", "writeSkim5"],
                   "writeSkim1": {"dataset": {"dataTier":  "RECO-AOD", "filterName": "skim1"}},
                   "writeSkim2":  {"dataset": {"dataTier": "RECO-AOD", "filterName": "skim2"}},
                   "writeSkim3":  {"dataset": {"dataTier": "RECO-AOD", "filterName": "skim3"}},
                   "writeSkim4":  {"dataset": {"dataTier": "RECO-AOD", "filterName": "skim4"}},
                   "writeSkim5":  {"dataset": {"dataTier": "RECO-AOD", "filterName": "skim5"}},
                }
        }
    couchDatabase.queue(rawConfig)
    couchDatabase.queue(recoConfig)
    couchDatabase.queue(alcaConfig)
    couchDatabase.queue(skimsConfig)
    result = couchDatabase.commit()

    docMap = {
        "DigiHLT" : result[0][u'id'],
        "Reco"    : result[1][u'id'],
        "ALCAReco": result[2][u'id'],
        "Skims"   : result[3][u'id'],
    }
    return docMap

def outputModuleList(task):
    """
    _outputModuleList_

    util to return list of output module names

    """
    result = []
    for om in task.getOutputModulesForTask():
        mods = om.listSections_()
        result.extend([str(x) for x in mods])
    return result

class TaskChainTests(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Initialize the database and couch.

        """
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("taskchain_t", "ConfigCache")
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)

        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("taskchain_t")
        self.testInit.generateWorkDir()
        self.workload = None

        self.differentNCores = getTestFile('data/ReqMgr/requests/Integration/TaskChain_RelVal_Multicore.json')
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

    def testGeneratorWorkflow(self):
        """
        _testGeneratorWorkflow_
        Test creating a request with an initial generator task
        it mocks a request where there are 2 similar paths starting
        from the generator, each one with a different PrimaryDataset, CMSSW configuration
        and processed dataset. Dropping the RAW output as well.
        Also include an ignored output module to keep things interesting...
        """
        generatorDoc = makeGeneratorConfig(self.configDatabase)
        processorDocs = makeProcessingConfigs(self.configDatabase)

        testArguments = TaskChainWorkloadFactory.getTestArguments()
        arguments = {
            "AcquisitionEra": "ReleaseValidation",
            "Requestor": "sfoulkes@fnal.gov",
            "CMSSWVersion": "CMSSW_3_5_8",
            "ScramArch": "slc5_ia32_gcc434",
            "ProcessingVersion": 1,
            "GlobalTag": "GR10_P_v4::All",
            "CouchURL": self.testInit.couchUrl,
            "CouchDBName": self.testInit.couchDbName,
            "SiteWhitelist" : ["T1_CH_CERN", "T1_US_FNAL"],
            "DashboardHost": "127.0.0.1",
            "DashboardPort": 8884,
            "TaskChain" : 6,
            "IgnoredOutputModules" : ["writeSkim2", "writeRAWDEBUGDIGI"],
            "Task1" :{
                "TaskName" : "GenSim",
                "ConfigCacheID" : generatorDoc,
                "SplittingAlgo"  : "EventBased",
                "RequestNumEvents" : 10000,
                "Seeding" : "AutomaticSeeding",
                "PrimaryDataset" : "RelValTTBar",
            },
            "Task2" : {
                "TaskName" : "DigiHLT_new",
                "InputTask" : "GenSim",
                "InputFromOutputModule" : "writeGENSIM",
                "ConfigCacheID" : processorDocs['DigiHLT'],
                "SplittingAlgo" : "LumiBased",
                "CMSSWVersion" : "CMSSW_5_2_6",
                "GlobalTag" : "GR_39_P_V5:All",
                "PrimaryDataset" : "PURelValTTBar",
                "KeepOutput" : False
            },
            "Task3" : {
                "TaskName" : "DigiHLT_ref",
                "InputTask" : "GenSim",
                "InputFromOutputModule" : "writeGENSIM",
                "ConfigCacheID" : processorDocs['DigiHLT'],
                "SplittingAlgo" : "EventBased",
                "CMSSWVersion" : "CMSSW_5_2_7",
                "GlobalTag" : "GR_40_P_V5:All",
                "AcquisitionEra" : "ReleaseValidationNewConditions",
                "ProcessingVersion" : 3,
                "ProcessingString" : "Test",
                "KeepOutput" : False
            },
            "Task4" : {
                "TaskName" : "Reco",
                "InputTask" : "DigiHLT_new",
                "InputFromOutputModule" : "writeRAWDIGI",
                "ConfigCacheID" : processorDocs['Reco'],
                "SplittingAlgo" : "FileBased",
                "TransientOutputModules" : ["writeRECO"]
            },
            "Task5" : {
                "TaskName" : "ALCAReco",
                "InputTask" : "DigiHLT_ref",
                "InputFromOutputModule" : "writeRAWDIGI",
                "ConfigCacheID" : processorDocs['ALCAReco'],
                "SplittingAlgo" : "LumiBased",

            },
            "Task6" : {
                "TaskName" : "Skims",
                "InputTask" : "Reco",
                "InputFromOutputModule" : "writeRECO",
                "ConfigCacheID" : processorDocs['Skims'],
                "SplittingAlgo" : "LumiBased",

            }

        }
        testArguments.update(arguments)
        arguments = testArguments

        factory = TaskChainWorkloadFactory()

        # Test a malformed task chain definition
        arguments['Task4']['TransientOutputModules'].append('writeAOD')
        self.assertRaises(WMSpecFactoryException, factory.validateSchema, arguments)

        arguments['Task4']['TransientOutputModules'].remove('writeAOD')
        try:
            self.workload = factory.factoryWorkloadConstruction("PullingTheChain", arguments)
        except Exception as ex:
            msg = "Error invoking TaskChainWorkloadFactory:\n%s" % str(ex)
            import traceback
            traceback.print_exc()
            self.fail(msg)

        testWMBSHelper = WMBSHelper(self.workload, "GenSim", "SomeBlock", cachepath = self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        firstTask = self.workload.getTaskByPath("/PullingTheChain/GenSim")

        self._checkTask(firstTask, arguments['Task1'], arguments)
        self._checkTask(self.workload.getTaskByPath("/PullingTheChain/GenSim/GenSimMergewriteGENSIM/DigiHLT_new"), arguments['Task2'], arguments)
        self._checkTask(self.workload.getTaskByPath("/PullingTheChain/GenSim/GenSimMergewriteGENSIM/DigiHLT_ref"), arguments['Task3'], arguments)
        self._checkTask(self.workload.getTaskByPath("/PullingTheChain/GenSim/GenSimMergewriteGENSIM/DigiHLT_new/Reco"),
                        arguments['Task4'], arguments)
        self._checkTask(self.workload.getTaskByPath("/PullingTheChain/GenSim/GenSimMergewriteGENSIM/DigiHLT_ref/ALCAReco"),
                        arguments['Task5'], arguments)
        self._checkTask(self.workload.getTaskByPath("/PullingTheChain/GenSim/GenSimMergewriteGENSIM/DigiHLT_new/Reco/Skims"),
                        arguments['Task6'], arguments)

        # Verify the output datasets
        outputDatasets = self.workload.listOutputDatasets()
        self.assertEqual(len(outputDatasets), 11, "Number of output datasets doesn't match")
        self.assertTrue("/RelValTTBar/ReleaseValidation-GenSimFilter-v1/GEN-SIM" in outputDatasets,
                        "/RelValTTBar/ReleaseValidation-GenSimFilter-v1/GEN-SIM not in output datasets")
        self.assertFalse("/RelValTTBar/ReleaseValidation-reco-v1/RECO" in outputDatasets,
                        "/RelValTTBar/ReleaseValidation-reco-v1/RECO in output datasets")
        self.assertTrue("/RelValTTBar/ReleaseValidation-AOD-v1/AOD" in outputDatasets,
                        "/RelValTTBar/ReleaseValidation-AOD-v1/AOD not in output datasets")
        self.assertTrue("/RelValTTBar/ReleaseValidation-alca-v1/ALCARECO" in outputDatasets,
                        "/RelValTTBar/ReleaseValidation-alca-v1/ALCARECO not in output datasets")
        for i in range(1, 5):
            self.assertTrue("/RelValTTBar/ReleaseValidation-alca%d-v1/ALCARECO" % i in outputDatasets,
                            "/RelValTTBar/ReleaseValidation-alca%d-v1/ALCARECO not in output datasets" % i)
        for i in range(1, 6):
            if i == 2:
                continue
            self.assertTrue("/RelValTTBar/ReleaseValidation-skim%d-v1/RECO-AOD" % i in outputDatasets,
                            "/RelValTTBar/ReleaseValidation-skim%d-v1/RECO-AOD not in output datasets" % i)

        return


    def _checkTask(self, task, taskConf, centralConf):
        """
        _checkTask_

        Verify the correctness of the task

        """
        if taskConf.get("InputTask") is not None:
            inpTaskPath = task.getPathName()
            inpTaskPath = inpTaskPath.replace(task.name(), "")
            inpTaskPath += "cmsRun1"
            self.assertEqual(task.data.input.inputStep, inpTaskPath, "Input step is wrong in the spec")
            self.assertTrue(taskConf["InputTask"] in inpTaskPath, "Input task is not in the path name for child task")

        if "MCPileup" in taskConf or "DataPileup" in taskConf:
            mcDataset = taskConf.get('MCPileup', None)
            dataDataset = taskConf.get('DataPileup', None)
            if mcDataset:
                self.assertEqual(task.data.steps.cmsRun1.pileup.mc.dataset, [mcDataset])
            if dataDataset:
                self.assertEqual(task.data.steps.cmsRun1.pileup.data.dataset, [dataDataset])

        workflow = Workflow(name = self.workload.name(),
                            task = task.getPathName())
        workflow.load()

        outputMods = outputModuleList(task)
        ignoredOutputMods = task.getIgnoredOutputModulesForTask()
        outputMods = set(outputMods) - set(ignoredOutputMods)
        self.assertEqual(len(workflow.outputMap.keys()), len(outputMods),
                         "Error: Wrong number of WF outputs")

        for outputModule in outputMods:
            filesets = workflow.outputMap[outputModule][0]
            merged = filesets['merged_output_fileset']
            unmerged = filesets['output_fileset']

            merged.loadData()
            unmerged.loadData()

            mergedset = task.getPathName() + "/" + task.name() + "Merge" + outputModule + "/merged-Merged"
            if outputModule == "logArchive" or not taskConf.get("KeepOutput", True) \
                or outputModule in taskConf.get("TransientOutputModules", []) or outputModule in centralConf.get("IgnoredOutputModules", []):
                mergedset = task.getPathName() + "/unmerged-" + outputModule
            unmergedset = task.getPathName() + "/unmerged-" + outputModule

            self.assertEqual(mergedset, merged.name, "Merged fileset name is wrong")
            self.assertEqual(unmergedset, unmerged.name, "Unmerged fileset name  is wrong")

            if outputModule != "logArchive" and taskConf.get("KeepOutput", True) \
                and outputModule not in taskConf.get("TransientOutputModules", []) \
                and outputModule not in centralConf.get("IgnoredOutputModules", []):
                mergeTask = task.getPathName() + "/" + task.name() + "Merge" + outputModule

                mergeWorkflow = Workflow(name = self.workload.name(),
                                         task = mergeTask)
                mergeWorkflow.load()
                self.assertTrue("Merged" in mergeWorkflow.outputMap, "Merge workflow does not contain a Merged output key")
                mergedOutputMod = mergeWorkflow.outputMap['Merged'][0]
                mergedFileset = mergedOutputMod['merged_output_fileset']
                unmergedFileset = mergedOutputMod['output_fileset']
                mergedFileset.loadData()
                unmergedFileset.loadData()
                self.assertEqual(mergedFileset.name, mergedset, "Merged fileset name in merge task is wrong")
                self.assertEqual(unmergedFileset.name, mergedset, "Unmerged fileset name in merge task is wrong")

                mrgLogArch = mergeWorkflow.outputMap['logArchive'][0]['merged_output_fileset']
                umrgLogArch = mergeWorkflow.outputMap['logArchive'][0]['output_fileset']
                mrgLogArch.loadData()
                umrgLogArch.loadData()

                archName = task.getPathName() + "/" + task.name() + "Merge" + outputModule + "/merged-logArchive"

                self.assertEqual(mrgLogArch.name, archName, "LogArchive merged fileset name is wrong in merge task")
                self.assertEqual(umrgLogArch.name, archName, "LogArchive unmerged fileset name is wrong in merge task")

            if outputModule != "logArchive":
                taskOutputMods = task.getOutputModulesForStep(stepName = "cmsRun1")
                currentModule = getattr(taskOutputMods, outputModule)
                if taskConf.get("PrimaryDataset") is not None:
                    self.assertEqual(currentModule.primaryDataset, taskConf["PrimaryDataset"], "Wrong primary dataset")
                processedDatasetParts = ["AcquisitionEra, ProcessingString, ProcessingVersion"]
                allParts = True
                for part in processedDatasetParts:
                    if part in taskConf:
                        self.assertTrue(part in currentModule.processedDataset, "Wrong processed dataset for module")
                    else:
                        allParts = False
                if allParts:
                    self.assertEqual("%s-%s-v%s" % (taskConf["AcquisitionEra"], taskConf["ProcessingString"],
                                                   taskConf["ProcessingVersion"]), "Wrong processed dataset for module")

        # Test subscriptions
        if taskConf.get("InputTask") is None:
            inputFileset = "%s-%s-SomeBlock" % (self.workload.name(), task.name())
        elif "Merge" in task.getPathName().split("/")[-2]:
            inpTaskPath = task.getPathName().replace(task.name(), "")
            inputFileset = inpTaskPath + "merged-Merged"
        else:
            inpTaskPath = task.getPathName().replace(task.name(), "")
            inputFileset = inpTaskPath + "unmerged-%s" % taskConf["InputFromOutputModule"]
        taskFileset = Fileset(name = inputFileset)
        taskFileset.loadData()

        taskSubscription = Subscription(fileset = taskFileset, workflow = workflow)
        taskSubscription.loadData()

        if taskConf.get("InputTask") is None and taskConf.get("InputDataset") is None:
            # Production type
            self.assertEqual(taskSubscription["type"], "Production",
                             "Error: Wrong subscription type for processing task")
            self.assertEqual(taskSubscription["split_algo"], taskConf["SplittingAlgo"],
                             "Error: Wrong split algo for generation task")
        else:
            # Processing type
            self.assertEqual(taskSubscription["type"], "Processing", "Wrong subscription type for task")
            if taskSubscription["split_algo"] != "WMBSMergeBySize":
                self.assertEqual(taskSubscription["split_algo"], taskConf['SplittingAlgo'], "Splitting algo mismatch")
            else:
                self.assertEqual(taskFileset.name, inpTaskPath + "unmerged-%s" % taskConf["InputFromOutputModule"],
                                 "Subscription uses WMBSMergeBySize on a merge fileset")

        return

    def testMultipleGlobalTags(self):
        """
        _testMultipleGlobalTags_

        Test creating a workload that starts in a processing task
        with an input dataset, and has different globalTags
        and CMSSW versions (with corresponding scramArch) in
        each task
        """
        processorDocs = makeProcessingConfigs(self.configDatabase)
        testArguments = TaskChainWorkloadFactory.getTestArguments()
        lumiDict = {"1":[[2,4], [8,50]], "2":[[100,200], [210,210]]}
        lumiDict2 = {"1":[[2,4], [8,40]], "2":[[100,150], [210,210]]}
        arguments = {
            "AcquisitionEra": "ReleaseValidation",
            "Requestor": "sfoulkes@fnal.gov",
            "CMSSWVersion": "CMSSW_3_5_8",
            "ScramArch": "slc5_ia32_gcc434",
            "ProcessingVersion": 1,
            "GlobalTag": "DefaultGlobalTag",
            "LumiList": lumiDict,
            "CouchURL": self.testInit.couchUrl,
            "CouchDBName": self.testInit.couchDbName,
            "SiteWhitelist" : ["T1_CH_CERN", "T1_US_FNAL"],
            "DashboardHost": "127.0.0.1",
            "DashboardPort": 8884,
            "TaskChain" : 4,
            "Task1" :{
                "TaskName" : "DigiHLT",
                "ConfigCacheID" : processorDocs['DigiHLT'],
                "InputDataset" : "/MinimumBias/Commissioning10-v4/GEN-SIM",
                "SplittingAlgo"  : "FileBased",
            },
            "Task2" : {
                "TaskName" : "Reco",
                "InputTask" : "DigiHLT",
                "InputFromOutputModule" : "writeRAWDIGI",
                "ConfigCacheID" : processorDocs['Reco'],
                "SplittingAlgo" : "FileBased",
                "GlobalTag" : "GlobalTagForReco",
                "CMSSWVersion" : "CMSSW_3_1_2",
                "ScramArch" : "CompatibleRECOArch",
                "PrimaryDataset" : "ZeroBias",
                "LumiList": lumiDict2,
            },
            "Task3" : {
                "TaskName" : "ALCAReco",
                "InputTask" : "Reco",
                "InputFromOutputModule" : "writeALCA",
                "ConfigCacheID" : processorDocs['ALCAReco'],
                "SplittingAlgo" : "FileBased",
                "GlobalTag" : "GlobalTagForALCAReco",
                "CMSSWVersion" : "CMSSW_3_1_3",
                "ScramArch" : "CompatibleALCAArch",

            },
            "Task4" : {
                "TaskName" : "Skims",
                "InputTask" : "Reco",
                "InputFromOutputModule" : "writeRECO",
                "ConfigCacheID" : processorDocs['Skims'],
                "SplittingAlgo" : "FileBased",
            }
        }
        testArguments.update(arguments)
        arguments = testArguments

        factory = TaskChainWorkloadFactory()
        try:
            self.workload = factory.factoryWorkloadConstruction("YankingTheChain", arguments)
        except Exception as ex:
            msg = "Error invoking TaskChainWorkloadFactory:\n%s" % str(ex)
            self.fail(msg)

        testWMBSHelper = WMBSHelper(self.workload, "DigiHLT", "SomeBlock", cachepath = self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)


        self._checkTask(self.workload.getTaskByPath("/YankingTheChain/DigiHLT"), arguments['Task1'], arguments)
        self._checkTask(self.workload.getTaskByPath("/YankingTheChain/DigiHLT/DigiHLTMergewriteRAWDIGI/Reco"), arguments['Task2'],
                        arguments)
        self._checkTask(self.workload.getTaskByPath("/YankingTheChain/DigiHLT/DigiHLTMergewriteRAWDIGI/Reco/RecoMergewriteALCA/ALCAReco"),
                        arguments['Task3'], arguments)
        self._checkTask(self.workload.getTaskByPath("/YankingTheChain/DigiHLT/DigiHLTMergewriteRAWDIGI/Reco/RecoMergewriteRECO/Skims"),
                        arguments['Task4'], arguments)

        digi = self.workload.getTaskByPath("/YankingTheChain/DigiHLT")
        self.assertEqual(lumiDict, digi.getLumiMask())
        digiStep = digi.getStepHelper("cmsRun1")
        self.assertEqual(digiStep.getGlobalTag(), arguments['GlobalTag'])
        self.assertEqual(digiStep.getCMSSWVersion(), arguments['CMSSWVersion'])
        self.assertEqual(digiStep.getScramArch(), arguments['ScramArch'])

        # Make sure this task has a different lumilist than the global one
        reco = self.workload.getTaskByPath("/YankingTheChain/DigiHLT/DigiHLTMergewriteRAWDIGI/Reco")
        self.assertEqual(lumiDict2, reco.getLumiMask())
        recoStep = reco.getStepHelper("cmsRun1")
        self.assertEqual(recoStep.getGlobalTag(), arguments['Task2']['GlobalTag'])
        self.assertEqual(recoStep.getCMSSWVersion(), arguments['Task2']['CMSSWVersion'])
        self.assertEqual(recoStep.getScramArch(), arguments['Task2']['ScramArch'])

        alca = self.workload.getTaskByPath("/YankingTheChain/DigiHLT/DigiHLTMergewriteRAWDIGI/Reco/RecoMergewriteALCA/ALCAReco")
        self.assertEqual(lumiDict, alca.getLumiMask())
        alcaStep = alca.getStepHelper("cmsRun1")
        self.assertEqual(alcaStep.getGlobalTag(), arguments['Task3']['GlobalTag'])
        self.assertEqual(alcaStep.getCMSSWVersion(), arguments['Task3']['CMSSWVersion'])
        self.assertEqual(alcaStep.getScramArch(), arguments['Task3']['ScramArch'])

        skim = self.workload.getTaskByPath("/YankingTheChain/DigiHLT/DigiHLTMergewriteRAWDIGI/Reco/RecoMergewriteRECO/Skims")
        skimStep = skim.getStepHelper("cmsRun1")
        self.assertEqual(skimStep.getGlobalTag(), arguments['GlobalTag'])
        self.assertEqual(skimStep.getCMSSWVersion(), arguments['CMSSWVersion'])
        self.assertEqual(skimStep.getScramArch(), arguments['ScramArch'])

        # Verify the output datasets
        outputDatasets = self.workload.listOutputDatasets()
        self.assertEqual(len(outputDatasets), 14, "Number of output datasets doesn't match")
        self.assertTrue("/MinimumBias/ReleaseValidation-RawDigiFilter-v1/RAW-DIGI" in outputDatasets,
                        "/MinimumBias/ReleaseValidation-RawDigiFilter-v1/RAW-DIGI not in output datasets")
        self.assertTrue("/MinimumBias/ReleaseValidation-RawDebugDigiFilter-v1/RAW-DEBUG-DIGI" in outputDatasets,
                        "/MinimumBias/ReleaseValidation-RawDebugDigiFilter-v1/RAW-DEBUG-DIGI not in output datasets")
        self.assertTrue("/ZeroBias/ReleaseValidation-reco-v1/RECO" in outputDatasets,
                        "/ZeroBias/ReleaseValidation-reco-v1/RECO not in output datasets")
        self.assertTrue("/ZeroBias/ReleaseValidation-AOD-v1/AOD" in outputDatasets,
                        "/ZeroBias/ReleaseValidation-AOD-v1/AOD not in output datasets")
        self.assertTrue("/ZeroBias/ReleaseValidation-alca-v1/ALCARECO" in outputDatasets,
                        "/ZeroBias/ReleaseValidation-alca-v1/ALCARECO not in output datasets")
        for i in range(1, 5):
            self.assertTrue("/MinimumBias/ReleaseValidation-alca%d-v1/ALCARECO" % i in outputDatasets,
                            "/MinimumBias/ReleaseValidation-alca%d-v1/ALCARECO not in output datasets" % i)
        for i in range(1, 6):
            self.assertTrue("/MinimumBias/ReleaseValidation-skim%d-v1/RECO-AOD" % i in outputDatasets,
                            "/MinimumBias/ReleaseValidation-skim%d-v1/RECO-AOD not in output datasets" % i)

        return

    def testMultithreadedTaskChain(self):
        """
        Test for multithreaded task chains where all steps run with the same
        number of cores
        """

        arguments = self.buildMultithreadedTaskChain(self.differentNCores)
        arguments['Task1']['Multicore'] = 4
        arguments['Task2']['Multicore'] = 4
        arguments['Task3']['Multicore'] = 4
        factory = TaskChainWorkloadFactory()
        try:
            self.workload = factory.factoryWorkloadConstruction("MultiChain", arguments)
        except Exception as ex:
            msg = "Error invoking TaskChainWorkloadFactory:\n%s" % str(ex)
            self.fail(msg)

        hlt = self.workload.getTaskByPath('/MultiChain/HLTD')
        reco = self.workload.getTaskByPath('/MultiChain/HLTD/HLTDMergewriteRAWDIGI/RECODreHLT')
        miniAOD = self.workload.getTaskByPath('/MultiChain/HLTD/HLTDMergewriteRAWDIGI/RECODreHLT/RECODreHLTMergewriteALCA/MINIAODDreHLT')

        hltStep = hlt.getStepHelper("cmsRun1")
        recoStep = reco.getStepHelper("cmsRun1")
        miniAODStep = miniAOD.getStepHelper("cmsRun1")

        self.assertEqual(hltStep.getNumberOfCores(), 4)
        self.assertEqual(recoStep.getNumberOfCores(), 4)
        self.assertEqual(miniAODStep.getNumberOfCores(), 4)

        return

    def testMultithreadedTasksTaskChain(self):
        """
        Test for multithreaded task chains where each step
        may run with a different number of cores
        """

        arguments = self.buildMultithreadedTaskChain(self.differentNCores)

        factory = TaskChainWorkloadFactory()
        try:
            self.workload = factory.factoryWorkloadConstruction("MultiChain2", arguments)
        except Exception as ex:
            msg = "Error invoking TaskChainWorkloadFactory:\n%s" % str(ex)
            self.fail(msg)

        hlt = self.workload.getTaskByPath('/MultiChain2/HLTD')
        reco = self.workload.getTaskByPath('/MultiChain2/HLTD/HLTDMergewriteRAWDIGI/RECODreHLT')
        miniAOD = self.workload.getTaskByPath('/MultiChain2/HLTD/HLTDMergewriteRAWDIGI/RECODreHLT/RECODreHLTMergewriteALCA/MINIAODDreHLT')

        hltMemory = hlt.jobSplittingParameters()['performance']['memoryRequirement']
        recoMemory = reco.jobSplittingParameters()['performance']['memoryRequirement']
        aodMemory = miniAOD.jobSplittingParameters()['performance']['memoryRequirement']

        hltStep = hlt.getStepHelper("cmsRun1")
        recoStep = reco.getStepHelper("cmsRun1")
        miniAODStep = miniAOD.getStepHelper("cmsRun1")

        self.assertEqual(hltStep.getNumberOfCores(), 4)
        self.assertEqual(recoStep.getNumberOfCores(), 8)
        self.assertEqual(miniAODStep.getNumberOfCores(), 1)

        self.assertEqual(recoMemory, 8000.0)
        self.assertEqual(aodMemory, 2000.0)
        self.assertEqual(hltMemory, 4000.0)

        return

    def testPileupTaskChain(self):
        """
        Test for multithreaded task chains where each step
        may run with a different number of cores
        """
        processorDocs = makeProcessingConfigs(self.configDatabase)

        testArguments = TaskChainWorkloadFactory.getTestArguments()
        arguments = {
            "AcquisitionEra": "ReleaseValidation",
            "Requestor": "alan.malta@cern.ch",
            "CMSSWVersion": "CMSSW_3_5_8",
            "ScramArch": "slc5_ia32_gcc434",
            "ProcessingVersion": 1,
            "GlobalTag": "GR10_P_v4::All",
            "CouchURL": self.testInit.couchUrl,
            "CouchDBName": self.testInit.couchDbName,
            "SiteWhitelist" : ["T1_CH_CERN", "T1_US_FNAL"],
            "DashboardHost": "127.0.0.1",
            "DashboardPort": 8884,
            "TaskChain" : 2,
            "Task1" :{
                "InputDataset" : "/cosmics/whatever-input-v1/GEN-SIM",
                "TaskName" : "DIGI",
                "ConfigCacheID" : processorDocs['DigiHLT'],
                "SplittingAlgo" : "LumiBased",
                "LumisPerJob": 4,
                "MCPileup": "/some/cosmics-mc-v1/GEN-SIM",
                "DeterministicPileup": True,
                "CMSSWVersion" : "CMSSW_5_2_6",
                "GlobalTag" : "GR_39_P_V5:All",
                "PrimaryDataset" : "PURelValTTBar",
                "AcquisitionEra": "CMSSW_5_2_6",
                "ProcessingString": "ProcStr_Task1"
            },
            "Task2" : {
                "TaskName" : "RECO",
                "InputTask" : "DIGI",
                "InputFromOutputModule" : "writeRAWDIGI",
                "ConfigCacheID" : processorDocs['Reco'],
                "DataPileup": "/some/minbias-data-v1/GEN-SIM",
                "SplittingAlgo" : "LumiBased",
                "LumisPerJob": 2,
                "GlobalTag": "GR_R_62_V3::All",
                "AcquisitionEra": "CMSSW_5_2_7",
                "ProcessingString": "ProcStr_Task2"
            },
        }
        
        testArguments.update(arguments)
        arguments = testArguments

        factory = TaskChainWorkloadFactory()
        self.workload = factory.factoryWorkloadConstruction("PullingTheChain", arguments)

        firstTask = self.workload.getTaskByPath("/PullingTheChain/DIGI")
        cmsRunStep = firstTask.getStep("cmsRun1").getTypeHelper()
        pileupData = cmsRunStep.getPileup()
        self.assertFalse(hasattr(pileupData, "data"))
        self.assertEqual(pileupData.mc.dataset, ["/some/cosmics-mc-v1/GEN-SIM"])
        splitting = firstTask.jobSplittingParameters()
        self.assertTrue(splitting["deterministicPileup"])

        secondTask = self.workload.getTaskByPath("/PullingTheChain/DIGI/DIGIMergewriteRAWDIGI/RECO")
        cmsRunStep = secondTask.getStep("cmsRun1").getTypeHelper()
        pileupData = cmsRunStep.getPileup()
        self.assertFalse(hasattr(pileupData, "mc"))
        self.assertEqual(pileupData.data.dataset, ["/some/minbias-data-v1/GEN-SIM"])
        splitting = secondTask.jobSplittingParameters()
        self.assertFalse(splitting["deterministicPileup"])


    def buildMultithreadedTaskChain(self, filename):
        """    d
        Build a TaskChain from several sources and customization
        """

        processorDocs = makeProcessingConfigs(self.configDatabase)
        testArguments = TaskChainWorkloadFactory.getTestArguments()

        # Read in the request
        request = json.load(open(filename))

        # Construct args from the pieces starting with test args ...
        arguments = testArguments

        # ... continuing with the request
        for key in ['CMSSWVersion', 'ScramArch', 'GlobalTag', 'ProcessingVersion',
                    'Multicore', 'Memory',
                    'TaskChain', 'Task1', 'Task2', 'Task3']:
            arguments.update({key : request['createRequest'][key]})

        for key in ['SiteBlacklist']:
            arguments.update({key : request['assignRequest'][key]})

        # ... then some local overrides

        del arguments['ConfigCacheID']
        del arguments['ConfigCacheUrl']
        arguments.update({
            "CouchURL": self.testInit.couchUrl,
            "CouchDBName": self.testInit.couchDbName,
        })

        # ... now fill in the ConfigCache documents created and override the inputs to link them up

        arguments['Task1']['ConfigCacheID'] = processorDocs['DigiHLT']

        arguments['Task2']['ConfigCacheID'] = processorDocs['Reco']
        arguments['Task2']['InputFromOutputModule'] = 'writeRAWDIGI'

        arguments['Task3']['ConfigCacheID'] = processorDocs['ALCAReco']
        arguments['Task3']['InputFromOutputModule'] = 'writeALCA'

        return arguments




if __name__ == '__main__':
    unittest.main()
