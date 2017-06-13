#!/usr/bin/env python

"""
_TaskChain_t_

Created by Dave Evans on 2011-06-21.
Copyright (c) 2011 Fermilab. All rights reserved.
"""

import json
import os
import unittest
from copy import deepcopy

from WMCore.Database.CMSCouch import CouchServer, Document
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMSpec.StdSpecs.TaskChain import TaskChainWorkloadFactory
from WMCore.WMSpec.WMSpecErrors import WMSpecFactoryException
from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMQuality.TestInitCouchApp import TestInitCouchApp

REQUEST = {
    "AcquisitionEra": "AcqEra_TopLevel",
    "CMSSWVersion": "CMSSW_8_0_20",
    "Campaign": "UnitTestTaskForce",
    "ConfigCacheUrl": os.environ["COUCHURL"],
    "CouchDBName": "taskchain_t",
    "DQMConfigCacheID": "Harvest",
    "DQMUploadUrl": "https://cmsweb-testbed.cern.ch/dqm/dev;https://cmsweb.cern.ch/dqm/relval-test",
    "EnableHarvesting": True,
    "GlobalTag": "GlobalTag-TopLevel",
    "Memory": 5000,
    "PrepID": "PREPID-TopLevel",
    "ProcessingString": "ProcStr_TopLevel",
    "ProcessingVersion": 20,
    "RequestPriority": 180000,
    "RequestType": "TaskChain",
    "Requestor": "amaltaro",
    "ScramArch": "slc6_amd64_gcc491",
    "SizePerEvent": 60,
    "SubRequestType": "ReDigi",
    "Task1": {
        "AcquisitionEra": "AcqEra_Task1",
        "CMSSWVersion": "CMSSW_8_0_21",
        "ConfigCacheID": "Scratch",
        "EventsPerLumi": 100,
        "GlobalTag": "GlobalTag-Task1",
        "KeepOutput": True,
        "Memory": 5001,
        "Multicore": 1,
        "EventStreams": 1,
        "PrepID": "PREPID-Task1",
        "PrimaryDataset": "MonoHtautau_Scalar_MZp-500_MChi-1_13TeV-madgraph",
        "ProcessingString": "ProcStr_Task1",
        "ProcessingVersion": 21,
        "RequestNumEvents": 50000,
        "ScramArch": "slc6_amd64_gcc493",
        "Seeding": "AutomaticSeeding",
        "SizePerEvent": 100,
        "SplittingAlgo": "EventBased",
        "TaskName": "myTask1",
        "TimePerEvent": 100.0
    },
    "Task2": {
        "AcquisitionEra": "AcqEra_Task2",
        "CMSSWVersion": "CMSSW_8_0_22",
        "ConfigCacheID": "Digi",
        "GlobalTag": "GlobalTag-Task2",
        "InputFromOutputModule": "RAWSIMoutput",
        "InputTask": "myTask1",
        "MCPileup": "/HighPileUp/Run2011A-v1/RAW",  # mocked data
        "Memory": 5002,
        "Multicore": 2,
        "EventStreams": 2,
        "PrepID": "PREPID-Task2",
        "PrimaryDataset": "MonoHtautau_Scalar_MZp-500_MChi-1_13TeV-madgraph",
        "ProcessingString": "ProcStr_Task2",
        "ProcessingVersion": 22,
        "ScramArch": ["slc6_amd64_gcc530", "slc6_amd64_gcc493"],
        "SizePerEvent": 90,
        "SplittingAlgo": "EventAwareLumiBased",
        "TaskName": "myTask2",
        "TimePerEvent": 90.0
    },
    "Task3": {
        "AcquisitionEra": "AcqEra_Task3",
        "CMSSWVersion": "CMSSW_8_0_23",
        "ConfigCacheID": "Aod",
        "GlobalTag": "GlobalTag-Task3",
        "InputFromOutputModule": "PREMIXRAWoutput",
        "InputTask": "myTask2",
        "KeepOutput": True,
        "MCPileup": "/HighPileUp/Run2011A-v1/RAW",  # mocked data
        "Memory": 5003,
        "Multicore": 3,
        "EventStreams": 3,
        "PrepID": "PREPID-Task3",
        "PrimaryDataset": "MonoHtautau_Scalar_MZp-500_MChi-1_13TeV-madgraph",
        "ProcessingString": "ProcStr_Task3",
        "ProcessingVersion": 23,
        "ScramArch": "slc6_amd64_gcc600",
        "SizePerEvent": 80,
        "SplittingAlgo": "EventAwareLumiBased",
        "TaskName": "myTask3",
        "TimePerEvent": 80.
    },
    "Task4": {
        "AcquisitionEra": "AcqEra_Task4",
        "CMSSWVersion": "CMSSW_8_0_24",
        "ConfigCacheID": "MiniAod",
        "GlobalTag": "GlobalTag-Task4",
        "InputFromOutputModule": "AODSIMoutput",
        "InputTask": "myTask3",
        "KeepOutput": True,
        "Memory": 5004,
        "Multicore": 4,
        "EventStreams": 4,
        "PrepID": "PREPID-Task4",
        "PrimaryDataset": "MonoHtautau_Scalar_MZp-500_MChi-1_13TeV-madgraph",
        "ProcessingString": "ProcStr_Task4",
        "ProcessingVersion": 24,
        "ScramArch": "slc6_amd64_gcc630",
        "SizePerEvent": 70,
        "SplittingAlgo": "EventAwareLumiBased",
        "TaskName": "myTask4",
        "TimePerEvent": 70.0
    },
    "TaskChain": 4,
    "TimePerEvent": 0.5
}


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


def makeRealConfigs(couchDatabase):
    """
    _makeRealConfigs_
    Create configs to be used by the joker 4 tasks request, just as tjey
    are used in testbed.
    Scratch - GEN-SIM - DIGI - RECO

    returns a map of config names to IDs
    """
    scratchConfig = Document()
    scratchConfig["owner"] = {"user": "amaltaro", "group": "DATAOPS"}
    scratchConfig["pset_tweak_details"] = {"process": {"outputModules_": ["RAWSIMoutput", "LHEoutput"],
                                                       "LHEoutput": {"dataset": {"filterName": "",
                                                                                 "dataTier": "LHE"}},
                                                       "RAWSIMoutput": {"dataset": {"filterName": "",
                                                                                    "dataTier": "GEN-SIM"}}}}
    digiConfig = Document()
    digiConfig["owner"] = {"user": "amaltaro", "group": "DATAOPS"}
    digiConfig["pset_tweak_details"] = {
        "process": {"outputModules_": ["PREMIXRAWoutput"],
                    "PREMIXRAWoutput": {"dataset": {"filterName": "", "dataTier": "GEN-SIM-RAW"}},
                    }
    }
    aodConfig = Document()
    aodConfig["owner"] = {"user": "amaltaro", "group": "DATAOPS"}
    aodConfig["pset_tweak_details"] = {
        "process": {"outputModules_": ["AODSIMoutput"],
                    "AODSIMoutput": {"dataset": {"filterName": "", "dataTier": "AODSIM"}},
                    }
    }

    miniaodConfig = Document()
    miniaodConfig["owner"] = {"user": "amaltaro", "group": "DATAOPS"}
    miniaodConfig["pset_tweak_details"] = {
        "process": {"outputModules_": ["MINIAODSIMoutput"],
                    "MINIAODSIMoutput": {"dataset": {"filterName": "", "dataTier": "MINIAODSIM"}},
                    }
    }

    harvestConfig = Document()
    harvestConfig["owner"] = {"user": "amaltaro", "group": "DATAOPS"}
    harvestConfig["pset_tweak_details"] = {
        "process": {"outputModules_": []}
    }

    couchDatabase.queue(scratchConfig)
    couchDatabase.queue(digiConfig)
    couchDatabase.queue(aodConfig)
    couchDatabase.queue(miniaodConfig)
    couchDatabase.queue(harvestConfig)
    result = couchDatabase.commit()

    docMap = {
        "Scratch": result[0][u'id'],
        "Digi": result[1][u'id'],
        "Aod": result[2][u'id'],
        "MiniAod": result[3][u'id'],
        "Harvest": result[4][u'id'],
    }
    return docMap


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
                                                   "writeRAWDEBUGDIGI": {"dataset": {"filterName": "RawDebugDigiFilter",
                                                                                     "dataTier": "RAW-DEBUG-DIGI"}}}}
    recoConfig = Document()
    recoConfig["info"] = None
    recoConfig["config"] = None
    recoConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e736f"
    recoConfig["pset_hash"] = "7c856ad35f9f544839d8525ca11765a7"
    recoConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
    recoConfig["pset_tweak_details"] = {
        "process": {"outputModules_": ["writeRECO", "writeAOD", "writeALCA"],
                    "writeRECO": {"dataset": {"dataTier": "RECO", "filterName": "reco"}},
                    "writeAOD": {"dataset": {"dataTier": "AOD", "filterName": "AOD"}},
                    "writeALCA": {"dataset": {"dataTier": "ALCARECO", "filterName": "alca"}},
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
                    "writeALCA2": {"dataset": {"dataTier": "ALCARECO", "filterName": "alca2"}},
                    "writeALCA3": {"dataset": {"dataTier": "ALCARECO", "filterName": "alca3"}},
                    "writeALCA4": {"dataset": {"dataTier": "ALCARECO", "filterName": "alca4"}},
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
                    "writeSkim1": {"dataset": {"dataTier": "RECO-AOD", "filterName": "skim1"}},
                    "writeSkim2": {"dataset": {"dataTier": "RECO-AOD", "filterName": "skim2"}},
                    "writeSkim3": {"dataset": {"dataTier": "RECO-AOD", "filterName": "skim3"}},
                    "writeSkim4": {"dataset": {"dataTier": "RECO-AOD", "filterName": "skim4"}},
                    "writeSkim5": {"dataset": {"dataTier": "RECO-AOD", "filterName": "skim5"}},
                    }
    }
    couchDatabase.queue(rawConfig)
    couchDatabase.queue(recoConfig)
    couchDatabase.queue(alcaConfig)
    couchDatabase.queue(skimsConfig)
    result = couchDatabase.commit()

    docMap = {
        "DigiHLT": result[0][u'id'],
        "Reco": result[1][u'id'],
        "ALCAReco": result[2][u'id'],
        "Skims": result[3][u'id'],
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


def createMultiGTArgs():
    """
    Return a dict of 4-tasks with multiple GTs
    """
    arguments = {
        "AcquisitionEra": "ReleaseValidation",
        "Requestor": "sfoulkes@fnal.gov",
        "CMSSWVersion": "CMSSW_8_0_17",
        "ConfigCacheUrl": os.environ["COUCHURL"],
        "CouchDBName": "taskchain_t",
        "ScramArch": "slc6_amd64_gcc530",
        "ProcessingVersion": 1,
        "GlobalTag": "DefaultGlobalTag",
        "DashboardHost": "127.0.0.1",
        "DashboardPort": 8884,
        "PrepID": "PREPID-TopLevel",
        "TaskChain": 4,
        "Task1": {
            "TaskName": "DigiHLT",
            "InputDataset": "/BprimeJetToBZ_M800GeV_Tune4C_13TeV-madgraph-tauola/Fall13-POSTLS162_V1-v1/GEN-SIM",
            "SplittingAlgo": "EventAwareLumiBased",
        },
        "Task2": {
            "TaskName": "Reco",
            "InputTask": "DigiHLT",
            "InputFromOutputModule": "writeRAWDIGI",
            "GlobalTag": "GlobalTagForReco",
            "CMSSWVersion": "CMSSW_8_0_18",
            "ScramArch": "slc6_amd64_gcc530",
            "PrimaryDataset": "ZeroBias",
            "PrepID": "PREPID-Task2",
            "SplittingAlgo": "EventAwareLumiBased",
        },
        "Task3": {
            "TaskName": "ALCAReco",
            "InputTask": "Reco",
            "InputFromOutputModule": "writeALCA",
            "GlobalTag": "GlobalTagForALCAReco",
            "CMSSWVersion": "CMSSW_8_0_19",
            "ScramArch": "slc6_amd64_gcc530",
            "SplittingAlgo": "EventAwareLumiBased",
        },
        "Task4": {
            "TaskName": "Skims",
            "InputTask": "Reco",
            "InputFromOutputModule": "writeRECO",
            "PrepID": "PREPID-Task4",
            "SplittingAlgo": "EventAwareLumiBased",
        }
    }
    return arguments


def buildComplexTaskChain(couchdb):
    """
    Build a TaskChain workflow with different settings (AcqEra/ProcStr/ProcVer/
    CMSSWVersion/ScramArch/GlobalTag/PrepId/TpE/SpE for each task.
    
    Return a TaskChain workload object.
    """
    testArguments = TaskChainWorkloadFactory.getTestArguments()
    testArguments.update(deepcopy(REQUEST))
    complexDocs = makeRealConfigs(couchdb)

    # additional request override
    del testArguments['ConfigCacheID']
    testArguments['DQMConfigCacheID'] = complexDocs['Harvest']
    testArguments['Task1']['ConfigCacheID'] = complexDocs['Scratch']
    testArguments['Task2']['ConfigCacheID'] = complexDocs['Digi']
    testArguments['Task3']['ConfigCacheID'] = complexDocs['Aod']
    testArguments['Task4']['ConfigCacheID'] = complexDocs['MiniAod']

    factory = TaskChainWorkloadFactory()
    testWorkload = factory.factoryWorkloadConstruction("ComplexChain", testArguments)

    return testWorkload


class TaskChainTests(EmulatedUnitTestCase):
    def setUp(self):
        """
        _setUp_

        Initialize the database and couch.

        """
        super(TaskChainTests, self).setUp()
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("taskchain_t", "ConfigCache")
        self.testInit.setSchema(customModules=["WMCore.WMBS"],
                                useDefault=False)

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
        super(TaskChainTests, self).tearDown()
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
            "CMSSWVersion": "CMSSW_8_0_17",
            "ScramArch": "slc6_amd64_gcc530",
            "ProcessingVersion": 1,
            "GlobalTag": "GR10_P_v4::All",
            "ConfigCacheUrl": self.testInit.couchUrl,
            "CouchDBName": self.testInit.couchDbName,
            "DashboardHost": "127.0.0.1",
            "DashboardPort": 8884,
            "TaskChain": 6,
            "IgnoredOutputModules": ["writeSkim2", "writeRAWDEBUGDIGI"],
            "Task1": {
                "TaskName": "GenSim",
                "ConfigCacheID": generatorDoc,
                "SplittingAlgo": "EventBased",
                "RequestNumEvents": 10000,
                "Seeding": "AutomaticSeeding",
                "PrimaryDataset": "RelValTTBar",
            },
            "Task2": {
                "TaskName": "DigiHLT_new",
                "InputTask": "GenSim",
                "InputFromOutputModule": "writeGENSIM",
                "ConfigCacheID": processorDocs['DigiHLT'],
                "SplittingAlgo": "LumiBased",
                "CMSSWVersion": "CMSSW_8_0_18",
                "GlobalTag": "GR_39_P_V5:All",
                "PrimaryDataset": "PURelValTTBar",
                "KeepOutput": False
            },
            "Task3": {
                "TaskName": "DigiHLT_ref",
                "InputTask": "GenSim",
                "InputFromOutputModule": "writeGENSIM",
                "ConfigCacheID": processorDocs['DigiHLT'],
                "SplittingAlgo": "EventBased",
                "CMSSWVersion": "CMSSW_8_0_18",
                "GlobalTag": "GR_40_P_V5:All",
                "AcquisitionEra": "ReleaseValidationNewConditions",
                "ProcessingVersion": 3,
                "ProcessingString": "Test",
                "KeepOutput": False
            },
            "Task4": {
                "TaskName": "Reco",
                "InputTask": "DigiHLT_new",
                "InputFromOutputModule": "writeRAWDIGI",
                "ConfigCacheID": processorDocs['Reco'],
                "SplittingAlgo": "FileBased",
                "TransientOutputModules": ["writeRECO"]
            },
            "Task5": {
                "TaskName": "ALCAReco",
                "InputTask": "DigiHLT_ref",
                "InputFromOutputModule": "writeRAWDIGI",
                "ConfigCacheID": processorDocs['ALCAReco'],
                "SplittingAlgo": "LumiBased",

            },
            "Task6": {
                "TaskName": "Skims",
                "InputTask": "Reco",
                "InputFromOutputModule": "writeRECO",
                "ConfigCacheID": processorDocs['Skims'],
                "SplittingAlgo": "LumiBased",

            }

        }
        testArguments.update(arguments)
        arguments = testArguments

        factory = TaskChainWorkloadFactory()

        # Test a malformed task chain definition
        arguments['Task4']['TransientOutputModules'].append('writeAOD')
        self.assertRaises(WMSpecFactoryException, factory.validateSchema, arguments)

        arguments['Task4']['TransientOutputModules'].remove('writeAOD')
        self.workload = factory.factoryWorkloadConstruction("PullingTheChain", arguments)

        testWMBSHelper = WMBSHelper(self.workload, "GenSim", "SomeBlock", cachepath=self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        firstTask = self.workload.getTaskByPath("/PullingTheChain/GenSim")

        self._checkTask(firstTask, arguments['Task1'], arguments)
        self._checkTask(self.workload.getTaskByPath("/PullingTheChain/GenSim/GenSimMergewriteGENSIM/DigiHLT_new"),
                        arguments['Task2'], arguments)
        self._checkTask(self.workload.getTaskByPath("/PullingTheChain/GenSim/GenSimMergewriteGENSIM/DigiHLT_ref"),
                        arguments['Task3'], arguments)
        self._checkTask(self.workload.getTaskByPath("/PullingTheChain/GenSim/GenSimMergewriteGENSIM/DigiHLT_new/Reco"),
                        arguments['Task4'], arguments)
        self._checkTask(
            self.workload.getTaskByPath("/PullingTheChain/GenSim/GenSimMergewriteGENSIM/DigiHLT_ref/ALCAReco"),
            arguments['Task5'], arguments)
        self._checkTask(
            self.workload.getTaskByPath("/PullingTheChain/GenSim/GenSimMergewriteGENSIM/DigiHLT_new/Reco/Skims"),
            arguments['Task6'], arguments)

        # Verify the output datasets
        outputDatasets = self.workload.listOutputDatasets()
        self.assertEqual(len(outputDatasets), 11, "Number of output datasets doesn't match")
        self.assertTrue("/RelValTTBar/ReleaseValidation-GenSimFilter-FAKE-v1/GEN-SIM" in outputDatasets)
        self.assertFalse("/RelValTTBar/ReleaseValidation-reco-FAKE-v1/RECO" in outputDatasets)
        self.assertTrue("/RelValTTBar/ReleaseValidation-AOD-FAKE-v1/AOD" in outputDatasets)
        self.assertTrue("/RelValTTBar/ReleaseValidation-alca-FAKE-v1/ALCARECO" in outputDatasets)
        for i in range(1, 5):
            self.assertTrue("/RelValTTBar/ReleaseValidation-alca%d-FAKE-v1/ALCARECO" % i in outputDatasets)
        for i in range(1, 6):
            if i == 2:
                continue
            self.assertTrue("/RelValTTBar/ReleaseValidation-skim%d-FAKE-v1/RECO-AOD" % i in outputDatasets)

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

        workflow = Workflow(name=self.workload.name(),
                            task=task.getPathName())
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
            if outputModule == "logArchive" or not taskConf.get("KeepOutput", True) or outputModule in taskConf.get(
                    "TransientOutputModules", []) or outputModule in centralConf.get("IgnoredOutputModules", []):
                mergedset = task.getPathName() + "/unmerged-" + outputModule
            unmergedset = task.getPathName() + "/unmerged-" + outputModule

            self.assertEqual(mergedset, merged.name, "Merged fileset name is wrong")
            self.assertEqual(unmergedset, unmerged.name, "Unmerged fileset name  is wrong")

            if outputModule != "logArchive" and taskConf.get("KeepOutput", True) \
                    and outputModule not in taskConf.get("TransientOutputModules", []) \
                    and outputModule not in centralConf.get("IgnoredOutputModules", []):
                mergeTask = task.getPathName() + "/" + task.name() + "Merge" + outputModule

                mergeWorkflow = Workflow(name=self.workload.name(),
                                         task=mergeTask)
                mergeWorkflow.load()
                self.assertTrue("Merged" in mergeWorkflow.outputMap,
                                "Merge workflow does not contain a Merged output key")
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
                taskOutputMods = task.getOutputModulesForStep(stepName="cmsRun1")
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
                                                    taskConf["ProcessingVersion"]),
                                     "Wrong processed dataset for module")

        # Test subscriptions
        if taskConf.get("InputTask") is None:
            inputFileset = "%s-%s-SomeBlock" % (self.workload.name(), task.name())
        elif "Merge" in task.getPathName().split("/")[-2]:
            inpTaskPath = task.getPathName().replace(task.name(), "")
            inputFileset = inpTaskPath + "merged-Merged"
        else:
            inpTaskPath = task.getPathName().replace(task.name(), "")
            inputFileset = inpTaskPath + "unmerged-%s" % taskConf["InputFromOutputModule"]
        taskFileset = Fileset(name=inputFileset)
        taskFileset.loadData()

        taskSubscription = Subscription(fileset=taskFileset, workflow=workflow)
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

    def testTrustFlags(self):
        """
        _testTrustFlags_

        Given a taskChain with 4 tasks, test whether TrustSitelists is set for
        the top level tasks and TrustPUSitelists is properly set to all tasks.
        """
        processorDocs = makeProcessingConfigs(self.configDatabase)
        testArguments = TaskChainWorkloadFactory.getTestArguments()
        testArguments.update(createMultiGTArgs())
        testArguments["Task1"]["ConfigCacheID"] = processorDocs['DigiHLT']
        testArguments["Task2"]["ConfigCacheID"] = processorDocs['Reco']
        testArguments["Task3"]["ConfigCacheID"] = processorDocs['ALCAReco']
        testArguments["Task4"]["ConfigCacheID"] = processorDocs['Skims']
        arguments = testArguments

        factory = TaskChainWorkloadFactory()
        workload = factory.factoryWorkloadConstruction("YankingTheChain", arguments)

        for task in workload.getAllTasks():
            flags = task.getTrustSitelists().values()
            self.assertEqual(flags, [False, False])

        # set both flags to true now
        workload.setTrustLocationFlag(True, True)
        for task in workload.getAllTasks():
            flags = task.getTrustSitelists()
            if task.isTopOfTree():
                self.assertEqual(flags.values(), [True, True])
            elif task.taskType() in ["Cleanup", "LogCollect"]:
                self.assertEqual(flags.values(), [False, False])
            else:
                self.assertFalse(flags['trustlists'])
                self.assertTrue(flags['trustPUlists'])

        # set both to false now
        workload.setTrustLocationFlag(False, False)
        for task in workload.getAllTasks(cpuOnly=True):
            flags = task.getTrustSitelists().values()
            self.assertEqual(flags, [False, False])
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
        testArguments.update(createMultiGTArgs())
        lumiDict = {"1": [[2, 4], [8, 50]], "2": [[100, 200], [210, 210]]}
        testArguments["Task1"]["LumiList"] = lumiDict
        testArguments["Task1"]["ConfigCacheID"] = processorDocs['DigiHLT']
        testArguments["Task2"]["ConfigCacheID"] = processorDocs['Reco']
        testArguments["Task3"]["ConfigCacheID"] = processorDocs['ALCAReco']
        testArguments["Task4"]["ConfigCacheID"] = processorDocs['Skims']
        arguments = testArguments

        factory = TaskChainWorkloadFactory()
        self.workload = factory.factoryWorkloadConstruction("YankingTheChain", arguments)

        testWMBSHelper = WMBSHelper(self.workload, "DigiHLT", "SomeBlock", cachepath=self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        self._checkTask(self.workload.getTaskByPath("/YankingTheChain/DigiHLT"), arguments['Task1'], arguments)
        self._checkTask(self.workload.getTaskByPath("/YankingTheChain/DigiHLT/DigiHLTMergewriteRAWDIGI/Reco"),
                        arguments['Task2'],
                        arguments)
        self._checkTask(self.workload.getTaskByPath(
            "/YankingTheChain/DigiHLT/DigiHLTMergewriteRAWDIGI/Reco/RecoMergewriteALCA/ALCAReco"),
            arguments['Task3'], arguments)
        self._checkTask(self.workload.getTaskByPath(
            "/YankingTheChain/DigiHLT/DigiHLTMergewriteRAWDIGI/Reco/RecoMergewriteRECO/Skims"),
            arguments['Task4'], arguments)

        digi = self.workload.getTaskByPath("/YankingTheChain/DigiHLT")
        self.assertEqual(lumiDict, digi.getLumiMask().getCompactList())
        digiStep = digi.getStepHelper("cmsRun1")
        self.assertEqual(digiStep.getGlobalTag(), arguments['GlobalTag'])
        self.assertEqual(digiStep.getCMSSWVersion(), arguments['CMSSWVersion'])
        self.assertEqual(digiStep.getScramArch(), arguments['ScramArch'])

        # Make sure this task has a different lumilist than the global one
        reco = self.workload.getTaskByPath("/YankingTheChain/DigiHLT/DigiHLTMergewriteRAWDIGI/Reco")
        recoStep = reco.getStepHelper("cmsRun1")
        self.assertEqual(recoStep.getGlobalTag(), arguments['Task2']['GlobalTag'])
        self.assertEqual(recoStep.getCMSSWVersion(), arguments['Task2']['CMSSWVersion'])
        self.assertEqual(recoStep.getScramArch(), arguments['Task2']['ScramArch'])

        alca = self.workload.getTaskByPath(
            "/YankingTheChain/DigiHLT/DigiHLTMergewriteRAWDIGI/Reco/RecoMergewriteALCA/ALCAReco")
        alcaStep = alca.getStepHelper("cmsRun1")
        self.assertEqual(alcaStep.getGlobalTag(), arguments['Task3']['GlobalTag'])
        self.assertEqual(alcaStep.getCMSSWVersion(), arguments['Task3']['CMSSWVersion'])
        self.assertEqual(alcaStep.getScramArch(), arguments['Task3']['ScramArch'])

        skim = self.workload.getTaskByPath(
            "/YankingTheChain/DigiHLT/DigiHLTMergewriteRAWDIGI/Reco/RecoMergewriteRECO/Skims")
        skimStep = skim.getStepHelper("cmsRun1")
        self.assertEqual(skimStep.getGlobalTag(), arguments['GlobalTag'])
        self.assertEqual(skimStep.getCMSSWVersion(), arguments['CMSSWVersion'])
        self.assertEqual(skimStep.getScramArch(), arguments['ScramArch'])

        # Verify the output datasets
        outputDatasets = self.workload.listOutputDatasets()
        self.assertEqual(len(outputDatasets), 14, "Number of output datasets doesn't match")
        self.assertTrue(
            "/BprimeJetToBZ_M800GeV_Tune4C_13TeV-madgraph-tauola/ReleaseValidation-RawDigiFilter-FAKE-v1/RAW-DIGI" in outputDatasets)
        self.assertTrue(
            "/BprimeJetToBZ_M800GeV_Tune4C_13TeV-madgraph-tauola/ReleaseValidation-RawDebugDigiFilter-FAKE-v1/RAW-DEBUG-DIGI" in outputDatasets)
        self.assertTrue("/ZeroBias/ReleaseValidation-reco-FAKE-v1/RECO" in outputDatasets)
        self.assertTrue("/ZeroBias/ReleaseValidation-AOD-FAKE-v1/AOD" in outputDatasets)
        self.assertTrue("/ZeroBias/ReleaseValidation-alca-FAKE-v1/ALCARECO" in outputDatasets)
        for i in range(1, 5):
            self.assertTrue(
                "/BprimeJetToBZ_M800GeV_Tune4C_13TeV-madgraph-tauola/ReleaseValidation-alca%d-FAKE-v1/ALCARECO" % i in outputDatasets)
        for i in range(1, 6):
            self.assertTrue(
                "/BprimeJetToBZ_M800GeV_Tune4C_13TeV-madgraph-tauola/ReleaseValidation-skim%d-FAKE-v1/RECO-AOD" % i in outputDatasets)

        return

    def test1TaskMemCoresSettings(self):
        """
        _test1TaskMemCoresSettings_

        Make sure the multicore and memory setings are properly propagated to
        all steps. Single step in a task.
        """
        generatorDoc = makeGeneratorConfig(self.configDatabase)
        testArguments = TaskChainWorkloadFactory.getTestArguments()
        arguments = {
            "ConfigCacheUrl": self.testInit.couchUrl,
            "CouchDBName": self.testInit.couchDbName,
            "TaskChain": 1,
            "Task1": {
                "TaskName": "TaskOne",
                "ConfigCacheID": generatorDoc,
                "RequestNumEvents": 10000,
                "PrimaryDataset": "RelValTTBar",
            },
        }

        testArguments.update(arguments)

        factory = TaskChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestTaskChainWorkload", testArguments)

        taskPaths = ['/TestTaskChainWorkload/TaskOne',
                     '/TestTaskChainWorkload/TaskOne/LogCollectForTaskOne',
                     '/TestTaskChainWorkload/TaskOne/TaskOneMergewriteGENSIM',
                     '/TestTaskChainWorkload/TaskOne/TaskOneMergewriteGENSIM/TaskOnewriteGENSIMMergeLogCollect',
                     '/TestTaskChainWorkload/TaskOne/TaskOneCleanupUnmergedwriteGENSIM']

        for task in taskPaths:
            taskObj = testWorkload.getTaskByPath(task)
            if taskObj.taskType() in ('Production', 'Processing'):
                for step in ('cmsRun1', 'stageOut1', 'logArch1'):
                    stepHelper = taskObj.getStepHelper(step)
                    self.assertEqual(stepHelper.getNumberOfCores(), 1)
                    self.assertEqual(stepHelper.getNumberOfStreams(), 0)
                perfParams = taskObj.jobSplittingParameters()['performance']
                self.assertEqual(perfParams['memoryRequirement'], 2300.0)
            elif taskObj.taskType() == 'LogCollect':
                stepHelper = taskObj.getStepHelper('logCollect1')
                self.assertEqual(stepHelper.getNumberOfCores(), 1)
                self.assertEqual(stepHelper.getNumberOfStreams(), 0)

        # now play with cores at top level
        testArguments['Multicore'] = 2
        testArguments['EventStreams'] = 2
        testWorkload = factory.factoryWorkloadConstruction("TestTaskChainWorkload", testArguments)

        for task in taskPaths:
            taskObj = testWorkload.getTaskByPath(task)
            if taskObj.taskType() in ('Production', 'Processing'):
                for step in ('cmsRun1', 'stageOut1', 'logArch1'):
                    stepHelper = taskObj.getStepHelper(step)
                    if step == 'cmsRun1':
                        self.assertEqual(stepHelper.getNumberOfCores(), testArguments['Multicore'])
                        self.assertEqual(stepHelper.getNumberOfStreams(), testArguments["EventStreams"])
                    else:
                        self.assertEqual(stepHelper.getNumberOfCores(), 1)
                        self.assertEqual(stepHelper.getNumberOfStreams(), 0)
                perfParams = taskObj.jobSplittingParameters()['performance']
                self.assertEqual(perfParams['memoryRequirement'], 2300.0)
            elif taskObj.taskType() == 'LogCollect':
                stepHelper = taskObj.getStepHelper('logCollect1')
                self.assertEqual(stepHelper.getNumberOfCores(), 1)
                self.assertEqual(stepHelper.getNumberOfStreams(), 0)

        # last but not least, play with cores at task level
        testArguments['Task1']['Multicore'] = 2
        testArguments['Task1']['EventStreams'] = 2
        testArguments.pop('Multicore', None)
        testArguments.pop('EventStreams', None)
        testWorkload = factory.factoryWorkloadConstruction("TestTaskChainWorkload", testArguments)

        for task in taskPaths:
            taskObj = testWorkload.getTaskByPath(task)
            if taskObj.taskType() in ('Production', 'Processing'):
                for step in ('cmsRun1', 'stageOut1', 'logArch1'):
                    stepHelper = taskObj.getStepHelper(step)
                    if step == 'cmsRun1':
                        self.assertEqual(stepHelper.getNumberOfCores(), testArguments['Task1']['Multicore'])
                        self.assertEqual(stepHelper.getNumberOfStreams(), testArguments['Task1']['EventStreams'])
                    else:
                        self.assertEqual(stepHelper.getNumberOfCores(), 1)
                        self.assertEqual(stepHelper.getNumberOfStreams(), 0)
                perfParams = taskObj.jobSplittingParameters()['performance']
                self.assertEqual(perfParams['memoryRequirement'], 2300.0)
            elif taskObj.taskType() == 'LogCollect':
                stepHelper = taskObj.getStepHelper('logCollect1')
                self.assertEqual(stepHelper.getNumberOfCores(), 1)
                self.assertEqual(stepHelper.getNumberOfStreams(), 0)

        return

    def testMultithreadedTaskChain(self):
        """
        Test multi-task TaskChain with default and multicore settings
        """
        arguments = self.buildMultithreadedTaskChain(self.differentNCores)
        arguments.pop('Multicore', None)
        arguments.pop('Memory', None)
        arguments.pop('EventStreams', None)
        arguments['Task1'].pop('Multicore', None)
        arguments['Task1'].pop('Memory', None)
        arguments['Task2'].pop('Multicore', None)
        arguments['Task2'].pop('Memory', None)
        arguments['Task2'].pop('EventStreams', None)
        arguments['Task3'].pop('Multicore', None)
        arguments['Task3'].pop('Memory', None)
        arguments['Task3'].pop('EventStreams', None)

        factory = TaskChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("MultiChain", arguments)

        hlt = testWorkload.getTaskByPath('/MultiChain/HLTD')
        reco = testWorkload.getTaskByPath('/MultiChain/HLTD/HLTDMergewriteRAWDIGI/RECODreHLT')
        miniAOD = testWorkload.getTaskByPath(
            '/MultiChain/HLTD/HLTDMergewriteRAWDIGI/RECODreHLT/RECODreHLTMergewriteALCA/MINIAODDreHLT')

        self.assertEqual(hlt.getStepHelper("cmsRun1").getNumberOfCores(), 1)
        self.assertEqual(reco.getStepHelper("cmsRun1").getNumberOfCores(), 1)
        self.assertEqual(miniAOD.getStepHelper("cmsRun1").getNumberOfCores(), 1)
        self.assertEqual(hlt.jobSplittingParameters()['performance']['memoryRequirement'], 2300.0)
        self.assertEqual(reco.jobSplittingParameters()['performance']['memoryRequirement'], 2300.0)
        self.assertEqual(miniAOD.jobSplittingParameters()['performance']['memoryRequirement'], 2300.0)
        self.assertEqual(hlt.getStepHelper("cmsRun1").getNumberOfStreams(), 0)
        self.assertEqual(reco.getStepHelper("cmsRun1").getNumberOfStreams(), 0)
        self.assertEqual(miniAOD.getStepHelper("cmsRun1").getNumberOfStreams(), 0)

        # now all with 16 cores, 16 event streams and 8GB of memory inherited from the top level
        arguments['Multicore'] = 16
        arguments['Memory'] = 8000
        arguments['EventStreams'] = 16
        testWorkload = factory.factoryWorkloadConstruction("MultiChain", arguments)

        hlt = testWorkload.getTaskByPath('/MultiChain/HLTD')
        reco = testWorkload.getTaskByPath('/MultiChain/HLTD/HLTDMergewriteRAWDIGI/RECODreHLT')
        miniAOD = testWorkload.getTaskByPath(
            '/MultiChain/HLTD/HLTDMergewriteRAWDIGI/RECODreHLT/RECODreHLTMergewriteALCA/MINIAODDreHLT')

        self.assertEqual(hlt.getStepHelper("cmsRun1").getNumberOfCores(), arguments['Multicore'])
        self.assertEqual(reco.getStepHelper("cmsRun1").getNumberOfCores(), arguments['Multicore'])
        self.assertEqual(miniAOD.getStepHelper("cmsRun1").getNumberOfCores(), arguments['Multicore'])
        self.assertEqual(hlt.jobSplittingParameters()['performance']['memoryRequirement'], arguments['Memory'])
        self.assertEqual(reco.jobSplittingParameters()['performance']['memoryRequirement'], arguments['Memory'])
        self.assertEqual(miniAOD.jobSplittingParameters()['performance']['memoryRequirement'], arguments['Memory'])
        self.assertEqual(hlt.getStepHelper("cmsRun1").getNumberOfStreams(), arguments['EventStreams'])
        self.assertEqual(reco.getStepHelper("cmsRun1").getNumberOfStreams(), arguments['EventStreams'])
        self.assertEqual(miniAOD.getStepHelper("cmsRun1").getNumberOfStreams(), arguments['EventStreams'])

        return

    def testMultithreadedTasksTaskChain(self):
        """
        Test for multithreaded task chains where each step
        may run with a different number of cores
        """

        arguments = self.buildMultithreadedTaskChain(self.differentNCores)
        factory = TaskChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("MultiChain2", arguments)

        hlt = testWorkload.getTaskByPath('/MultiChain2/HLTD')
        reco = testWorkload.getTaskByPath('/MultiChain2/HLTD/HLTDMergewriteRAWDIGI/RECODreHLT')
        miniAOD = testWorkload.getTaskByPath(
            '/MultiChain2/HLTD/HLTDMergewriteRAWDIGI/RECODreHLT/RECODreHLTMergewriteALCA/MINIAODDreHLT')

        hltMemory = hlt.jobSplittingParameters()['performance']['memoryRequirement']
        recoMemory = reco.jobSplittingParameters()['performance']['memoryRequirement']
        aodMemory = miniAOD.jobSplittingParameters()['performance']['memoryRequirement']

        self.assertEqual(hlt.getStepHelper("cmsRun1").getNumberOfCores(), arguments['Multicore'])
        self.assertEqual(reco.getStepHelper("cmsRun1").getNumberOfCores(), arguments['Task2']['Multicore'])
        self.assertEqual(miniAOD.getStepHelper("cmsRun1").getNumberOfCores(), arguments['Task3']['Multicore'])
        self.assertEqual(hlt.getStepHelper("cmsRun1").getNumberOfStreams(), arguments['EventStreams'])
        self.assertEqual(reco.getStepHelper("cmsRun1").getNumberOfStreams(), arguments['Task2']['EventStreams'])
        self.assertEqual(miniAOD.getStepHelper("cmsRun1").getNumberOfStreams(), arguments['Task3']['EventStreams'])

        self.assertEqual(hltMemory, arguments['Memory'])
        self.assertEqual(recoMemory, arguments['Task2']['Memory'])
        self.assertEqual(aodMemory, arguments['Task3']['Memory'])

        # and test another mix of cores, event streams and memory
        arguments['Task1']['Multicore'] = arguments.pop('Multicore', None)
        arguments['Task1']['Memory'] = arguments.pop('Memory', None)
        arguments['Task2'].pop('Multicore', None)
        arguments['Task2'].pop('Memory', None)
        arguments['Task3']['Multicore'] = 2
        arguments['EventStreams'] = 32
        arguments['Task2']['EventStreams'] = 8
        arguments['Task3']['EventStreams'] = 0
        testWorkload = factory.factoryWorkloadConstruction("MultiChain2", arguments)

        hlt = testWorkload.getTaskByPath('/MultiChain2/HLTD')
        reco = testWorkload.getTaskByPath('/MultiChain2/HLTD/HLTDMergewriteRAWDIGI/RECODreHLT')
        miniAOD = testWorkload.getTaskByPath(
            '/MultiChain2/HLTD/HLTDMergewriteRAWDIGI/RECODreHLT/RECODreHLTMergewriteALCA/MINIAODDreHLT')

        hltMemory = hlt.jobSplittingParameters()['performance']['memoryRequirement']
        recoMemory = reco.jobSplittingParameters()['performance']['memoryRequirement']
        aodMemory = miniAOD.jobSplittingParameters()['performance']['memoryRequirement']

        self.assertEqual(hlt.getStepHelper("cmsRun1").getNumberOfCores(), arguments['Task1']['Multicore'])
        self.assertEqual(reco.getStepHelper("cmsRun1").getNumberOfCores(), 1)
        self.assertEqual(miniAOD.getStepHelper("cmsRun1").getNumberOfCores(), arguments['Task3']['Multicore'])
        self.assertEqual(hlt.getStepHelper("cmsRun1").getNumberOfStreams(), arguments['EventStreams'])
        self.assertEqual(reco.getStepHelper("cmsRun1").getNumberOfStreams(), arguments['Task2']['EventStreams'])
        self.assertEqual(miniAOD.getStepHelper("cmsRun1").getNumberOfStreams(), arguments['Task3']['EventStreams'])

        self.assertEqual(hltMemory, arguments['Task1']['Memory'])
        self.assertEqual(recoMemory, 2300.0)
        self.assertEqual(aodMemory, arguments['Task3']['Memory'])

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
            "CMSSWVersion": "CMSSW_8_0_17",
            "ScramArch": "slc6_amd64_gcc530",
            "GlobalTag": "GR10_P_v4::All",
            "ConfigCacheUrl": self.testInit.couchUrl,
            "CouchDBName": self.testInit.couchDbName,
            "DashboardHost": "127.0.0.1",
            "DashboardPort": 8884,
            "TaskChain": 2,
            "Task1": {
                "InputDataset": "/Cosmics/ComissioningHI-v1/RAW",
                "TaskName": "DIGI",
                "ConfigCacheID": processorDocs['DigiHLT'],
                "MCPileup": "/Cosmics/ComissioningHI-PromptReco-v1/RECO",
                "DeterministicPileup": True
            },
            "Task2": {
                "TaskName": "RECO",
                "InputTask": "DIGI",
                "InputFromOutputModule": "writeRAWDIGI",
                "ConfigCacheID": processorDocs['Reco'],
                "DataPileup": "/some/minbias-data-v1/GEN-SIM"
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
        self.assertEqual(pileupData.mc.dataset, [arguments['Task1']['MCPileup']])
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
        """
        Build a TaskChain from several sources and customization
        """

        processorDocs = makeProcessingConfigs(self.configDatabase)
        testArguments = TaskChainWorkloadFactory.getTestArguments()

        # Read in the request
        request = json.load(open(filename))

        # Construct args from the pieces starting with test args ...
        arguments = testArguments

        # ... continuing with the request
        for key in ['GlobalTag', 'ProcessingVersion', 'Multicore', 'Memory', 'EventStreams',
                    'TaskChain', 'Task1', 'Task2', 'Task3']:
            arguments.update({key: request['createRequest'][key]})

        # ... then some local overrides
        arguments['CMSSWVersion'] = 'CMSSW_8_0_17'
        arguments['ScramArch'] = 'slc6_amd64_gcc530'
        del arguments['ConfigCacheID']
        arguments.update({
            "ConfigCacheUrl": self.testInit.couchUrl,
            "CouchDBName": self.testInit.couchDbName,
        })

        # then mocked data
        arguments['Task1']['InputDataset'] = '/HighPileUp/Run2011A-v1/RAW'

        # ... now fill in the ConfigCache documents created and override the inputs to link them up

        arguments['Task1']['ConfigCacheID'] = processorDocs['DigiHLT']

        arguments['Task2']['ConfigCacheID'] = processorDocs['Reco']
        arguments['Task2']['InputFromOutputModule'] = 'writeRAWDIGI'

        arguments['Task3']['ConfigCacheID'] = processorDocs['ALCAReco']
        arguments['Task3']['InputFromOutputModule'] = 'writeALCA'
        return arguments

    def testWorkloadJobSplitting(self):
        """
        Test a many-tasks TaskChain workload with specific settings for every
        single task. Checks are done mainly at workload level. It validates:
         * input data settings
         * job splitting and performance settings
        """
        # create a taskChain workload
        testWorkload = buildComplexTaskChain(self.configDatabase)

        # test workload properties
        self.assertEqual(testWorkload.getRequestType(), REQUEST['RequestType'])
        self.assertEqual(testWorkload.getDashboardActivity(), "relval")
        self.assertEqual(testWorkload.getCampaign(), REQUEST['Campaign'])
        self.assertEqual(testWorkload.getAcquisitionEra(), REQUEST['AcquisitionEra'])
        self.assertEqual(testWorkload.getProcessingString(), REQUEST['ProcessingString'])
        self.assertEqual(testWorkload.getProcessingVersion(), REQUEST['ProcessingVersion'])
        self.assertEqual(testWorkload.getPrepID(), REQUEST['PrepID'])
        self.assertItemsEqual(testWorkload.getCMSSWVersions(), ['CMSSW_8_0_21', 'CMSSW_8_0_22',
                                                                'CMSSW_8_0_23', 'CMSSW_8_0_24'])
        self.assertEqual(testWorkload.getLumiList(), {})
        self.assertFalse(testWorkload.getAllowOpportunistic())
        self.assertEqual(testWorkload.getUnmergedLFNBase(), '/store/unmerged')
        self.assertEqual(testWorkload.getMergedLFNBase(), '/store/data')
        self.assertEqual(testWorkload.listInputDatasets(), [])

        tasksProducingOutput = [
            '/ComplexChain/myTask1',
            '/ComplexChain/myTask1/myTask1MergeLHEoutput',
            '/ComplexChain/myTask1/myTask1MergeRAWSIMoutput',
            '/ComplexChain/myTask1/myTask1MergeRAWSIMoutput/myTask2',
            '/ComplexChain/myTask1/myTask1MergeRAWSIMoutput/myTask2/myTask2MergePREMIXRAWoutput',
            '/ComplexChain/myTask1/myTask1MergeRAWSIMoutput/myTask2/myTask2MergePREMIXRAWoutput/myTask3',
            '/ComplexChain/myTask1/myTask1MergeRAWSIMoutput/myTask2/myTask2MergePREMIXRAWoutput/myTask3/myTask3MergeAODSIMoutput',
            '/ComplexChain/myTask1/myTask1MergeRAWSIMoutput/myTask2/myTask2MergePREMIXRAWoutput/myTask3/myTask3MergeAODSIMoutput/myTask4',
            '/ComplexChain/myTask1/myTask1MergeRAWSIMoutput/myTask2/myTask2MergePREMIXRAWoutput/myTask3/myTask3MergeAODSIMoutput/myTask4/myTask4MergeMINIAODSIMoutput'
        ]
        self.assertItemsEqual(testWorkload.listOutputProducingTasks(), tasksProducingOutput)

        # workqueue start policy checks
        self.assertEqual(testWorkload.startPolicy(), "MonteCarlo")
        self.assertDictEqual(testWorkload.startPolicyParameters(), {'SliceSize': 288, 'SliceType': 'NumberOfEvents',
                                                                    'SplittingAlgo': 'EventBased', 'SubSliceSize': 100,
                                                                    'SubSliceType': 'NumberOfEventsPerLumi',
                                                                    'blowupFactor': 3.4, 'policyName': 'MonteCarlo'})

        # nasty splitting settings check
        splitArgs = testWorkload.listJobSplittingParametersByTask()
        task1Splitting = splitArgs['/ComplexChain/myTask1']
        self.assertEqual(task1Splitting['type'], 'Production')
        self.assertEqual(task1Splitting['algorithm'], 'EventBased')
        self.assertEqual(task1Splitting['events_per_job'], 288)
        self.assertEqual(task1Splitting['events_per_lumi'], 100)
        self.assertFalse(task1Splitting['deterministicPileup'])
        self.assertFalse(task1Splitting['lheInputFiles'])
        self.assertFalse(task1Splitting['trustSitelists'])
        self.assertFalse(task1Splitting['trustPUSitelists'])
        self.assertDictEqual(task1Splitting['performance'], {'memoryRequirement': 5001.0,
                                                             'sizePerEvent': 100.0,
                                                             'timePerEvent': 100.0})
        task2Splitting = splitArgs['/ComplexChain/myTask1/myTask1MergeRAWSIMoutput/myTask2']
        self.assertEqual(task2Splitting['type'], 'Processing')
        self.assertEqual(task2Splitting['algorithm'], 'EventAwareLumiBased')
        self.assertEqual(task2Splitting['events_per_job'], 320)
        self.assertEqual(task2Splitting['max_events_per_lumi'], 20000)
        self.assertFalse(task2Splitting['deterministicPileup'])
        self.assertFalse(task2Splitting['lheInputFiles'])
        self.assertFalse(task2Splitting['trustSitelists'])
        self.assertFalse(task2Splitting['trustPUSitelists'])
        self.assertDictEqual(task2Splitting['performance'], {'memoryRequirement': 5002.0,
                                                             'sizePerEvent': 90.0,
                                                             'timePerEvent': 90.0})
        task3Splitting = splitArgs[
            '/ComplexChain/myTask1/myTask1MergeRAWSIMoutput/myTask2/myTask2MergePREMIXRAWoutput/myTask3']
        self.assertEqual(task3Splitting['type'], 'Processing')
        self.assertEqual(task3Splitting['algorithm'], 'EventAwareLumiBased')
        self.assertEqual(task3Splitting['events_per_job'], 360)
        self.assertEqual(task3Splitting['max_events_per_lumi'], 20000)
        self.assertFalse(task3Splitting['deterministicPileup'])
        self.assertFalse(task3Splitting['lheInputFiles'])
        self.assertFalse(task3Splitting['trustSitelists'])
        self.assertFalse(task3Splitting['trustPUSitelists'])
        self.assertDictEqual(task3Splitting['performance'], {'memoryRequirement': 5003.0,
                                                             'sizePerEvent': 80.0,
                                                             'timePerEvent': 80.0})
        task4Splitting = splitArgs[
            '/ComplexChain/myTask1/myTask1MergeRAWSIMoutput/myTask2/myTask2MergePREMIXRAWoutput/myTask3/myTask3MergeAODSIMoutput/myTask4']
        self.assertEqual(task4Splitting['type'], 'Processing')
        self.assertEqual(task4Splitting['algorithm'], 'EventAwareLumiBased')
        self.assertEqual(task4Splitting['events_per_job'], 411)
        self.assertEqual(task4Splitting['max_events_per_lumi'], 20000)
        self.assertFalse(task4Splitting['deterministicPileup'])
        self.assertFalse(task4Splitting['lheInputFiles'])
        self.assertFalse(task4Splitting['trustSitelists'])
        self.assertFalse(task4Splitting['trustPUSitelists'])
        self.assertDictEqual(task4Splitting['performance'], {'memoryRequirement': 5004.0,
                                                             'sizePerEvent': 70.0,
                                                             'timePerEvent': 70.0})

        return

    def testCMSSWSettings(self):
        """
        Test CMSSW/ScramArchs settings at workload/task/step level
        """

        def _checkCMSSWScram(workload):
            "Validate CMSSW and ScramArch for the 4-tasks request and their merge tasks"
            for t in ["Task1", "Task2", "Task3", "Task4"]:
                task = workload.getTaskByName(REQUEST[t]['TaskName'])
                self.assertEqual(task.getSwVersion(), REQUEST[t]['CMSSWVersion'])
                if isinstance(REQUEST[t]['ScramArch'], basestring):
                    scramArchs = [REQUEST[t]['ScramArch']]
                else:
                    scramArchs = REQUEST[t]['ScramArch']
                self.assertEqual(task.getScramArch(), scramArchs)

                for childName in task.listChildNames():
                    child = workload.getTaskByName(childName)
                    if child.taskType() in ["Merge", "Production", "Processing"]:
                        self.assertEqual(child.getSwVersion(), REQUEST[t]['CMSSWVersion'])
                        self.assertEqual(child.getScramArch(), scramArchs)

                step = task.getStepHelper(task.getTopStepName())
                self.assertEqual(step.getCMSSWVersion(), REQUEST[t]['CMSSWVersion'])
                self.assertItemsEqual(step.getScramArch(), scramArchs)
            return

        # Case 1: workflow creation only
        testWorkload = buildComplexTaskChain(self.configDatabase)
        self.assertItemsEqual(testWorkload.getCMSSWVersions(), ['CMSSW_8_0_21', 'CMSSW_8_0_22',
                                                                'CMSSW_8_0_23', 'CMSSW_8_0_24'])
        _checkCMSSWScram(testWorkload)

        # Case 2: now we assign it just to make sure no changes will happen to the release values
        assignDict = {"SiteWhitelist": ["T2_US_Nebraska", "T2_IT_Rome"], "Team": "The-A-Team"}
        testWorkload.updateArguments(assignDict)

        self.assertItemsEqual(testWorkload.getCMSSWVersions(), ['CMSSW_8_0_21', 'CMSSW_8_0_22',
                                                                'CMSSW_8_0_23', 'CMSSW_8_0_24'])
        _checkCMSSWScram(testWorkload)

        return

    def testPrepIDSettings(self):
        """
        Test the prepid settings for the workload and tasks
        """
        testWorkload = buildComplexTaskChain(self.configDatabase)

        self.assertEqual(testWorkload.getPrepID(), REQUEST['PrepID'])
        for t in ["myTask1", "myTask1MergeLHEoutput", "myTask1MergeRAWSIMoutput"]:
            self.assertEqual(testWorkload.getTaskByName(t).getPrepID(), REQUEST['Task1']['PrepID'])
        for t in ["myTask2", "myTask2MergePREMIXRAWoutput"]:
            self.assertEqual(testWorkload.getTaskByName(t).getPrepID(), REQUEST['Task2']['PrepID'])
        for t in ["myTask3", "myTask3MergeAODSIMoutput"]:
            self.assertEqual(testWorkload.getTaskByName(t).getPrepID(), REQUEST['Task3']['PrepID'])
        for t in ["myTask4", "myTask4MergeMINIAODSIMoutput"]:
            self.assertEqual(testWorkload.getTaskByName(t).getPrepID(), REQUEST['Task4']['PrepID'])

        # Now we assign it just to make sure no changes will happen to the prepid
        assignDict = {"SiteWhitelist": ["T2_US_Nebraska", "T2_IT_Rome"], "Team": "The-A-Team"}
        testWorkload.updateArguments(assignDict)

        self.assertEqual(testWorkload.getPrepID(), REQUEST['PrepID'])
        for t in ["myTask1", "myTask1MergeLHEoutput", "myTask1MergeRAWSIMoutput"]:
            self.assertEqual(testWorkload.getTaskByName(t).getPrepID(), REQUEST['Task1']['PrepID'])
        for t in ["myTask2", "myTask2MergePREMIXRAWoutput"]:
            self.assertEqual(testWorkload.getTaskByName(t).getPrepID(), REQUEST['Task2']['PrepID'])
        for t in ["myTask3", "myTask3MergeAODSIMoutput"]:
            self.assertEqual(testWorkload.getTaskByName(t).getPrepID(), REQUEST['Task3']['PrepID'])
        for t in ["myTask4", "myTask4MergeMINIAODSIMoutput"]:
            self.assertEqual(testWorkload.getTaskByName(t).getPrepID(), REQUEST['Task4']['PrepID'])


        ### Now test it with top level inheritance, creation only
        processorDocs = makeProcessingConfigs(self.configDatabase)
        testArguments = TaskChainWorkloadFactory.getTestArguments()
        testArguments.update(createMultiGTArgs())
        testArguments["Task1"]["ConfigCacheID"] = processorDocs['DigiHLT']
        testArguments["Task2"]["ConfigCacheID"] = processorDocs['Reco']
        testArguments["Task3"]["ConfigCacheID"] = processorDocs['ALCAReco']
        testArguments["Task4"]["ConfigCacheID"] = processorDocs['Skims']
        factory = TaskChainWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("YankingTheChain", testArguments)

        self.assertEqual(testWorkload.getPrepID(), testArguments['PrepID'])
        for t in ["DigiHLT", "DigiHLTMergewriteRAWDIGI", "DigiHLTMergewriteRAWDEBUGDIGI"]:
            self.assertEqual(testWorkload.getTaskByName(t).getPrepID(),
                             testArguments['Task1'].get('PrepID', testArguments['PrepID']))
        for t in ["Reco", "RecoMergewriteRECO", "RecoMergewriteALCA", "RecoMergewriteAOD"]:
            self.assertEqual(testWorkload.getTaskByName(t).getPrepID(),
                             testArguments['Task2'].get('PrepID', testArguments['PrepID']))
        for t in ["ALCAReco", "ALCARecoMergewriteALCA1", "ALCARecoMergewriteALCA2",
                  "ALCARecoMergewriteALCA3", "ALCARecoMergewriteALCA4"]:
            self.assertEqual(testWorkload.getTaskByName(t).getPrepID(),
                             testArguments['Task3'].get('PrepID', testArguments['PrepID']))
        for t in ["Skims", "SkimsMergewriteSkim1", "SkimsMergewriteSkim2",
                  "SkimsMergewriteSkim3", "SkimsMergewriteSkim4"]:
            self.assertEqual(testWorkload.getTaskByName(t).getPrepID(),
                             testArguments['Task4'].get('PrepID', testArguments['PrepID']))

        # Now we assign it just to make sure no changes will happen to the prepid
        assignDict = {"SiteWhitelist": ["T2_US_Nebraska", "T2_IT_Rome"], "Team": "The-A-Team"}
        testWorkload.updateArguments(assignDict)
        self.assertEqual(testWorkload.getPrepID(), testArguments['PrepID'])
        self.assertEqual(testWorkload.getPrepID(), testArguments['PrepID'])
        for t in ["DigiHLT", "DigiHLTMergewriteRAWDIGI", "DigiHLTMergewriteRAWDEBUGDIGI"]:
            self.assertEqual(testWorkload.getTaskByName(t).getPrepID(),
                             testArguments['Task1'].get('PrepID', testArguments['PrepID']))
        for t in ["Reco", "RecoMergewriteRECO", "RecoMergewriteALCA", "RecoMergewriteAOD"]:
            self.assertEqual(testWorkload.getTaskByName(t).getPrepID(),
                             testArguments['Task2'].get('PrepID', testArguments['PrepID']))
        for t in ["ALCAReco", "ALCARecoMergewriteALCA1", "ALCARecoMergewriteALCA2",
                  "ALCARecoMergewriteALCA3", "ALCARecoMergewriteALCA4"]:
            self.assertEqual(testWorkload.getTaskByName(t).getPrepID(),
                             testArguments['Task3'].get('PrepID', testArguments['PrepID']))
        for t in ["Skims", "SkimsMergewriteSkim1", "SkimsMergewriteSkim2",
                  "SkimsMergewriteSkim3", "SkimsMergewriteSkim4"]:
            self.assertEqual(testWorkload.getTaskByName(t).getPrepID(),
                             testArguments['Task4'].get('PrepID', testArguments['PrepID']))

        return

    def testInputDataSettings(self):
        """
        Test input data settings for a many-tasks TaskChain workload with specific
        settings for every single task.
        """
        inputSteps = {'Task1':None,
                      'Task2': '/ComplexChain/myTask1/myTask1MergeRAWSIMoutput/cmsRun1',
                      'Task3': '/ComplexChain/myTask1/myTask1MergeRAWSIMoutput/myTask2/myTask2MergePREMIXRAWoutput/cmsRun1',
                      'Task4': '/ComplexChain/myTask1/myTask1MergeRAWSIMoutput/myTask2/myTask2MergePREMIXRAWoutput/myTask3/myTask3MergeAODSIMoutput/cmsRun1'}
        childNames = {
            'Task1': ["myTask1MergeRAWSIMoutput", "myTask1MergeLHEoutput", "LogCollectFormyTask1",
                      "myTask1CleanupUnmergedLHEoutput", "myTask1CleanupUnmergedRAWSIMoutput"],
            'Task2': ["myTask2MergePREMIXRAWoutput", "LogCollectFormyTask2", "myTask2CleanupUnmergedPREMIXRAWoutput"],
            'Task3': ["myTask3MergeAODSIMoutput", "LogCollectFormyTask3", "myTask3CleanupUnmergedAODSIMoutput"],
            'Task4': ["myTask4MergeMINIAODSIMoutput", "LogCollectFormyTask4", "myTask4CleanupUnmergedMINIAODSIMoutput"]
        }

        def _checkInputData(workload, sitewhitelist=None):
            "Validate input data/block/run/step/PU for the 4-tasks request"
            sitewhitelist = sitewhitelist or []
            self.assertEqual(workload.listPileupDatasets().values(), [{REQUEST['Task2']['MCPileup']}])

            for t in ["Task1", "Task2", "Task3", "Task4"]:
                task = workload.getTaskByName(REQUEST[t]['TaskName'])
                taskType = 'Production' if t == 'Task1' else 'Processing'
                sitewhitelist = sitewhitelist if t == 'Task1' else []

                self.assertEqual(task.taskType(), taskType)
                if 'RequestNumEvents' in REQUEST[t]:
                    self.assertEqual(task.totalEvents(), REQUEST[t]['RequestNumEvents'])
                self.assertItemsEqual(task.listChildNames(), childNames[t])
                self.assertEqual(task.getInputStep(), inputSteps[t])
                self.assertDictEqual(task.getLumiMask(), {})
                self.assertEqual(task.getFirstEvent(), REQUEST['Task1'].get('FirstEvent', 1))
                self.assertEqual(task.getFirstLumi(), REQUEST['Task1'].get('FirstLumi', 1))
                self.assertEqual(task.parentProcessingFlag(), REQUEST[t].get('IncludeParents', False))
                self.assertEqual(task.inputDataset(), REQUEST['Task1'].get('InputDataset'))
                self.assertEqual(task.dbsUrl(), REQUEST.get('DbsUrl'))
                self.assertEqual(task.inputBlockWhitelist(), REQUEST[t].get('inputBlockWhitelist'))
                self.assertEqual(task.inputBlockBlacklist(), REQUEST[t].get('inputBlockBlacklist'))
                self.assertEqual(task.inputRunWhitelist(), REQUEST[t].get('inputRunWhitelist'))
                self.assertEqual(task.inputRunBlacklist(), REQUEST[t].get('inputRunBlacklist'))
                self.assertItemsEqual(task.siteWhitelist(), sitewhitelist)
                self.assertItemsEqual(task.siteBlacklist(), REQUEST[t].get('siteBlacklist', []))
                self.assertDictEqual(task.getTrustSitelists(), {'trustPUlists': False, 'trustlists': False})
                self.assertItemsEqual(task.getIgnoredOutputModulesForTask(), REQUEST[t].get('IgnoredOutputModules', []))

            return

        # Case 1: only workload creation
        testWorkload = buildComplexTaskChain(self.configDatabase)
        _checkInputData(testWorkload)

        # Case 2: workload assignment. Only the site whitelist is supposed to change
        assignDict = {"SiteWhitelist": ["T2_US_Nebraska", "T2_IT_Rome"], "Team": "The-A-Team"}
        testWorkload.updateArguments(assignDict)
        _checkInputData(testWorkload, sitewhitelist=["T2_US_Nebraska", "T2_IT_Rome"])

        return

    def testOutputDataSettings(self):
        """
        Test output datasets, output modules and subscriptions for the workload
        and each task. Tests are done against basic workload creation and then
        against a workload assigned (mimic'ing Ops assignment)
        """
        outputLFNBases = [
            '/store/unmerged/AcqEra_Task1/MonoHtautau_Scalar_MZp-500_MChi-1_13TeV-madgraph/LHE/ProcStr_Task1-v21',
            '/store/unmerged/AcqEra_Task1/MonoHtautau_Scalar_MZp-500_MChi-1_13TeV-madgraph/GEN-SIM/ProcStr_Task1-v21',
            '/store/unmerged/AcqEra_Task2/MonoHtautau_Scalar_MZp-500_MChi-1_13TeV-madgraph/GEN-SIM-RAW/ProcStr_Task2-v22',
            '/store/unmerged/AcqEra_Task3/MonoHtautau_Scalar_MZp-500_MChi-1_13TeV-madgraph/AODSIM/ProcStr_Task3-v23',
            '/store/unmerged/AcqEra_Task4/MonoHtautau_Scalar_MZp-500_MChi-1_13TeV-madgraph/MINIAODSIM/ProcStr_Task4-v24',
            '/store/data/AcqEra_Task1/MonoHtautau_Scalar_MZp-500_MChi-1_13TeV-madgraph/LHE/ProcStr_Task1-v21',
            '/store/data/AcqEra_Task1/MonoHtautau_Scalar_MZp-500_MChi-1_13TeV-madgraph/GEN-SIM/ProcStr_Task1-v21',
            '/store/data/AcqEra_Task2/MonoHtautau_Scalar_MZp-500_MChi-1_13TeV-madgraph/GEN-SIM-RAW/ProcStr_Task2-v22',
            '/store/data/AcqEra_Task3/MonoHtautau_Scalar_MZp-500_MChi-1_13TeV-madgraph/AODSIM/ProcStr_Task3-v23',
            '/store/data/AcqEra_Task4/MonoHtautau_Scalar_MZp-500_MChi-1_13TeV-madgraph/MINIAODSIM/ProcStr_Task4-v24']

        outDsets = {
            "Task1": ['/MonoHtautau_Scalar_MZp-500_MChi-1_13TeV-madgraph/AcqEra_Task1-ProcStr_Task1-v21/GEN-SIM',
                      '/MonoHtautau_Scalar_MZp-500_MChi-1_13TeV-madgraph/AcqEra_Task1-ProcStr_Task1-v21/LHE'],
            "Task2": ['/MonoHtautau_Scalar_MZp-500_MChi-1_13TeV-madgraph/AcqEra_Task2-ProcStr_Task2-v22/GEN-SIM-RAW'],
            "Task3": ['/MonoHtautau_Scalar_MZp-500_MChi-1_13TeV-madgraph/AcqEra_Task3-ProcStr_Task3-v23/AODSIM'],
            "Task4": ['/MonoHtautau_Scalar_MZp-500_MChi-1_13TeV-madgraph/AcqEra_Task4-ProcStr_Task4-v24/MINIAODSIM']
            }

        outMods = {"Task1": {'LHEoutput': dict(dataTier='LHE', filterName='', transient=True,
                                               primaryDataset=REQUEST['Task1']['PrimaryDataset'],
                                               processedDataset="AcqEra_Task1-ProcStr_Task1-v21",
                                               lfnBase=outputLFNBases[0],
                                               mergedLFNBase=outputLFNBases[0+5]),
                             'RAWSIMoutput': dict(dataTier='GEN-SIM', filterName='', transient=True,
                                                  primaryDataset=REQUEST['Task1']['PrimaryDataset'],
                                                  processedDataset="AcqEra_Task1-ProcStr_Task1-v21",
                                                  lfnBase=outputLFNBases[1],
                                                  mergedLFNBase=outputLFNBases[1+5])},
                   "Task2": {'PREMIXRAWoutput': dict(dataTier='GEN-SIM-RAW', filterName='', transient=True,
                                                     primaryDataset=REQUEST['Task2']['PrimaryDataset'],
                                                     processedDataset="AcqEra_Task2-ProcStr_Task2-v22",
                                                     lfnBase=outputLFNBases[2],
                                                     mergedLFNBase=outputLFNBases[2+5])},
                   "Task3": {'AODSIMoutput': dict(dataTier='AODSIM', filterName='', transient=True,
                                                  primaryDataset=REQUEST['Task3']['PrimaryDataset'],
                                                  processedDataset="AcqEra_Task3-ProcStr_Task3-v23",
                                                  lfnBase=outputLFNBases[3],
                                                  mergedLFNBase=outputLFNBases[3+5])},
                   "Task4": {'MINIAODSIMoutput': dict(dataTier='MINIAODSIM', filterName='', transient=True,
                                                      primaryDataset=REQUEST['Task4']['PrimaryDataset'],
                                                      processedDataset="AcqEra_Task4-ProcStr_Task4-v24",
                                                      lfnBase=outputLFNBases[4],
                                                      mergedLFNBase=outputLFNBases[4+5])}
                   }
        mergedMods = deepcopy(outMods)
        mergedMods['Task1']['LHEoutput'].update({'transient': False, 'lfnBase': outputLFNBases[0+5]})
        mergedMods['Task1']['RAWSIMoutput'].update({'transient': False, 'lfnBase': outputLFNBases[1+5]})
        mergedMods['Task2']['PREMIXRAWoutput'].update({'transient': False, 'lfnBase': outputLFNBases[2+5]})
        mergedMods['Task3']['AODSIMoutput'].update({'transient': False, 'lfnBase': outputLFNBases[3+5]})
        mergedMods['Task4']['MINIAODSIMoutput'].update({'transient': False, 'lfnBase': outputLFNBases[4+5]})

        # create a taskChain workload
        testWorkload = buildComplexTaskChain(self.configDatabase)

        # Case 1: only workload creation
        lfnBases = ("/store/unmerged", "/store/data")
        outputDsets = [dset for k, dsets in outDsets.iteritems() for dset in dsets]
        self.assertItemsEqual(testWorkload.listOutputDatasets(), outputDsets)
        self.assertItemsEqual(testWorkload.listAllOutputModulesLFNBases(onlyUnmerged=False), outputLFNBases)
        for t in ["Task1", "Task2", "Task3", "Task4"]:
            task = testWorkload.getTaskByName(REQUEST[t]['TaskName'])
            self._checkOutputDsetsAndMods(task, outMods[t], outDsets[t], lfnBases)
            # then test the merge tasks
            for modName in mergedMods[t]:
                mergeName = REQUEST[t]['TaskName'] + "Merge" + modName
                task = testWorkload.getTaskByName(mergeName)
                step = task.getStepHelper("cmsRun1")
                self._validateOutputModule('Merged', step.getOutputModule('Merged'), mergedMods[t][modName])

        # Case 2: workload creation and assignment, with no output dataset override
        assignDict = {"SiteWhitelist": ["T2_US_Nebraska", "T2_IT_Rome"], "Team": "The-A-Team"}
        testWorkload.updateArguments(assignDict)
        self.assertItemsEqual(testWorkload.listOutputDatasets(), outputDsets)
        self.assertItemsEqual(testWorkload.listAllOutputModulesLFNBases(onlyUnmerged=False), outputLFNBases)
        for t in ["Task1", "Task2", "Task3", "Task4"]:
            task = testWorkload.getTaskByName(REQUEST[t]['TaskName'])
            self._checkOutputDsetsAndMods(task, outMods[t], outDsets[t], lfnBases, assigned=True)
            # then test the merge tasks
            for modName, value in mergedMods[t].iteritems():
                mergeName = REQUEST[t]['TaskName'] + "Merge" + modName
                task = testWorkload.getTaskByName(mergeName)
                step = task.getStepHelper("cmsRun1")
                self._validateOutputModule('Merged', step.getOutputModule('Merged'), mergedMods[t][modName])

        # Case 3: workload creation and assignment, output dataset overriden with the same values
        testWorkload = buildComplexTaskChain(self.configDatabase)
        assignDict = {"SiteWhitelist": ["T2_US_Nebraska", "T2_IT_Rome"], "Team": "The-A-Team",
                      "AcquisitionEra": {"myTask1": "AcqEra_Task1", "myTask2": "AcqEra_Task2",
                                         "myTask3": "AcqEra_Task3", "myTask4": "AcqEra_Task4"},
                      "ProcessingString": {"myTask1": "ProcStr_Task1", "myTask2": "ProcStr_Task2",
                                           "myTask3": "ProcStr_Task3", "myTask4": "ProcStr_Task4"},
                      "ProcessingVersion": {"myTask1": 21, "myTask2": 22, "myTask3": 23, "myTask4": 24},
                      "MergedLFNBase": "/store/data",
                      "UnmergedLFNBase": "/store/unmerged"
                      }
        testWorkload.updateArguments(assignDict)
        for t in ["Task1", "Task2", "Task3", "Task4"]:
            task = testWorkload.getTaskByName(REQUEST[t]['TaskName'])
            self._checkOutputDsetsAndMods(task, outMods[t], outDsets[t], lfnBases, assigned=True)
            # then test the merge tasks
            for modName, value in mergedMods[t].iteritems():
                mergeName = REQUEST[t]['TaskName'] + "Merge" + modName
                task = testWorkload.getTaskByName(mergeName)
                step = task.getStepHelper("cmsRun1")
                self._validateOutputModule('Merged', step.getOutputModule('Merged'), mergedMods[t][modName])

        # Case 4: workload creation and assignment, output dataset overriden with new values
        testWorkload = buildComplexTaskChain(self.configDatabase)
        assignDict = {"SiteWhitelist": ["T2_US_Nebraska", "T2_IT_Rome"], "Team": "The-A-Team",
                      "AcquisitionEra": {"myTask1": "AcqEra_TaskA", "myTask2": "AcqEra_TaskB",
                                         "myTask3": "AcqEra_TaskC", "myTask4": "AcqEra_TaskD"},
                      "ProcessingString": {"myTask1": "ProcStr_TaskA", "myTask2": "ProcStr_TaskB",
                                           "myTask3": "ProcStr_TaskC", "myTask4": "ProcStr_TaskD"},
                      "ProcessingVersion": {"myTask1": 11, "myTask2": 12, "myTask3": 13, "myTask4": 14},
                      "MergedLFNBase": "/store/mc",
                      "UnmergedLFNBase": "/store/unmerged"
                      }
        testWorkload.updateArguments(assignDict)


        for tp in [("Task1", "TaskA"), ("Task2", "TaskB"), ("Task3", "TaskC"), ("Task4", "TaskD")]:
            outDsets[tp[0]] = [dset.replace(tp[0], tp[1]) for dset in outDsets[tp[0]]]
            outputLFNBases = [lfn.replace(tp[0], tp[1]) for lfn in outputLFNBases]
            for mod in outMods[tp[0]]:
                outMods[tp[0]][mod] = {k: (v.replace(tp[0], tp[1]) if isinstance(v, basestring) else v)
                                       for k, v in outMods[tp[0]][mod].items()}
            for tpp in [("v21", "v11"), ("v22", "v12"), ("v23", "v13"), ("v24", "v14"),("/store/data", "/store/mc")]:
                outDsets[tp[0]] = [dset.replace(tpp[0], tpp[1]) for dset in outDsets[tp[0]]]
                outputLFNBases = [lfn.replace(tpp[0], tpp[1]) for lfn in outputLFNBases]
                for mod in outMods[tp[0]]:
                    outMods[tp[0]][mod] = {k: (v.replace(tpp[0], tpp[1]) if isinstance(v, basestring) else v)
                                           for k, v in outMods[tp[0]][mod].items()}
        mergedMods = deepcopy(outMods)
        mergedMods['Task1']['LHEoutput'].update({'transient': False, 'lfnBase': outputLFNBases[0+5]})
        mergedMods['Task1']['RAWSIMoutput'].update({'transient': False, 'lfnBase': outputLFNBases[1+5]})
        mergedMods['Task2']['PREMIXRAWoutput'].update({'transient': False, 'lfnBase': outputLFNBases[2+5]})
        mergedMods['Task3']['AODSIMoutput'].update({'transient': False, 'lfnBase': outputLFNBases[3+5]})
        mergedMods['Task4']['MINIAODSIMoutput'].update({'transient': False, 'lfnBase': outputLFNBases[4+5]})

        lfnBases = ("/store/unmerged", "/store/mc")
        for t in ["Task1", "Task2", "Task3", "Task4"]:
            task = testWorkload.getTaskByName(REQUEST[t]['TaskName'])
            self._checkOutputDsetsAndMods(task, outMods[t], outDsets[t], lfnBases, assigned=True)
            # then test the merge tasks
            for modName, value in mergedMods[t].iteritems():
                mergeName = REQUEST[t]['TaskName'] + "Merge" + modName
                task = testWorkload.getTaskByName(mergeName)
                step = task.getStepHelper("cmsRun1")
                self._validateOutputModule('Merged', step.getOutputModule('Merged'), mergedMods[t][modName])

        return

    def _checkOutputDsetsAndMods(self, task, outMods, outDsets, lfnBases, assigned=False):
        """
        Validate data related to output dataset, output modules and subscriptions
        :param task: task object
        :param outMods: dictionary with the output module info for this task
        :param outDsets: dictionary with the output datasets info for this task
        :param assigned: flag saying whether the workload was assigned or not
        """
        self.assertItemsEqual(task.getIgnoredOutputModulesForTask(), [])

        self.assertItemsEqual(task._getLFNBase(), lfnBases)
        outputDsets = [x['outputDataset'] for x in task.listOutputDatasetsAndModules()]
        self.assertItemsEqual(outputDsets, outDsets)
        outModDict = task.getOutputModulesForTask(cmsRunOnly=True)[0].dictionary_()  # only 1 cmsRun process
        self.assertItemsEqual(outModDict.keys(), outMods.keys())
        for modName, outMod in outModDict.iteritems():
            self._validateOutputModule(modName, outModDict[modName], outMods[modName])

        if assigned:
            defaultSubs = {'Priority': 'Low', 'NonCustodialSites': [], 'AutoApproveSites': [],
                           'DeleteFromSource': False, 'NonCustodialGroup': 'DataOps', 'CustodialSites': [],
                           'CustodialSubType': 'Replica', 'CustodialGroup': 'DataOps', 'NonCustodialSubType': 'Replica'}
            subscription = {dset: defaultSubs for dset in outputDsets}
        else:
            subscription = {}
        self.assertDictEqual(task.getSubscriptionInformation(), subscription)

        # step level checks
        self.assertEqual(task.getTopStepName(), 'cmsRun1')
        step = task.getStepHelper(task.getTopStepName())
        self.assertItemsEqual(step.listOutputModules(), outMods.keys())
        for modName in outMods:
            self._validateOutputModule(modName, step.getOutputModule(modName), outMods[modName])

    def _validateOutputModule(self, outModName, outModObj, dictExp):
        """
        Make sure the task/step provided output module object contains
        the same values as the expected from the request json settings.
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


if __name__ == '__main__':
    unittest.main()
