#!/usr/bin/env python
"""
_Express_t_

Unit tests for the Express workflow.
"""

from __future__ import division, print_function
from future.utils import viewitems

import threading
import unittest
from copy import deepcopy

from Utils.PythonVersion import PY3

from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMSpec.StdSpecs.Express import ExpressWorkloadFactory
from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMQuality.TestInitCouchApp import TestInitCouchApp

REQUEST = {
    "AcquisitionEra": "TestAcquisitionEra",
    "AlcaHarvestTimeout": 12 * 3600,
    "AlcaHarvestCondLFNBase": "somelfn",
    "AlcaHarvestLumiURL": "someurl",
    "AlcaSkims": ["TkAlMinBias", "PromptCalibProd"],
    "CMSSWVersion": "CMSSW_9_0_0",
    "DQMSequences": ["@common"],
    "DQMUploadProxy": "/somepath/proxy",
    "DQMUploadUrl": "https://cmsweb.cern.ch/dqm/offline",
    "GlobalTag": "SomeGlobalTag",
    "GlobalTagConnect": "frontier://PromptProd/CMS_CONDITIONS",
    "MaxInputRate": 23 * 1000,
    "MaxInputEvents": 400,
    "MaxInputSize": 2 * 1024 * 1024 * 1024,
    "MaxInputFiles": 15,
    "MaxLatency": 15 * 23,
    "Outputs": [{'dataTier': "FEVT", 'eventContent': "FEVT",
                 'selectEvents': ["Path1:HLT,Path2:HLT"], 'primaryDataset': "PrimaryDataset1"},
                {'dataTier': "ALCARECO", 'eventContent': "ALCARECO", 'primaryDataset': "StreamExpress"},
                {'dataTier': "DQMIO", 'eventContent': "DQMIO", 'primaryDataset': "StreamExpress"}],
    "PeriodicHarvestInterval": 20 * 60,
    "ProcessingString": "Express",
    "ProcessingVersion": 9,
    "RecoCMSSWVersion": "CMSSW_9_0_1",
    "RecoScramArch": "slc6_amd64_gcc630",
    "RunNumber": 123456,
    "ScramArch": "slc6_amd64_gcc530",
    "Scenario": "test_scenario",
    "SpecialDataset": "StreamExpress",
    "StreamName": "Express"
}


class ExpressTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Initialize the database and couch.
        """
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules=["WMCore.WMBS"],
                                useDefault=False)
        self.testDir = self.testInit.generateWorkDir()

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
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()
        return

    def testExpress(self):
        """
        _testExpress_

        Create an Express workflow
        and verify it installs into WMBS correctly.
        """
        testArguments = ExpressWorkloadFactory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))
        testArguments['RecoCMSSWVersion'] = "CMSSW_9_0_0"

        factory = ExpressWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)
        testWorkload.setSpecUrl("somespec")
        testWorkload.setOwnerDetails("Dirk.Hufnagel@cern.ch", "T0")

        testWMBSHelper = WMBSHelper(testWorkload, "Express", cachepath=self.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        expressWorkflow = Workflow(name="TestWorkload",
                                   task="/TestWorkload/Express")
        expressWorkflow.load()
        self.assertEqual(len(expressWorkflow.outputMap), len(testArguments["Outputs"]) + 1,
                         "Error: Wrong number of WF outputs in the Express WF.")

        goldenOutputMods = {"write_PrimaryDataset1_FEVT": "FEVT",
                            "write_StreamExpress_ALCARECO": "ALCARECO",
                            "write_StreamExpress_DQMIO": "DQMIO"}
        for goldenOutputMod, tier in viewitems(goldenOutputMods):
            fset = goldenOutputMod + tier
            mergedOutput = expressWorkflow.outputMap[fset][0]["merged_output_fileset"]
            unmergedOutput = expressWorkflow.outputMap[fset][0]["output_fileset"]
            mergedOutput.loadData()
            unmergedOutput.loadData()

            if goldenOutputMod != "write_StreamExpress_ALCARECO":
                self.assertEqual(mergedOutput.name,
                                 "/TestWorkload/Express/ExpressMerge%s/merged-Merged%s" % (goldenOutputMod, tier),
                                 "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/Express/unmerged-%s" % fset,
                             "Error: Unmerged output fileset is wrong: %s" % unmergedOutput.name)

        logArchOutput = expressWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = expressWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name, "/TestWorkload/Express/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Express/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        alcaSkimWorkflow = Workflow(name="TestWorkload",
                                    task="/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO")
        alcaSkimWorkflow.load()
        self.assertEqual(len(alcaSkimWorkflow.outputMap), len(testArguments["AlcaSkims"]) + 1,
                         "Error: Wrong number of WF outputs in the AlcaSkim WF.")

        goldenOutputMods = {"ALCARECOStreamPromptCalibProd": "ALCAPROMPT",
                            "ALCARECOStreamTkAlMinBias": "ALCARECO"}
        for goldenOutputMod, tier in viewitems(goldenOutputMods):
            fset = goldenOutputMod + tier
            mergedOutput = alcaSkimWorkflow.outputMap[fset][0]["merged_output_fileset"]
            unmergedOutput = alcaSkimWorkflow.outputMap[fset][0]["output_fileset"]
            mergedOutput.loadData()
            unmergedOutput.loadData()
            self.assertEqual(mergedOutput.name,
                             "/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/unmerged-%s" % fset,
                             "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name,
                             "/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/unmerged-%s" % fset,
                             "Error: Unmerged output fileset is wrong: %s" % unmergedOutput.name)

        logArchOutput = alcaSkimWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = alcaSkimWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name,
                         "/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name,
                         "/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        for dqmString in ["ExpressMergewrite_StreamExpress_DQMIOEndOfRunDQMHarvestMerged",
                          "ExpressMergewrite_StreamExpress_DQMIOPeriodicDQMHarvestMerged"]:
            dqmTask = "/TestWorkload/Express/ExpressMergewrite_StreamExpress_DQMIO/%s" % dqmString

            dqmWorkflow = Workflow(name="TestWorkload",
                                   task=dqmTask)
            dqmWorkflow.load()

            logArchOutput = dqmWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = dqmWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()

            self.assertEqual(logArchOutput.name,
                             "%s/unmerged-logArchive" % dqmTask,
                             "Error: LogArchive output fileset is wrong.")
            self.assertEqual(unmergedLogArchOutput.name,
                             "%s/unmerged-logArchive" % dqmTask,
                             "Error: LogArchive output fileset is wrong.")

        goldenOutputMods = {"write_PrimaryDataset1_FEVT": "FEVT",
                            "write_StreamExpress_DQMIO": "DQMIO"}
        for goldenOutputMod, tier in viewitems(goldenOutputMods):
            mergeWorkflow = Workflow(name="TestWorkload",
                                     task="/TestWorkload/Express/ExpressMerge%s" % goldenOutputMod)
            mergeWorkflow.load()

            self.assertEqual(len(mergeWorkflow.outputMap), 2,
                             "Error: Wrong number of WF outputs.")

            mergedMergeOutput = mergeWorkflow.outputMap["Merged%s" % tier][0]["merged_output_fileset"]
            unmergedMergeOutput = mergeWorkflow.outputMap["Merged%s" % tier][0]["output_fileset"]

            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()

            self.assertEqual(mergedMergeOutput.name,
                             "/TestWorkload/Express/ExpressMerge%s/merged-Merged%s" % (goldenOutputMod, tier),
                             "Error: Merged output fileset is wrong.")
            self.assertEqual(unmergedMergeOutput.name,
                             "/TestWorkload/Express/ExpressMerge%s/merged-Merged%s" % (goldenOutputMod, tier),
                             "Error: Unmerged output fileset is wrong.")

            logArchOutput = mergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = mergeWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()

            self.assertEqual(logArchOutput.name,
                             "/TestWorkload/Express/ExpressMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
            self.assertEqual(unmergedLogArchOutput.name,
                             "/TestWorkload/Express/ExpressMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")

        topLevelFileset = Fileset(name="TestWorkload-Express")
        topLevelFileset.loadData()

        expressSubscription = Subscription(fileset=topLevelFileset, workflow=expressWorkflow)
        expressSubscription.loadData()

        self.assertEqual(expressSubscription["type"], "Express",
                         "Error: Wrong subscription type.")
        self.assertEqual(expressSubscription["split_algo"], "Express",
                         "Error: Wrong split algorithm. %s" % expressSubscription["split_algo"])

        alcaRecoFileset = Fileset(name="/TestWorkload/Express/unmerged-write_StreamExpress_ALCARECOALCARECO")
        alcaRecoFileset.loadData()

        alcaSkimSubscription = Subscription(fileset=alcaRecoFileset, workflow=alcaSkimWorkflow)
        alcaSkimSubscription.loadData()

        self.assertEqual(alcaSkimSubscription["type"], "Express",
                         "Error: Wrong subscription type.")
        self.assertEqual(alcaSkimSubscription["split_algo"], "ExpressMerge",
                         "Error: Wrong split algorithm. %s" % alcaSkimSubscription["split_algo"])

        mergedDQMFileset = Fileset(
            name="/TestWorkload/Express/ExpressMergewrite_StreamExpress_DQMIO/merged-MergedDQMIO")
        mergedDQMFileset.loadData()

        dqmSubscription = Subscription(fileset=mergedDQMFileset, workflow=dqmWorkflow)
        dqmSubscription.loadData()

        self.assertEqual(dqmSubscription["type"], "Harvesting",
                         "Error: Wrong subscription type.")
        self.assertEqual(dqmSubscription["split_algo"], "Harvest",
                         "Error: Wrong split algo.")

        unmergedOutputs = {"write_PrimaryDataset1_FEVT": "FEVT",
                           "write_StreamExpress_DQMIO": "DQMIO"}
        for unmergedOutput, tier in viewitems(unmergedOutputs):
            fset = unmergedOutput + tier
            unmergedDataTier = Fileset(name="/TestWorkload/Express/unmerged-%s" % fset)
            unmergedDataTier.loadData()
            dataTierMergeWorkflow = Workflow(name="TestWorkload",
                                             task="/TestWorkload/Express/ExpressMerge%s" % unmergedOutput)
            dataTierMergeWorkflow.load()
            mergeSubscription = Subscription(fileset=unmergedDataTier, workflow=dataTierMergeWorkflow)
            mergeSubscription.loadData()

            self.assertEqual(mergeSubscription["type"], "Merge",
                             "Error: Wrong subscription type.")
            self.assertEqual(mergeSubscription["split_algo"], "ExpressMerge",
                             "Error: Wrong split algorithm. %s" % mergeSubscription["split_algo"])

        goldenOutputMods = {"write_PrimaryDataset1_FEVT": "FEVT",
                            "write_StreamExpress_ALCARECO": "ALCARECO",
                            "write_StreamExpress_DQMIO": "DQMIO"}
        for goldenOutputMod, tier in viewitems(goldenOutputMods):
            fset = goldenOutputMod + tier
            unmergedFileset = Fileset(name="/TestWorkload/Express/unmerged-%s" % fset)
            unmergedFileset.loadData()
            cleanupWorkflow = Workflow(name="TestWorkload",
                                       task="/TestWorkload/Express/ExpressCleanupUnmerged%s" % goldenOutputMod)
            cleanupWorkflow.load()
            cleanupSubscription = Subscription(fileset=unmergedFileset, workflow=cleanupWorkflow)
            cleanupSubscription.loadData()

            self.assertEqual(cleanupSubscription["type"], "Cleanup",
                             "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong subscription type.")

        expressLogCollect = Fileset(name="/TestWorkload/Express/unmerged-logArchive")
        expressLogCollect.loadData()
        expressLogCollectWorkflow = Workflow(name="TestWorkload",
                                             task="/TestWorkload/Express/ExpressLogCollect")
        expressLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset=expressLogCollect, workflow=expressLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algorithm.")

        alcaSkimLogCollect = Fileset(
            name="/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/unmerged-logArchive")
        alcaSkimLogCollect.loadData()
        alcaSkimLogCollectWorkflow = Workflow(name="TestWorkload",
                                              task="/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/AlcaSkimLogCollect")
        alcaSkimLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset=alcaSkimLogCollect, workflow=alcaSkimLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algorithm.")

        goldenOutputMods = ["write_PrimaryDataset1_FEVT", "write_StreamExpress_DQMIO"]
        for goldenOutputMod in goldenOutputMods:
            expressMergeLogCollect = Fileset(
                name="/TestWorkload/Express/ExpressMerge%s/merged-logArchive" % goldenOutputMod)
            expressMergeLogCollect.loadData()
            expressMergeLogCollectWorkflow = Workflow(name="TestWorkload",
                                                      task="/TestWorkload/Express/ExpressMerge%s/Express%sMergeLogCollect" % (
                                                          goldenOutputMod, goldenOutputMod))
            expressMergeLogCollectWorkflow.load()
            logCollectSubscription = Subscription(fileset=expressMergeLogCollect,
                                                  workflow=expressMergeLogCollectWorkflow)
            logCollectSubscription.loadData()

            self.assertEqual(logCollectSub["type"], "LogCollect",
                             "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                             "Error: Wrong split algorithm.")

        for dqmStrings in [("ExpressMergewrite_StreamExpress_DQMIOEndOfRunDQMHarvestMerged",
                            "ExpressMergewrite_StreamExpress_DQMIOMergedEndOfRunDQMHarvestLogCollect"),
                           ("ExpressMergewrite_StreamExpress_DQMIOPeriodicDQMHarvestMerged",
                            "ExpressMergewrite_StreamExpress_DQMIOMergedPeriodicDQMHarvestLogCollect")]:
            dqmFileset = "/TestWorkload/Express/ExpressMergewrite_StreamExpress_DQMIO/%s/unmerged-logArchive" % \
                         dqmStrings[0]
            dqmHarvestLogCollect = Fileset(name=dqmFileset)
            dqmHarvestLogCollect.loadData()

            dqmTask = "/TestWorkload/Express/ExpressMergewrite_StreamExpress_DQMIO/%s/%s" % dqmStrings
            dqmHarvestLogCollectWorkflow = Workflow(name="TestWorkload", task=dqmTask)
            dqmHarvestLogCollectWorkflow.load()

            logCollectSub = Subscription(fileset=dqmHarvestLogCollect, workflow=dqmHarvestLogCollectWorkflow)
            logCollectSub.loadData()

            self.assertEqual(logCollectSub["type"], "LogCollect",
                             "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                             "Error: Wrong split algo.")

        return

    def testMemCoresSettings(self):
        """
        _testMemCoresSettings_

        Make sure the multicore and memory setings are properly propagated to
        all tasks and steps.
        """
        testArguments = ExpressWorkloadFactory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))

        factory = ExpressWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        # test default values
        taskPaths = ['/TestWorkload/Express', '/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO']
        for task in taskPaths:
            taskObj = testWorkload.getTaskByPath(task)
            for step in ('cmsRun1', 'stageOut1', 'logArch1'):
                stepHelper = taskObj.getStepHelper(step)
                self.assertEqual(stepHelper.getNumberOfCores(), 1)
                self.assertEqual(stepHelper.getNumberOfStreams(), 0)
            # then test Memory requirements
            perfParams = taskObj.jobSplittingParameters()['performance']
            self.assertEqual(perfParams['memoryRequirement'], 2300.0)

        # now test case where args are provided
        # testArguments = ExpressWorkloadFactory.getTestArguments()
        testArguments["Multicore"] = 6
        testArguments["Memory"] = 4600.0
        testArguments["EventStreams"] = 3
        testArguments["Outputs"] = deepcopy(REQUEST['Outputs'])

        factory = ExpressWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        for task in taskPaths:
            taskObj = testWorkload.getTaskByPath(task)
            for step in ('cmsRun1', 'stageOut1', 'logArch1'):
                stepHelper = taskObj.getStepHelper(step)
                if task == '/TestWorkload/Express' and step == 'cmsRun1':
                    self.assertEqual(stepHelper.getNumberOfCores(), testArguments["Multicore"])
                    self.assertEqual(stepHelper.getNumberOfStreams(), testArguments["EventStreams"])
                elif step in ('stageOut1', 'logArch1'):
                    self.assertEqual(stepHelper.getNumberOfCores(), 1)
                    self.assertEqual(stepHelper.getNumberOfStreams(), 0)
                else:
                    self.assertEqual(stepHelper.getNumberOfCores(), 1, "%s should be single-core" % task)
                    self.assertEqual(stepHelper.getNumberOfStreams(), 0)
            # then test Memory requirements
            perfParams = taskObj.jobSplittingParameters()['performance']
            self.assertEqual(perfParams['memoryRequirement'], testArguments["Memory"])

        return

    def testWithRepackStep(self):
        """
        _testWithRepackStep_

        Make sure we get an initial repack step if CMSSW versions are mismatched
        """
        testArguments = ExpressWorkloadFactory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))

        factory = ExpressWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        taskObj = testWorkload.getTaskByPath('/TestWorkload/Express')

        stepHelper = taskObj.getStepHelper('cmsRun1')
        self.assertEqual(stepHelper.getScenario(), "test_scenario")
        self.assertEqual(stepHelper.getCMSSWVersion(), "CMSSW_9_0_0")
        self.assertEqual(stepHelper.getScramArch(), ["slc6_amd64_gcc530"])

        stepHelper = taskObj.getStepHelper('cmsRun2')
        self.assertEqual(stepHelper.getScenario(), "test_scenario")
        self.assertEqual(stepHelper.getCMSSWVersion(), "CMSSW_9_0_1")
        self.assertEqual(stepHelper.getScramArch(), ["slc6_amd64_gcc630"])

        return

    def testFilesets(self):
        """
        Test filesets created for an Express workflow
        """
        # expected tasks, filesets, subscriptions, etc
        expOutTasks = ['/TestWorkload/Express',
                       '/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO',
                       '/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/ExpressAlcaSkimwrite_StreamExpress_ALCARECOAlcaHarvestALCARECOStreamPromptCalibProd',
                       '/TestWorkload/Express/ExpressMergewrite_StreamExpress_DQMIO',
                       '/TestWorkload/Express/ExpressMergewrite_PrimaryDataset1_FEVT']
        expWfTasks = ['/TestWorkload/Express',
                      '/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO',
                      '/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/AlcaSkimLogCollect',
                      '/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/ExpressAlcaSkimwrite_StreamExpress_ALCARECOAlcaHarvestALCARECOStreamPromptCalibProd',
                      '/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/ExpressAlcaSkimwrite_StreamExpress_ALCARECOAlcaHarvestALCARECOStreamPromptCalibProd/ExpressAlcaSkimwrite_StreamExpress_ALCARECOAlcaHarvestALCARECOStreamPromptCalibProdConditionSqlite',
                      '/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/ExpressAlcaSkimwrite_StreamExpress_ALCARECOAlcaHarvestALCARECOStreamPromptCalibProd/ExpressAlcaSkimwrite_StreamExpress_ALCARECOALCARECOStreamPromptCalibProdAlcaHarvestLogCollect',
                      '/TestWorkload/Express/ExpressCleanupUnmergedwrite_PrimaryDataset1_FEVT',
                      '/TestWorkload/Express/ExpressCleanupUnmergedwrite_StreamExpress_ALCARECO',
                      '/TestWorkload/Express/ExpressCleanupUnmergedwrite_StreamExpress_DQMIO',
                      '/TestWorkload/Express/ExpressLogCollect',
                      '/TestWorkload/Express/ExpressMergewrite_PrimaryDataset1_FEVT',
                      '/TestWorkload/Express/ExpressMergewrite_PrimaryDataset1_FEVT/Expresswrite_PrimaryDataset1_FEVTMergeLogCollect',
                      '/TestWorkload/Express/ExpressMergewrite_StreamExpress_DQMIO',
                      '/TestWorkload/Express/ExpressMergewrite_StreamExpress_DQMIO/ExpressMergewrite_StreamExpress_DQMIOEndOfRunDQMHarvestMerged',
                      '/TestWorkload/Express/ExpressMergewrite_StreamExpress_DQMIO/ExpressMergewrite_StreamExpress_DQMIOEndOfRunDQMHarvestMerged/ExpressMergewrite_StreamExpress_DQMIOMergedEndOfRunDQMHarvestLogCollect',
                      '/TestWorkload/Express/ExpressMergewrite_StreamExpress_DQMIO/ExpressMergewrite_StreamExpress_DQMIOPeriodicDQMHarvestMerged',
                      '/TestWorkload/Express/ExpressMergewrite_StreamExpress_DQMIO/ExpressMergewrite_StreamExpress_DQMIOPeriodicDQMHarvestMerged/ExpressMergewrite_StreamExpress_DQMIOMergedPeriodicDQMHarvestLogCollect',
                      '/TestWorkload/Express/ExpressMergewrite_StreamExpress_DQMIO/Expresswrite_StreamExpress_DQMIOMergeLogCollect']
        expFsets = ['TestWorkload-Express-Run123456',
                    '/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/unmerged-ALCARECOStreamPromptCalibProdALCAPROMPT',
                    '/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/unmerged-ALCARECOStreamTkAlMinBiasALCARECO',
                    '/TestWorkload/Express/unmerged-write_RAWRAW',
                    '/TestWorkload/Express/unmerged-write_StreamExpress_ALCARECOALCARECO',
                    '/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/ExpressAlcaSkimwrite_StreamExpress_ALCARECOAlcaHarvestALCARECOStreamPromptCalibProd/unmerged-logArchive',
                    '/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/ExpressAlcaSkimwrite_StreamExpress_ALCARECOAlcaHarvestALCARECOStreamPromptCalibProd/unmerged-SqliteALCAPROMPT',
                    '/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/unmerged-logArchive',
                    '/TestWorkload/Express/ExpressMergewrite_StreamExpress_DQMIO/ExpressMergewrite_StreamExpress_DQMIOPeriodicDQMHarvestMerged/unmerged-logArchive',
                    '/TestWorkload/Express/ExpressMergewrite_StreamExpress_DQMIO/merged-MergedDQMIO',
                    '/TestWorkload/Express/unmerged-write_StreamExpress_DQMIODQMIO',
                    '/TestWorkload/Express/ExpressMergewrite_PrimaryDataset1_FEVT/merged-logArchive',
                    '/TestWorkload/Express/ExpressMergewrite_PrimaryDataset1_FEVT/merged-MergedFEVT',
                    '/TestWorkload/Express/ExpressMergewrite_StreamExpress_DQMIO/ExpressMergewrite_StreamExpress_DQMIOEndOfRunDQMHarvestMerged/unmerged-logArchive',
                    '/TestWorkload/Express/ExpressMergewrite_StreamExpress_DQMIO/merged-logArchive',
                    '/TestWorkload/Express/unmerged-logArchive',
                    '/TestWorkload/Express/unmerged-write_PrimaryDataset1_FEVTFEVT']

        subMaps = [(8,
                    '/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/ExpressAlcaSkimwrite_StreamExpress_ALCARECOAlcaHarvestALCARECOStreamPromptCalibProd/unmerged-logArchive',
                    '/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/ExpressAlcaSkimwrite_StreamExpress_ALCARECOAlcaHarvestALCARECOStreamPromptCalibProd/ExpressAlcaSkimwrite_StreamExpress_ALCARECOALCARECOStreamPromptCalibProdAlcaHarvestLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (7,
                    '/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/ExpressAlcaSkimwrite_StreamExpress_ALCARECOAlcaHarvestALCARECOStreamPromptCalibProd/unmerged-SqliteALCAPROMPT',
                    '/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/ExpressAlcaSkimwrite_StreamExpress_ALCARECOAlcaHarvestALCARECOStreamPromptCalibProd/ExpressAlcaSkimwrite_StreamExpress_ALCARECOAlcaHarvestALCARECOStreamPromptCalibProdConditionSqlite',
                    'Condition',
                    'Harvesting'),
                   (6,
                    '/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/unmerged-ALCARECOStreamPromptCalibProdALCAPROMPT',
                    '/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/ExpressAlcaSkimwrite_StreamExpress_ALCARECOAlcaHarvestALCARECOStreamPromptCalibProd',
                    'AlcaHarvest',
                    'Harvesting'),
                   (9,
                    '/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/unmerged-logArchive',
                    '/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/AlcaSkimLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (4,
                    '/TestWorkload/Express/ExpressMergewrite_PrimaryDataset1_FEVT/merged-logArchive',
                    '/TestWorkload/Express/ExpressMergewrite_PrimaryDataset1_FEVT/Expresswrite_PrimaryDataset1_FEVTMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (14,
                    '/TestWorkload/Express/ExpressMergewrite_StreamExpress_DQMIO/ExpressMergewrite_StreamExpress_DQMIOEndOfRunDQMHarvestMerged/unmerged-logArchive',
                    '/TestWorkload/Express/ExpressMergewrite_StreamExpress_DQMIO/ExpressMergewrite_StreamExpress_DQMIOEndOfRunDQMHarvestMerged/ExpressMergewrite_StreamExpress_DQMIOMergedEndOfRunDQMHarvestLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (16,
                    '/TestWorkload/Express/ExpressMergewrite_StreamExpress_DQMIO/ExpressMergewrite_StreamExpress_DQMIOPeriodicDQMHarvestMerged/unmerged-logArchive',
                    '/TestWorkload/Express/ExpressMergewrite_StreamExpress_DQMIO/ExpressMergewrite_StreamExpress_DQMIOPeriodicDQMHarvestMerged/ExpressMergewrite_StreamExpress_DQMIOMergedPeriodicDQMHarvestLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (17,
                    '/TestWorkload/Express/ExpressMergewrite_StreamExpress_DQMIO/merged-logArchive',
                    '/TestWorkload/Express/ExpressMergewrite_StreamExpress_DQMIO/Expresswrite_StreamExpress_DQMIOMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (13,
                    '/TestWorkload/Express/ExpressMergewrite_StreamExpress_DQMIO/merged-MergedDQMIO',
                    '/TestWorkload/Express/ExpressMergewrite_StreamExpress_DQMIO/ExpressMergewrite_StreamExpress_DQMIOEndOfRunDQMHarvestMerged',
                    'Harvest',
                    'Harvesting'),
                   (15,
                    '/TestWorkload/Express/ExpressMergewrite_StreamExpress_DQMIO/merged-MergedDQMIO',
                    '/TestWorkload/Express/ExpressMergewrite_StreamExpress_DQMIO/ExpressMergewrite_StreamExpress_DQMIOPeriodicDQMHarvestMerged',
                    'Harvest',
                    'Harvesting'),
                   (18,
                    '/TestWorkload/Express/unmerged-logArchive',
                    '/TestWorkload/Express/ExpressLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (2,
                    '/TestWorkload/Express/unmerged-write_PrimaryDataset1_FEVTFEVT',
                    '/TestWorkload/Express/ExpressCleanupUnmergedwrite_PrimaryDataset1_FEVT',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (3,
                    '/TestWorkload/Express/unmerged-write_PrimaryDataset1_FEVTFEVT',
                    '/TestWorkload/Express/ExpressMergewrite_PrimaryDataset1_FEVT',
                    'ExpressMerge',
                    'Merge'),
                   (5,
                    '/TestWorkload/Express/unmerged-write_StreamExpress_ALCARECOALCARECO',
                    '/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO',
                    'ExpressMerge',
                    'Express'),
                   (10,
                    '/TestWorkload/Express/unmerged-write_StreamExpress_ALCARECOALCARECO',
                    '/TestWorkload/Express/ExpressCleanupUnmergedwrite_StreamExpress_ALCARECO',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (11,
                    '/TestWorkload/Express/unmerged-write_StreamExpress_DQMIODQMIO',
                    '/TestWorkload/Express/ExpressCleanupUnmergedwrite_StreamExpress_DQMIO',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (12,
                    '/TestWorkload/Express/unmerged-write_StreamExpress_DQMIODQMIO',
                    '/TestWorkload/Express/ExpressMergewrite_StreamExpress_DQMIO',
                    'ExpressMerge',
                    'Merge'),
                   (1,
                    'TestWorkload-Express-Run123456',
                    '/TestWorkload/Express',
                    'Express',
                    'Express')]

        testArguments = ExpressWorkloadFactory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))

        factory = ExpressWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        testWMBSHelper = WMBSHelper(testWorkload, "Express",
                                    blockName="Run123456",
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


if __name__ == '__main__':
    unittest.main()
