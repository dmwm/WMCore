#!/usr/bin/env python
"""
_Express_t_

Unit tests for the Express workflow.
"""

from __future__ import division

import unittest

from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow

from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMCore.WMSpec.StdSpecs.Express import ExpressWorkloadFactory

from WMQuality.TestInitCouchApp import TestInitCouchApp


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
        testArguments['ProcessingString'] = "Express"
        testArguments['ProcessingVersion'] = 9
        testArguments['Scenario'] = "test_scenario"
        testArguments['CMSSWVersion'] = "CMSSW_9_0_0"
        testArguments['ScramArch'] = "slc6_amd64_gcc530"
        testArguments['RecoCMSSWVersion'] = "CMSSW_9_0_0"
        testArguments['RecoScramArch'] = "slc6_amd64_gcc530"
        testArguments['GlobalTag'] = "SomeGlobalTag"
        testArguments['GlobalTagTransaction'] = "Express_123456"
        testArguments['GlobalTagConnect'] = "frontier://PromptProd/CMS_CONDITIONS"
        testArguments['MaxInputRate'] = 23 * 1000
        testArguments['MaxInputEvents'] = 400
        testArguments['MaxInputSize'] = 2 * 1024 * 1024 * 1024
        testArguments['MaxInputFiles'] = 15
        testArguments['MaxLatency'] = 15 * 23
        testArguments['AlcaSkims'] = [ "TkAlMinBias", "PromptCalibProd" ]
        testArguments['DQMSequences'] = [ "@common" ]
        testArguments['AlcaHarvestTimeout'] = 12 * 3600
        testArguments['AlcaHarvestDir'] = "/somepath"
        testArguments['DQMUploadProxy'] = "/somepath/proxy"
        testArguments['DQMUploadUrl'] = "https://cmsweb.cern.ch/dqm/offline"
        testArguments['StreamName'] = "Express"
        testArguments['SpecialDataset'] = "StreamExpress"

        testArguments['RunNumber'] = 123456
        testArguments['AcquisitionEra'] = "TestAcquisitionEra"
        testArguments['ValidStatus'] = "VALID"

        testArguments['Outputs'] = []
        testArguments['Outputs'].append( { 'dataTier' : "FEVT",
                                           'eventContent' : "FEVT",
                                           'selectEvents' : ["Path1:HLT,Path2:HLT"],
                                           'primaryDataset' : "PrimaryDataset1" } )
        testArguments['Outputs'].append( { 'dataTier' : "ALCARECO",
                                           'eventContent' : "ALCARECO",
                                           'primaryDataset' : "StreamExpress" } )
        testArguments['Outputs'].append( { 'dataTier' : "DQMIO",
                                           'eventContent' : "DQMIO",
                                           'primaryDataset' : "StreamExpress" } )

        testArguments['PeriodicHarvestInterval'] = 20 * 60
                                         
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
        self.assertEqual(len(expressWorkflow.outputMap.keys()), len(testArguments["Outputs"]) + 1,
                         "Error: Wrong number of WF outputs in the Express WF.")

        goldenOutputMods = ["write_PrimaryDataset1_FEVT", "write_StreamExpress_ALCARECO", "write_StreamExpress_DQMIO"]
        for goldenOutputMod in goldenOutputMods:
            mergedOutput = expressWorkflow.outputMap[goldenOutputMod][0]["merged_output_fileset"]
            unmergedOutput = expressWorkflow.outputMap[goldenOutputMod][0]["output_fileset"]
            mergedOutput.loadData()
            unmergedOutput.loadData()

            if goldenOutputMod != "write_StreamExpress_ALCARECO":
                self.assertEqual(mergedOutput.name, "/TestWorkload/Express/ExpressMerge%s/merged-Merged" % goldenOutputMod,
                                 "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/Express/unmerged-%s" % goldenOutputMod,
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
        self.assertEqual(len(alcaSkimWorkflow.outputMap.keys()), len(testArguments["AlcaSkims"]) + 1,
                         "Error: Wrong number of WF outputs in the AlcaSkim WF.")

        goldenOutputMods = []
        for alcaProd in testArguments["AlcaSkims"]:
            goldenOutputMods.append("ALCARECOStream%s" % alcaProd)

        for goldenOutputMod in goldenOutputMods:
            mergedOutput = alcaSkimWorkflow.outputMap[goldenOutputMod][0]["merged_output_fileset"]
            unmergedOutput = alcaSkimWorkflow.outputMap[goldenOutputMod][0]["output_fileset"]
            mergedOutput.loadData()
            unmergedOutput.loadData()
            self.assertEqual(mergedOutput.name, "/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/unmerged-%s" % goldenOutputMod,
                             "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/unmerged-%s" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong: %s" % unmergedOutput.name)

        logArchOutput = alcaSkimWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = alcaSkimWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name, "/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/unmerged-logArchive",
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

        goldenOutputMods = ["write_PrimaryDataset1_FEVT", "write_StreamExpress_DQMIO"]
        for goldenOutputMod in goldenOutputMods:
            mergeWorkflow = Workflow(name="TestWorkload",
                                     task="/TestWorkload/Express/ExpressMerge%s" % goldenOutputMod)
            mergeWorkflow.load()

            self.assertEqual(len(mergeWorkflow.outputMap.keys()), 2,
                             "Error: Wrong number of WF outputs.")

            mergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["merged_output_fileset"]
            unmergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["output_fileset"]

            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()

            self.assertEqual(mergedMergeOutput.name, "/TestWorkload/Express/ExpressMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong.")
            self.assertEqual(unmergedMergeOutput.name, "/TestWorkload/Express/ExpressMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong.")

            logArchOutput = mergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = mergeWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()

            self.assertEqual(logArchOutput.name, "/TestWorkload/Express/ExpressMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
            self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Express/ExpressMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")

        topLevelFileset = Fileset(name="TestWorkload-Express")
        topLevelFileset.loadData()

        expressSubscription = Subscription(fileset=topLevelFileset, workflow=expressWorkflow)
        expressSubscription.loadData()

        self.assertEqual(expressSubscription["type"], "Express",
                         "Error: Wrong subscription type.")
        self.assertEqual(expressSubscription["split_algo"], "Express",
                         "Error: Wrong split algorithm. %s" % expressSubscription["split_algo"])

        alcaRecoFileset = Fileset(name="/TestWorkload/Express/unmerged-write_StreamExpress_ALCARECO")
        alcaRecoFileset.loadData()

        alcaSkimSubscription = Subscription(fileset=alcaRecoFileset, workflow=alcaSkimWorkflow)
        alcaSkimSubscription.loadData()

        self.assertEqual(alcaSkimSubscription["type"], "Express",
                         "Error: Wrong subscription type.")
        self.assertEqual(alcaSkimSubscription["split_algo"], "ExpressMerge",
                         "Error: Wrong split algorithm. %s" % alcaSkimSubscription["split_algo"])

        mergedDQMFileset = Fileset(name="/TestWorkload/Express/ExpressMergewrite_StreamExpress_DQMIO/merged-Merged")
        mergedDQMFileset.loadData()

        dqmSubscription = Subscription(fileset=mergedDQMFileset, workflow=dqmWorkflow)
        dqmSubscription.loadData()

        self.assertEqual(dqmSubscription["type"], "Harvesting",
                         "Error: Wrong subscription type.")
        self.assertEqual(dqmSubscription["split_algo"], "Harvest",
                         "Error: Wrong split algo.")

        unmergedOutputs = ["write_PrimaryDataset1_FEVT", "write_StreamExpress_DQMIO"]
        for unmergedOutput in unmergedOutputs:
            unmergedDataTier = Fileset(name="/TestWorkload/Express/unmerged-%s" % unmergedOutput)
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

        goldenOutputMods = ["write_PrimaryDataset1_FEVT", "write_StreamExpress_ALCARECO", "write_StreamExpress_DQMIO"]
        for goldenOutputMod in goldenOutputMods:
            unmergedFileset = Fileset(name="/TestWorkload/Express/unmerged-%s" % goldenOutputMod)
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

        alcaSkimLogCollect = Fileset(name="/TestWorkload/Express/ExpressAlcaSkimwrite_StreamExpress_ALCARECO/unmerged-logArchive")
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
            expressMergeLogCollect = Fileset(name="/TestWorkload/Express/ExpressMerge%s/merged-logArchive" % goldenOutputMod)
            expressMergeLogCollect.loadData()
            expressMergeLogCollectWorkflow = Workflow(name="TestWorkload",
                                                      task="/TestWorkload/Express/ExpressMerge%s/Express%sMergeLogCollect" % (goldenOutputMod, goldenOutputMod))
            expressMergeLogCollectWorkflow.load()
            logCollectSubscription = Subscription(fileset=expressMergeLogCollect, workflow=expressMergeLogCollectWorkflow)
            logCollectSubscription.loadData()

            self.assertEqual(logCollectSub["type"], "LogCollect",
                             "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                             "Error: Wrong split algorithm.")

        for dqmStrings in [("ExpressMergewrite_StreamExpress_DQMIOEndOfRunDQMHarvestMerged",
                            "ExpressMergewrite_StreamExpress_DQMIOMergedEndOfRunDQMHarvestLogCollect"),
                           ("ExpressMergewrite_StreamExpress_DQMIOPeriodicDQMHarvestMerged",
                            "ExpressMergewrite_StreamExpress_DQMIOMergedPeriodicDQMHarvestLogCollect")]:

            dqmFileset = "/TestWorkload/Express/ExpressMergewrite_StreamExpress_DQMIO/%s/unmerged-logArchive" % dqmStrings[0]
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
        testArguments['ProcessingString'] = "Express"
        testArguments['ProcessingVersion'] = 9
        testArguments['Scenario'] = "test_scenario"
        testArguments['CMSSWVersion'] = "CMSSW_9_0_0"
        testArguments['ScramArch'] = "slc6_amd64_gcc530"
        testArguments['RecoCMSSWVersion'] = "CMSSW_9_0_0"
        testArguments['RecoScramArch'] = "slc6_amd64_gcc530"
        testArguments['GlobalTag'] = "SomeGlobalTag"
        testArguments['GlobalTagTransaction'] = "Express_123456"
        testArguments['GlobalTagConnect'] = "frontier://PromptProd/CMS_CONDITIONS"
        testArguments['MaxInputRate'] = 23 * 1000
        testArguments['MaxInputEvents'] = 400
        testArguments['MaxInputSize'] = 2 * 1024 * 1024 * 1024
        testArguments['MaxInputFiles'] = 15
        testArguments['MaxLatency'] = 15 * 23
        testArguments['AlcaSkims'] = [ "TkAlMinBias", "PromptCalibProd" ]
        testArguments['DQMSequences'] = [ "@common" ]
        testArguments['AlcaHarvestTimeout'] = 12 * 3600
        testArguments['AlcaHarvestDir'] = "/somepath"
        testArguments['DQMUploadProxy'] = "/somepath/proxy"
        testArguments['DQMUploadUrl'] = "https://cmsweb.cern.ch/dqm/offline"
        testArguments['StreamName'] = "Express"
        testArguments['SpecialDataset'] = "StreamExpress"

        testArguments['RunNumber'] = 123456
        testArguments['AcquisitionEra'] = "TestAcquisitionEra"
        testArguments['ValidStatus'] = "VALID"

        testArguments['Outputs'] = []
        testArguments['Outputs'].append( { 'dataTier' : "FEVT",
                                           'eventContent' : "FEVT",
                                           'selectEvents' : ["Path1:HLT,Path2:HLT"],
                                           'primaryDataset' : "PrimaryDataset1" } )
        testArguments['Outputs'].append( { 'dataTier' : "ALCARECO",
                                           'eventContent' : "ALCARECO",
                                           'primaryDataset' : "StreamExpress" } )
        testArguments['Outputs'].append( { 'dataTier' : "DQMIO",
                                           'eventContent' : "DQMIO",
                                           'primaryDataset' : "StreamExpress" } )

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
        testArguments = ExpressWorkloadFactory.getTestArguments()
        testArguments['ProcessingString'] = "Express"
        testArguments['ProcessingVersion'] = 9
        testArguments['Scenario'] = "test_scenario"
        testArguments['CMSSWVersion'] = "CMSSW_9_0_0"
        testArguments['ScramArch'] = "slc6_amd64_gcc530"
        testArguments['RecoCMSSWVersion'] = "CMSSW_9_0_0"
        testArguments['RecoScramArch'] = "slc6_amd64_gcc530"
        testArguments['GlobalTag'] = "SomeGlobalTag"
        testArguments['GlobalTagTransaction'] = "Express_123456"
        testArguments['GlobalTagConnect'] = "frontier://PromptProd/CMS_CONDITIONS"
        testArguments['MaxInputRate'] = 23 * 1000
        testArguments['MaxInputEvents'] = 400
        testArguments['MaxInputSize'] = 2 * 1024 * 1024 * 1024
        testArguments['MaxInputFiles'] = 15
        testArguments['MaxLatency'] = 15 * 23
        testArguments['AlcaSkims'] = [ "TkAlMinBias", "PromptCalibProd" ]
        testArguments['DQMSequences'] = [ "@common" ]
        testArguments['AlcaHarvestTimeout'] = 12 * 3600
        testArguments['AlcaHarvestDir'] = "/somepath"
        testArguments['DQMUploadProxy'] = "/somepath/proxy"
        testArguments['DQMUploadUrl'] = "https://cmsweb.cern.ch/dqm/offline"
        testArguments['StreamName'] = "Express"
        testArguments['SpecialDataset'] = "StreamExpress"

        testArguments['RunNumber'] = 123456
        testArguments['AcquisitionEra'] = "TestAcquisitionEra"
        testArguments['ValidStatus'] = "VALID"

        testArguments['Outputs'] = []
        testArguments['Outputs'].append( { 'dataTier' : "FEVT",
                                           'eventContent' : "FEVT",
                                           'selectEvents' : ["Path1:HLT,Path2:HLT"],
                                           'primaryDataset' : "PrimaryDataset1" } )
        testArguments['Outputs'].append( { 'dataTier' : "ALCARECO",
                                           'eventContent' : "ALCARECO",
                                           'primaryDataset' : "StreamExpress" } )
        testArguments['Outputs'].append( { 'dataTier' : "DQMIO",
                                           'eventContent' : "DQMIO",
                                           'primaryDataset' : "StreamExpress" } )

        testArguments["Multicore"] = 6
        testArguments["Memory"] = 4600.0
        testArguments["EventStreams"] = 3

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
        testArguments['ProcessingString'] = "Express"
        testArguments['ProcessingVersion'] = 9
        testArguments['Scenario'] = "test_scenario"
        testArguments['CMSSWVersion'] = "CMSSW_9_0_0"
        testArguments['ScramArch'] = "slc6_amd64_gcc530"
        testArguments['RecoCMSSWVersion'] = "CMSSW_9_0_1"
        testArguments['RecoScramArch'] = "slc6_amd64_gcc630"
        testArguments['GlobalTag'] = "SomeGlobalTag"
        testArguments['GlobalTagTransaction'] = "Express_123456"
        testArguments['GlobalTagConnect'] = "frontier://PromptProd/CMS_CONDITIONS"
        testArguments['MaxInputRate'] = 23 * 1000
        testArguments['MaxInputEvents'] = 400
        testArguments['MaxInputSize'] = 2 * 1024 * 1024 * 1024
        testArguments['MaxInputFiles'] = 15
        testArguments['MaxLatency'] = 15 * 23
        testArguments['AlcaSkims'] = [ "TkAlMinBias", "PromptCalibProd" ]
        testArguments['DQMSequences'] = [ "@common" ]
        testArguments['AlcaHarvestTimeout'] = 12 * 3600
        testArguments['AlcaHarvestDir'] = "/somepath"
        testArguments['DQMUploadProxy'] = "/somepath/proxy"
        testArguments['DQMUploadUrl'] = "https://cmsweb.cern.ch/dqm/offline"
        testArguments['StreamName'] = "Express"
        testArguments['SpecialDataset'] = "StreamExpress"

        testArguments['RunNumber'] = 123456
        testArguments['AcquisitionEra'] = "TestAcquisitionEra"
        testArguments['ValidStatus'] = "VALID"

        testArguments['Outputs'] = []
        testArguments['Outputs'].append( { 'dataTier' : "FEVT",
                                           'eventContent' : "FEVT",
                                           'selectEvents' : ["Path1:HLT,Path2:HLT"],
                                           'primaryDataset' : "PrimaryDataset1" } )
        testArguments['Outputs'].append( { 'dataTier' : "ALCARECO",
                                           'eventContent' : "ALCARECO",
                                           'primaryDataset' : "StreamExpress" } )
        testArguments['Outputs'].append( { 'dataTier' : "DQMIO",
                                           'eventContent' : "DQMIO",
                                           'primaryDataset' : "StreamExpress" } )

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

if __name__ == '__main__':
    unittest.main()
