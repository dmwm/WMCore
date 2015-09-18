#!/usr/bin/env python
"""
_PromptReco_t_

Unit tests for the new Tier1 PromptReconstruction workflow.
"""

import unittest
import os

from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow

from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMCore.WMSpec.StdSpecs.PromptReco import PromptRecoWorkloadFactory

from WMCore.Configuration import ConfigSection

from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.Database.CMSCouch import CouchServer

from nose.plugins.attrib import attr

class PromptRecoTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Initialize the database and couch.
        """
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("promptreco_t", "ConfigCache")
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("promptreco_t")
        self.testDir = self.testInit.generateWorkDir()
        return

    def tearDown(self):
        """
        _tearDown_

        Clear out the database.
        """
        self.testInit.tearDownCouch()
        self.testInit.delWorkDir()
        return

    def setupPromptSkimConfigObject(self):
        """
        _setupPromptSkimConfigObject_
        Creates a custom config object for testing
        of the skim functionality
        """
        self.promptSkim = ConfigSection(name="Tier1Skim")
        self.promptSkim.SkimName = "TestSkim1"
        self.promptSkim.DataTier = "RECO"
        self.promptSkim.TwoFileRead = False
        self.promptSkim.ProcessingVersion = "PromptSkim-v1"
        self.promptSkim.ConfigURL = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/DataOps/python/prescaleskimmer.py?revision=1.1"

    def testPromptReco(self):
        """
        _testPromptReco_

        Create a Prompt Reconstruction workflow
        and verify it installs into WMBS correctly.
        """
        testArguments = PromptRecoWorkloadFactory.getTestArguments()
        testArguments["CouchURL"] = os.environ["COUCHURL"]
        testArguments["EnableHarvesting"] = True

        factory = PromptRecoWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)
        testWorkload.setSpecUrl("somespec")
        testWorkload.setOwnerDetails("dballest@fnal.gov", "T0")

        testWMBSHelper = WMBSHelper(testWorkload, "Reco", "SomeBlock", cachepath = self.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        recoWorkflow = Workflow(name = "TestWorkload",
                                task = "/TestWorkload/Reco")
        recoWorkflow.load()
        self.assertEqual(len(recoWorkflow.outputMap.keys()), len(testArguments["WriteTiers"]) + 1,
                         "Error: Wrong number of WF outputs in the Reco WF.")

        goldenOutputMods = ["write_RECO", "write_ALCARECO", "write_AOD", "write_DQM"]
        for goldenOutputMod in goldenOutputMods:
            mergedOutput = recoWorkflow.outputMap[goldenOutputMod][0]["merged_output_fileset"]
            unmergedOutput = recoWorkflow.outputMap[goldenOutputMod][0]["output_fileset"]
            mergedOutput.loadData()
            unmergedOutput.loadData()

            if goldenOutputMod != "write_ALCARECO":
                self.assertEqual(mergedOutput.name, "/TestWorkload/Reco/RecoMerge%s/merged-Merged" % goldenOutputMod,
                                 "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/Reco/unmerged-%s" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong: %s" % unmergedOutput.name)

        logArchOutput = recoWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = recoWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name, "/TestWorkload/Reco/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Reco/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        alcaSkimWorkflow = Workflow(name = "TestWorkload",
                                    task = "/TestWorkload/Reco/AlcaSkim")
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
            self.assertEqual(mergedOutput.name, "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/Reco/AlcaSkim/unmerged-%s" % goldenOutputMod,
                              "Error: Unmerged output fileset is wrong: %s" % unmergedOutput.name)

        logArchOutput = alcaSkimWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = alcaSkimWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name, "/TestWorkload/Reco/AlcaSkim/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Reco/AlcaSkim/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        dqmWorkflow = Workflow(name = "TestWorkload",
                               task = "/TestWorkload/Reco/RecoMergewrite_DQM/RecoMergewrite_DQMEndOfRunDQMHarvestMerged")
        dqmWorkflow.load()

        logArchOutput = dqmWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = dqmWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name, "/TestWorkload/Reco/RecoMergewrite_DQM/RecoMergewrite_DQMEndOfRunDQMHarvestMerged/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Reco/RecoMergewrite_DQM/RecoMergewrite_DQMEndOfRunDQMHarvestMerged/unmerged-logArchive",
                     "Error: LogArchive output fileset is wrong.")

        goldenOutputMods = ["write_RECO", "write_AOD", "write_DQM"]
        for goldenOutputMod in goldenOutputMods:
            mergeWorkflow = Workflow(name = "TestWorkload",
                                     task = "/TestWorkload/Reco/RecoMerge%s" % goldenOutputMod)
            mergeWorkflow.load()

            self.assertEqual(len(mergeWorkflow.outputMap.keys()), 2,
                             "Error: Wrong number of WF outputs.")

            mergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["merged_output_fileset"]
            unmergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["output_fileset"]

            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()

            self.assertEqual(mergedMergeOutput.name, "/TestWorkload/Reco/RecoMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong.")
            self.assertEqual(unmergedMergeOutput.name, "/TestWorkload/Reco/RecoMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong.")

            logArchOutput = mergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = mergeWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()

            self.assertEqual(logArchOutput.name, "/TestWorkload/Reco/RecoMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
            self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Reco/RecoMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")

        goldenOutputMods = []
        for alcaProd in testArguments["AlcaSkims"]:
            goldenOutputMods.append("ALCARECOStream%s" % alcaProd)

        for goldenOutputMod in goldenOutputMods:
            mergeWorkflow = Workflow(name = "TestWorkload",
                                     task = "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s" % goldenOutputMod)
            mergeWorkflow.load()

            self.assertEqual(len(mergeWorkflow.outputMap.keys()), 2,
                             "Error: Wrong number of WF outputs %d." % len(mergeWorkflow.outputMap.keys()))

            mergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["merged_output_fileset"]
            unmergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["output_fileset"]

            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()

            self.assertEqual(mergedMergeOutput.name, "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong.")
            self.assertEqual(unmergedMergeOutput.name, "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong.")

            logArchOutput = mergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = mergeWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()

            self.assertEqual(logArchOutput.name, "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
            self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")

        topLevelFileset = Fileset(name = "TestWorkload-Reco-SomeBlock")
        topLevelFileset.loadData()

        recoSubscription = Subscription(fileset = topLevelFileset, workflow = recoWorkflow)
        recoSubscription.loadData()

        self.assertEqual(recoSubscription["type"], "Processing",
                         "Error: Wrong subscription type.")
        self.assertEqual(recoSubscription["split_algo"], "EventBased",
                         "Error: Wrong split algorithm. %s" % recoSubscription["split_algo"])

        alcaRecoFileset = Fileset(name = "/TestWorkload/Reco/unmerged-write_ALCARECO")
        alcaRecoFileset.loadData()

        alcaSkimSubscription = Subscription(fileset = alcaRecoFileset, workflow = alcaSkimWorkflow)
        alcaSkimSubscription.loadData()

        self.assertEqual(alcaSkimSubscription["type"], "Processing",
                         "Error: Wrong subscription type.")
        self.assertEqual(alcaSkimSubscription["split_algo"], "WMBSMergeBySize",
                         "Error: Wrong split algorithm. %s" % alcaSkimSubscription["split_algo"])

        mergedDQMFileset = Fileset(name = "/TestWorkload/Reco/RecoMergewrite_DQM/merged-Merged")
        mergedDQMFileset.loadData()

        dqmSubscription = Subscription(fileset = mergedDQMFileset, workflow = dqmWorkflow)
        dqmSubscription.loadData()

        self.assertEqual(dqmSubscription["type"], "Harvesting",
                         "Error: Wrong subscription type.")
        self.assertEqual(dqmSubscription["split_algo"], "Harvest",
                         "Error: Wrong split algo.")

        unmergedOutputs = ["write_RECO", "write_AOD", "write_DQM"]
        for unmergedOutput in unmergedOutputs:
            unmergedDataTier = Fileset(name = "/TestWorkload/Reco/unmerged-%s" % unmergedOutput)
            unmergedDataTier.loadData()
            dataTierMergeWorkflow = Workflow(name = "TestWorkload",
                                             task = "/TestWorkload/Reco/RecoMerge%s" % unmergedOutput)
            dataTierMergeWorkflow.load()
            mergeSubscription = Subscription(fileset = unmergedDataTier, workflow = dataTierMergeWorkflow)
            mergeSubscription.loadData()

            self.assertEqual(mergeSubscription["type"], "Merge",
                             "Error: Wrong subscription type.")
            self.assertEqual(mergeSubscription["split_algo"], "WMBSMergeBySize",
                             "Error: Wrong split algorithm. %s" % mergeSubscription["split_algo"])
        unmergedOutputs = []
        for alcaProd in testArguments["AlcaSkims"]:
            unmergedOutputs.append("ALCARECOStream%s" % alcaProd)
        for unmergedOutput in unmergedOutputs:
            unmergedAlcaSkim = Fileset(name = "/TestWorkload/Reco/AlcaSkim/unmerged-%s" % unmergedOutput)
            unmergedAlcaSkim.loadData()
            alcaSkimMergeWorkflow = Workflow(name = "TestWorkload",
                                             task = "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s" % unmergedOutput)
            alcaSkimMergeWorkflow.load()
            mergeSubscription = Subscription(fileset = unmergedAlcaSkim, workflow = alcaSkimMergeWorkflow)
            mergeSubscription.loadData()

            self.assertEqual(mergeSubscription["type"], "Merge",
                             "Error: Wrong subscription type.")
            self.assertEqual(mergeSubscription["split_algo"], "WMBSMergeBySize",
                             "Error: Wrong split algorithm. %s" % mergeSubscription["split_algo"])

        goldenOutputMods = ["write_RECO", "write_AOD", "write_DQM", "write_ALCARECO"]
        for goldenOutputMod in goldenOutputMods:
            unmergedFileset = Fileset(name = "/TestWorkload/Reco/unmerged-%s" % goldenOutputMod)
            unmergedFileset.loadData()
            cleanupWorkflow = Workflow(name = "TestWorkload",
                                       task = "/TestWorkload/Reco/RecoCleanupUnmerged%s" % goldenOutputMod)
            cleanupWorkflow.load()
            cleanupSubscription = Subscription(fileset = unmergedFileset, workflow = cleanupWorkflow)
            cleanupSubscription.loadData()

            self.assertEqual(cleanupSubscription["type"], "Cleanup",
                             "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong subscription type.")

        goldenOutputMods = []
        for alcaProd in testArguments["AlcaSkims"]:
            goldenOutputMods.append("ALCARECOStream%s" % alcaProd)
        for goldenOutputMod in goldenOutputMods:
            unmergedFileset = Fileset(name = "/TestWorkload/Reco/AlcaSkim/unmerged-%s" % goldenOutputMod)
            unmergedFileset.loadData()
            cleanupWorkflow = Workflow(name = "TestWorkload",
                                       task = "/TestWorkload/Reco/AlcaSkim/AlcaSkimCleanupUnmerged%s" %goldenOutputMod)
            cleanupWorkflow.load()
            cleanupSubscription = Subscription(fileset = unmergedFileset, workflow = cleanupWorkflow)
            cleanupSubscription.loadData()

            self.assertEqual(cleanupSubscription["type"], "Cleanup",
                             "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong subscription type.")

        recoLogCollect = Fileset(name = "/TestWorkload/Reco/unmerged-logArchive")
        recoLogCollect.loadData()
        recoLogCollectWorkflow = Workflow(name = "TestWorkload",
                                          task = "/TestWorkload/Reco/LogCollect")
        recoLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset = recoLogCollect, workflow = recoLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algorithm.")

        alcaSkimLogCollect = Fileset(name = "/TestWorkload/Reco/AlcaSkim/unmerged-logArchive")
        alcaSkimLogCollect.loadData()
        alcaSkimLogCollectWorkflow = Workflow(name = "TestWorkload",
                                                task = "/TestWorkload/Reco/AlcaSkim/AlcaSkimLogCollect")
        alcaSkimLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset = alcaSkimLogCollect, workflow = alcaSkimLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algorithm.")

        goldenOutputMods = ["write_RECO", "write_AOD", "write_DQM"]
        for goldenOutputMod in goldenOutputMods:
            recoMergeLogCollect = Fileset(name = "/TestWorkload/Reco/RecoMerge%s/merged-logArchive" % goldenOutputMod)
            recoMergeLogCollect.loadData()
            recoMergeLogCollectWorkflow = Workflow(name = "TestWorkload",
                                       task = "/TestWorkload/Reco/RecoMerge%s/Reco%sMergeLogCollect" % (goldenOutputMod, goldenOutputMod))
            recoMergeLogCollectWorkflow.load()
            logCollectSubscription = Subscription(fileset = recoMergeLogCollect, workflow = recoMergeLogCollectWorkflow)
            logCollectSubscription.loadData()

            self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algorithm.")

        goldenOutputMods = []
        for alcaProd in testArguments["AlcaSkims"]:
            goldenOutputMods.append("ALCARECOStream%s" % alcaProd)
        for goldenOutputMod in goldenOutputMods:
            alcaSkimLogCollect = Fileset(name = "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-logArchive" % goldenOutputMod)
            alcaSkimLogCollect.loadData()
            alcaSkimLogCollectWorkflow = Workflow(name = "TestWorkload",
                                       task = "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/AlcaSkim%sMergeLogCollect" % (goldenOutputMod, goldenOutputMod))
            alcaSkimLogCollectWorkflow.load()
            logCollectSubscription = Subscription(fileset = alcaSkimLogCollect, workflow = alcaSkimLogCollectWorkflow)
            logCollectSubscription.loadData()

            self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algorithm.")

        dqmHarvestLogCollect = Fileset(name = "/TestWorkload/Reco/RecoMergewrite_DQM/RecoMergewrite_DQMEndOfRunDQMHarvestMerged/unmerged-logArchive")
        dqmHarvestLogCollect.loadData()
        dqmHarvestLogCollectWorkflow = Workflow(name = "TestWorkload",
                                               task = "/TestWorkload/Reco/RecoMergewrite_DQM/RecoMergewrite_DQMEndOfRunDQMHarvestMerged/RecoMergewrite_DQMMergedEndOfRunDQMHarvestLogCollect")
        dqmHarvestLogCollectWorkflow.load()

        logCollectSub = Subscription(fileset = dqmHarvestLogCollect, workflow = dqmHarvestLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algo.")

        return

    @attr("integration")
    def testPromptRecoWithSkims(self):
        """
        _testT1PromptRecoWithSkim_

        Create a T1 Prompt Reconstruction workflow with PromptSkims
        and verify it installs into WMBS correctly.
        """
        self.setupPromptSkimConfigObject()
        testArguments = PromptRecoWorkloadFactory.getTestArguments()
        testArguments["PromptSkims"] = [self.promptSkim]
        testArguments["CouchURL"] = os.environ["COUCHURL"]
        testArguments["CouchDBName"] = "promptreco_t"
        testArguments["EnvPath"] = os.environ.get("EnvPath", None)
        testArguments["BinPath"] = os.environ.get("BinPath", None)

        factory = PromptRecoWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)
        testWorkload.setSpecUrl("somespec")
        testWorkload.setOwnerDetails("dballest@fnal.gov", "T0")

        testWMBSHelper = WMBSHelper(testWorkload, "Reco", "SomeBlock", cachepath = self.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        recoWorkflow = Workflow(name = "TestWorkload",
                                task = "/TestWorkload/Reco")
        recoWorkflow.load()
        self.assertEqual(len(recoWorkflow.outputMap.keys()), len(testArguments["WriteTiers"]) + 1,
                         "Error: Wrong number of WF outputs in the Reco WF.")

        goldenOutputMods = ["write_RECO", "write_ALCARECO", "write_AOD", "write_DQM"]
        for goldenOutputMod in goldenOutputMods:
            mergedOutput = recoWorkflow.outputMap[goldenOutputMod][0]["merged_output_fileset"]
            unmergedOutput = recoWorkflow.outputMap[goldenOutputMod][0]["output_fileset"]
            mergedOutput.loadData()
            unmergedOutput.loadData()

            if goldenOutputMod != "write_ALCARECO":
                self.assertEqual(mergedOutput.name, "/TestWorkload/Reco/RecoMerge%s/merged-Merged" % goldenOutputMod,
                                 "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/Reco/unmerged-%s" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong: %s" % unmergedOutput.name)

        logArchOutput = recoWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = recoWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name, "/TestWorkload/Reco/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Reco/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        alcaSkimWorkflow = Workflow(name = "TestWorkload",
                                    task = "/TestWorkload/Reco/AlcaSkim")
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
            self.assertEqual(mergedOutput.name, "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/Reco/AlcaSkim/unmerged-%s" % goldenOutputMod,
                              "Error: Unmerged output fileset is wrong: %s" % unmergedOutput.name)

        logArchOutput = alcaSkimWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = alcaSkimWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name, "/TestWorkload/Reco/AlcaSkim/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Reco/AlcaSkim/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        promptSkimWorkflow = Workflow(name="TestWorkload",
                                      task="/TestWorkload/Reco/RecoMergewrite_RECO/TestSkim1")
        promptSkimWorkflow.load()

        self.assertEqual(len(promptSkimWorkflow.outputMap.keys()), 6,
                         "Error: Wrong number of WF outputs.")

        goldenOutputMods = ["fakeSkimOut1", "fakeSkimOut2", "fakeSkimOut3",
                            "fakeSkimOut4", "fakeSkimOut5"]
        for goldenOutputMod in goldenOutputMods:
            mergedOutput = promptSkimWorkflow.outputMap[goldenOutputMod][0]["merged_output_fileset"]
            unmergedOutput = promptSkimWorkflow.outputMap[goldenOutputMod][0]["output_fileset"]
            mergedOutput.loadData()
            unmergedOutput.loadData()

            self.assertEqual(mergedOutput.name, "/TestWorkload/Reco/RecoMergewrite_RECO/TestSkim1/TestSkim1Merge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/Reco/RecoMergewrite_RECO/TestSkim1/unmerged-%s" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong: %s" % unmergedOutput.name)

        logArchOutput = promptSkimWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = promptSkimWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name, "/TestWorkload/Reco/RecoMergewrite_RECO/TestSkim1/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Reco/RecoMergewrite_RECO/TestSkim1/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        goldenOutputMods = ["write_RECO", "write_AOD", "write_DQM"]
        for goldenOutputMod in goldenOutputMods:
            mergeWorkflow = Workflow(name = "TestWorkload",
                                     task = "/TestWorkload/Reco/RecoMerge%s" % goldenOutputMod)
            mergeWorkflow.load()

            self.assertEqual(len(mergeWorkflow.outputMap.keys()), 2,
                             "Error: Wrong number of WF outputs.")

            mergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["merged_output_fileset"]
            unmergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["output_fileset"]

            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()

            self.assertEqual(mergedMergeOutput.name, "/TestWorkload/Reco/RecoMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong.")
            self.assertEqual(unmergedMergeOutput.name, "/TestWorkload/Reco/RecoMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong.")

            logArchOutput = mergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = mergeWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()

            self.assertEqual(logArchOutput.name, "/TestWorkload/Reco/RecoMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
            self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Reco/RecoMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")

        goldenOutputMods = []
        for alcaProd in testArguments["AlcaSkims"]:
            goldenOutputMods.append("ALCARECOStream%s" % alcaProd)

        for goldenOutputMod in goldenOutputMods:
            mergeWorkflow = Workflow(name = "TestWorkload",
                                     task = "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s" % goldenOutputMod)
            mergeWorkflow.load()

            self.assertEqual(len(mergeWorkflow.outputMap.keys()), 2,
                             "Error: Wrong number of WF outputs %d." % len(mergeWorkflow.outputMap.keys()))

            mergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["merged_output_fileset"]
            unmergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["output_fileset"]

            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()

            self.assertEqual(mergedMergeOutput.name, "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong.")
            self.assertEqual(unmergedMergeOutput.name, "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong.")

            logArchOutput = mergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = mergeWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()

            self.assertEqual(logArchOutput.name, "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
            self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")

        goldenOutputMods = ["fakeSkimOut1", "fakeSkimOut2", "fakeSkimOut3",
                            "fakeSkimOut4", "fakeSkimOut5"]
        for goldenOutputMod in goldenOutputMods:
            mergeWorkflow = Workflow(name = "TestWorkload",
                                     task = "/TestWorkload/Reco/RecoMergewrite_RECO/TestSkim1/TestSkim1Merge%s" % goldenOutputMod)
            mergeWorkflow.load()

            self.assertEqual(len(mergeWorkflow.outputMap.keys()), 2,
                             "Error: Wrong number of WF outputs %d." % len(mergeWorkflow.outputMap.keys()))

            mergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["merged_output_fileset"]
            unmergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["output_fileset"]

            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()

            self.assertEqual(mergedMergeOutput.name, "/TestWorkload/Reco/RecoMergewrite_RECO/TestSkim1/TestSkim1Merge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong.")
            self.assertEqual(unmergedMergeOutput.name, "/TestWorkload/Reco/RecoMergewrite_RECO/TestSkim1/TestSkim1Merge%s/merged-Merged" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong.")

            logArchOutput = mergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = mergeWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()

            self.assertEqual(logArchOutput.name, "/TestWorkload/Reco/RecoMergewrite_RECO/TestSkim1/TestSkim1Merge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
            self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Reco/RecoMergewrite_RECO/TestSkim1/TestSkim1Merge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")

        topLevelFileset = Fileset(name = "TestWorkload-Reco-SomeBlock")
        topLevelFileset.loadData()

        recoSubscription = Subscription(fileset = topLevelFileset, workflow = recoWorkflow)
        recoSubscription.loadData()

        self.assertEqual(recoSubscription["type"], "Processing",
                         "Error: Wrong subscription type.")
        self.assertEqual(recoSubscription["split_algo"], "EventBased",
                         "Error: Wrong split algorithm. %s" % recoSubscription["split_algo"])

        alcaRecoFileset = Fileset(name = "/TestWorkload/Reco/unmerged-write_ALCARECO")
        alcaRecoFileset.loadData()

        alcaSkimSubscription = Subscription(fileset = alcaRecoFileset, workflow = alcaSkimWorkflow)
        alcaSkimSubscription.loadData()

        self.assertEqual(alcaSkimSubscription["type"], "Processing",
                         "Error: Wrong subscription type.")
        self.assertEqual(alcaSkimSubscription["split_algo"], "WMBSMergeBySize",
                         "Error: Wrong split algorithm. %s" % alcaSkimSubscription["split_algo"])

        mergedRecoFileset = Fileset(name = "/TestWorkload/Reco/RecoMergewrite_RECO/merged-Merged")
        mergedRecoFileset.loadData()

        promptSkimSubscription = Subscription(fileset = mergedRecoFileset, workflow = promptSkimWorkflow)
        promptSkimSubscription.loadData()

        self.assertEqual(promptSkimSubscription["type"], "Skim",
                         "Error: Wrong subscription type.")
        self.assertEqual(promptSkimSubscription["split_algo"], "FileBased",
                         "Error: Wrong split algorithm. %s" % promptSkimSubscription["split_algo"])

        unmergedOutputs = ["write_RECO", "write_AOD", "write_DQM"]
        for unmergedOutput in unmergedOutputs:
            unmergedDataTier = Fileset(name = "/TestWorkload/Reco/unmerged-%s" % unmergedOutput)
            unmergedDataTier.loadData()
            dataTierMergeWorkflow = Workflow(name = "TestWorkload",
                                             task = "/TestWorkload/Reco/RecoMerge%s" % unmergedOutput)
            dataTierMergeWorkflow.load()
            mergeSubscription = Subscription(fileset = unmergedDataTier, workflow = dataTierMergeWorkflow)
            mergeSubscription.loadData()

            self.assertEqual(mergeSubscription["type"], "Merge",
                             "Error: Wrong subscription type.")
            self.assertEqual(mergeSubscription["split_algo"], "WMBSMergeBySize",
                             "Error: Wrong split algorithm. %s" % mergeSubscription["split_algo"])
        unmergedOutputs = []
        for alcaProd in testArguments["AlcaSkims"]:
            unmergedOutputs.append("ALCARECOStream%s" % alcaProd)
        for unmergedOutput in unmergedOutputs:
            unmergedAlcaSkim = Fileset(name = "/TestWorkload/Reco/AlcaSkim/unmerged-%s" % unmergedOutput)
            unmergedAlcaSkim.loadData()
            alcaSkimMergeWorkflow = Workflow(name = "TestWorkload",
                                             task = "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s" % unmergedOutput)
            alcaSkimMergeWorkflow.load()
            mergeSubscription = Subscription(fileset = unmergedAlcaSkim, workflow = alcaSkimMergeWorkflow)
            mergeSubscription.loadData()

            self.assertEqual(mergeSubscription["type"], "Merge",
                             "Error: Wrong subscription type.")
            self.assertEqual(mergeSubscription["split_algo"], "WMBSMergeBySize",
                             "Error: Wrong split algorithm. %s" % mergeSubscription["split_algo"])

        unmergedOutputs = ["fakeSkimOut1", "fakeSkimOut2", "fakeSkimOut3",
                           "fakeSkimOut4", "fakeSkimOut5"]
        for unmergedOutput in unmergedOutputs:
            unmergedPromptSkim = Fileset(name = "/TestWorkload/Reco/RecoMergewrite_RECO/TestSkim1/unmerged-%s" % unmergedOutput)
            unmergedPromptSkim.loadData()
            promptSkimMergeWorkflow = Workflow(name = "TestWorkload",
                                               task = "/TestWorkload/Reco/RecoMergewrite_RECO/TestSkim1/TestSkim1Merge%s" % unmergedOutput)
            promptSkimMergeWorkflow.load()
            mergeSubscription = Subscription(fileset = unmergedPromptSkim, workflow = promptSkimMergeWorkflow)
            mergeSubscription.loadData()

            self.assertEqual(mergeSubscription["type"], "Merge",
                             "Error: Wrong subscription type.")
            self.assertEqual(mergeSubscription["split_algo"], "ParentlessMergeBySize",
                             "Error: Wrong split algorithm. %s" % mergeSubscription["split_algo"])

        goldenOutputMods = ["write_RECO", "write_AOD", "write_DQM", "write_ALCARECO"]
        for goldenOutputMod in goldenOutputMods:
            unmergedFileset = Fileset(name = "/TestWorkload/Reco/unmerged-%s" % goldenOutputMod)
            unmergedFileset.loadData()
            cleanupWorkflow = Workflow(name = "TestWorkload",
                                       task = "/TestWorkload/Reco/RecoCleanupUnmerged%s" % goldenOutputMod)
            cleanupWorkflow.load()
            cleanupSubscription = Subscription(fileset = unmergedFileset, workflow = cleanupWorkflow)
            cleanupSubscription.loadData()

            self.assertEqual(cleanupSubscription["type"], "Cleanup",
                             "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong subscription type.")

        goldenOutputMods = []
        for alcaProd in testArguments["AlcaSkims"]:
            goldenOutputMods.append("ALCARECOStream%s" % alcaProd)
        for goldenOutputMod in goldenOutputMods:
            unmergedFileset = Fileset(name = "/TestWorkload/Reco/AlcaSkim/unmerged-%s" % goldenOutputMod)
            unmergedFileset.loadData()
            cleanupWorkflow = Workflow(name = "TestWorkload",
                                       task = "/TestWorkload/Reco/AlcaSkim/AlcaSkimCleanupUnmerged%s" %goldenOutputMod)
            cleanupWorkflow.load()
            cleanupSubscription = Subscription(fileset = unmergedFileset, workflow = cleanupWorkflow)
            cleanupSubscription.loadData()

            self.assertEqual(cleanupSubscription["type"], "Cleanup",
                             "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong subscription type.")

        goldenOutputMods = ["fakeSkimOut1", "fakeSkimOut2", "fakeSkimOut3",
                           "fakeSkimOut4", "fakeSkimOut5"]
        for goldenOutputMod in goldenOutputMods:
            unmergedFileset = Fileset(name = "/TestWorkload/Reco/RecoMergewrite_RECO/TestSkim1/unmerged-%s" % unmergedOutput)
            unmergedFileset.loadData()
            cleanupWorkflow = Workflow(name = "TestWorkload",
                                               task = "/TestWorkload/Reco/RecoMergewrite_RECO/TestSkim1/TestSkim1CleanupUnmerged%s" % unmergedOutput)
            cleanupWorkflow.load()
            cleanupSubscription = Subscription(fileset = unmergedFileset, workflow = cleanupWorkflow)
            cleanupSubscription.loadData()

            self.assertEqual(cleanupSubscription["type"], "Cleanup",
                             "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong split algorithm. %s" % cleanupSubscription["split_algo"])

        recoLogCollect = Fileset(name = "/TestWorkload/Reco/unmerged-logArchive")
        recoLogCollect.loadData()
        recoLogCollectWorkflow = Workflow(name = "TestWorkload",
                                          task = "/TestWorkload/Reco/LogCollect")
        recoLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset = recoLogCollect, workflow = recoLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algorithm.")

        alcaSkimLogCollect = Fileset(name = "/TestWorkload/Reco/AlcaSkim/unmerged-logArchive")
        alcaSkimLogCollect.loadData()
        alcaSkimLogCollectWorkflow = Workflow(name = "TestWorkload",
                                                task = "/TestWorkload/Reco/AlcaSkim/AlcaSkimLogCollect")
        alcaSkimLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset = alcaSkimLogCollect, workflow = alcaSkimLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algorithm.")

        promptSkimLogCollect = Fileset(name = "/TestWorkload/Reco/RecoMergewrite_RECO/TestSkim1/unmerged-logArchive")
        promptSkimLogCollect.loadData()
        promptSkimLogCollectWorkflow = Workflow(name = "TestWorkload",
                                                task = "/TestWorkload/Reco/RecoMergewrite_RECO/TestSkim1/TestSkim1LogCollect")
        promptSkimLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset = promptSkimLogCollect, workflow = promptSkimLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algorithm.")

        goldenOutputMods = ["write_RECO", "write_AOD", "write_DQM"]
        for goldenOutputMod in goldenOutputMods:
            recoMergeLogCollect = Fileset(name = "/TestWorkload/Reco/RecoMerge%s/merged-logArchive" % goldenOutputMod)
            recoMergeLogCollect.loadData()
            recoMergeLogCollectWorkflow = Workflow(name = "TestWorkload",
                                       task = "/TestWorkload/Reco/RecoMerge%s/Reco%sMergeLogCollect" % (goldenOutputMod, goldenOutputMod))
            recoMergeLogCollectWorkflow.load()
            logCollectSubscription = Subscription(fileset = recoMergeLogCollect, workflow = recoMergeLogCollectWorkflow)
            logCollectSubscription.loadData()

            self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algorithm.")

        goldenOutputMods = []
        for alcaProd in testArguments["AlcaSkims"]:
            goldenOutputMods.append("ALCARECOStream%s" % alcaProd)
        for goldenOutputMod in goldenOutputMods:
            alcaSkimLogCollect = Fileset(name = "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-logArchive" % goldenOutputMod)
            alcaSkimLogCollect.loadData()
            alcaSkimLogCollectWorkflow = Workflow(name = "TestWorkload",
                                       task = "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/AlcaSkim%sMergeLogCollect" % (goldenOutputMod, goldenOutputMod))
            alcaSkimLogCollectWorkflow.load()
            logCollectSubscription = Subscription(fileset = alcaSkimLogCollect, workflow = alcaSkimLogCollectWorkflow)
            logCollectSubscription.loadData()

            self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algorithm.")

        goldenOutputMods = ["fakeSkimOut1", "fakeSkimOut2", "fakeSkimOut3",
                           "fakeSkimOut4", "fakeSkimOut5"]
        for goldenOutputMod in goldenOutputMods:
            promptSkimMergeLogCollect = Fileset(name = "/TestWorkload/Reco/RecoMergewrite_RECO/TestSkim1/TestSkim1Merge%s/merged-logArchive" % goldenOutputMod)
            promptSkimMergeLogCollect.loadData()
            promptSkimMergeLogCollectWorkflow = Workflow(name = "TestWorkload",
                                                    task = "/TestWorkload/Reco/RecoMergewrite_RECO/TestSkim1/TestSkim1Merge%s/TestSkim1%sMergeLogCollect" % (goldenOutputMod, goldenOutputMod))
            promptSkimMergeLogCollectWorkflow.load()
            logCollectSubscription = Subscription(fileset = promptSkimMergeLogCollect, workflow = promptSkimMergeLogCollectWorkflow)
            logCollectSubscription.loadData()

            self.assertEqual(logCollectSub["type"], "LogCollect",
                             "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                             "Error: Wrong split algorithm.")

        return

if __name__ == '__main__':
    unittest.main()
