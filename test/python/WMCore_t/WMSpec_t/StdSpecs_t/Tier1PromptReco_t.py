#!/usr/bin/env python
"""
_Tier1PromptReco_t_

Unit tests for the new Tier1 PromptReconstruction workflow.
"""

import unittest
import os
import threading

from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow

from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMCore.WMSpec.StdSpecs.Tier1PromptReco import getTestArguments, tier1promptrecoWorkload

from WMCore.Configuration import ConfigSection

from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.Database.CMSCouch import CouchServer

from nose.plugins.attrib import attr

class Tier1PromptRecoTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Initialize the database and couch.
        """
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("tier1promptreco_t", "ConfigCache")
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("tier1promptreco_t")
        return

    def tearDown(self):
        """
        _tearDown_

        Clear out the database.
        """
        self.testInit.tearDownCouch()
        self.testInit.clearDatabase()
        return

    def setupPromptSkimConfigObjects(self):
        """
        _setupPromptSkimConfigObjects_

        Creates two config objects for testing
        of the skim functionality
        """
        self.promptSkims = []
        promptSkim = ConfigSection(name="Tier1Skim")
        promptSkim.SkimName = "TestSkim1"
        promptSkim.DataTier = "RECO"
        promptSkim.TwoFileRead = False
        promptSkim.ProcessingVersion = "PromptSkim-v1"
        promptSkim.ConfigURL = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/DataOps/python/prescaleskimmer.py?revision=1.1"
        self.promptSkims.append(promptSkim)

        promptSkim = ConfigSection(name="Tier1Skim")
        promptSkim.SkimName = "TestSkim2"
        promptSkim.DataTier = "AOD"
        promptSkim.TwoFileRead = False
        promptSkim.ProcessingVersion = "PromptSkim-v2"
        promptSkim.ConfigURL = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/COMP/T0/operations/skimConfigs/skim_LogError.py?revision=1.1"
        self.promptSkims.append(promptSkim)

    def commonTier1PromptRecoTest(self, workload, arguments):
        """
        _commonTier1PromptRecoTest_

        Test the basic skeleton of a tier1PromptReco workflow with
        the default config.DP with all output modules enabled
        """

        testWMBSHelper = WMBSHelper(workload, "Reco", "SomeBlock")
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper.createSubscription(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        recoWorkflow = Workflow(name = "TestWorkload",
                                task = "/TestWorkload/Reco")
        recoWorkflow.load()
        self.assertEqual(len(recoWorkflow.outputMap.keys()), len(arguments["WriteTiers"]) + 1,
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
        self.assertEqual(len(alcaSkimWorkflow.outputMap.keys()), len(arguments["AlcaSkims"]) + 1,
                        "Error: Wrong number of WF outputs in the AlcaSkim WF.")

        goldenOutputMods = []
        for alcaProd in arguments["AlcaSkims"]:
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
        for alcaProd in arguments["AlcaSkims"]:
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
        for alcaProd in arguments["AlcaSkims"]:
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
        for alcaProd in arguments["AlcaSkims"]:
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

        return

    def testTier1PromptRecoA(self):
        """
        _testT1PromptRecoA_

        Create a T1 Prompt Reconstruction workflow with EventBased algo
        and verify it installs into WMBS correctly.
        """
        testArguments = getTestArguments()

        testWorkload = tier1promptrecoWorkload("TestWorkload", testArguments)
        testWorkload.setSpecUrl("somespec")
        testWorkload.setOwnerDetails("dballest@fnal.gov", "T0")

        self.commonTier1PromptRecoTest(testWorkload, testArguments)

        recoWorkflow = Workflow(name = "TestWorkload",
                                task = "/TestWorkload/Reco")
        recoWorkflow.load()
        topLevelFileset = Fileset(name = "TestWorkload-Reco-SomeBlock")
        topLevelFileset.loadData()

        recoSubscription = Subscription(fileset = topLevelFileset, workflow = recoWorkflow)
        recoSubscription.loadData()

        self.assertEqual(recoSubscription["type"], "Processing",
                         "Error: Wrong subscription type.")
        self.assertEqual(recoSubscription["split_algo"], "EventBased",
                         "Error: Wrong split algorithm. %s" % recoSubscription["split_algo"])

        alcaSkimWorkflow = Workflow(name = "TestWorkload",
                                    task = "/TestWorkload/Reco/AlcaSkim")
        alcaSkimWorkflow.load()

        alcaRecoFileset = Fileset(name = "/TestWorkload/Reco/unmerged-write_ALCARECO")
        alcaRecoFileset.loadData()

        alcaSkimSubscription = Subscription(fileset = alcaRecoFileset, workflow = alcaSkimWorkflow)
        alcaSkimSubscription.loadData()

        self.assertEqual(alcaSkimSubscription["type"], "Processing",
                         "Error: Wrong subscription type.")
        self.assertEqual(alcaSkimSubscription["split_algo"], "WMBSMergeBySize",
                         "Error: Wrong split algorithm. %s" % alcaSkimSubscription["split_algo"])

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

    def testTier1PromptRecoB(self):
        """
        _testT1PromptRecoB_

        Create a T1 Prompt Reconstruction workflow with FileBased algo
        and verify it installs into WMBS.
        """
        testArguments = getTestArguments()
        testArguments["StdJobSplitAlgo"] = "FileBased"
        testArguments["StdJobSplitArgs"] = {"files_per_job": 1,
                                            "include_parents": False}

        testWorkload = tier1promptrecoWorkload("TestWorkload", testArguments)
        testWorkload.setSpecUrl("somespec")
        testWorkload.setOwnerDetails("dballest@fnal.gov", "T0")

        self.commonTier1PromptRecoTest(testWorkload, testArguments)

        recoWorkflow = Workflow(name = "TestWorkload",
                                task = "/TestWorkload/Reco")
        recoWorkflow.load()
        topLevelFileset = Fileset(name = "TestWorkload-Reco-SomeBlock")
        topLevelFileset.loadData()

        recoSubscription = Subscription(fileset = topLevelFileset, workflow = recoWorkflow)
        recoSubscription.loadData()

        self.assertEqual(recoSubscription["type"], "Processing",
                         "Error: Wrong subscription type.")
        self.assertEqual(recoSubscription["split_algo"], "FileBased",
                         "Error: Wrong split algorithm. %s" % recoSubscription["split_algo"])

        alcaSkimWorkflow = Workflow(name = "TestWorkload",
                                    task = "/TestWorkload/Reco/AlcaSkim")
        alcaSkimWorkflow.load()

        alcaRecoFileset = Fileset(name = "/TestWorkload/Reco/unmerged-write_ALCARECO")
        alcaRecoFileset.loadData()

        alcaSkimSubscription = Subscription(fileset = alcaRecoFileset, workflow = alcaSkimWorkflow)
        alcaSkimSubscription.loadData()

        self.assertEqual(alcaSkimSubscription["type"], "Processing",
                         "Error: Wrong subscription type.")
        self.assertEqual(alcaSkimSubscription["split_algo"], "ParentlessMergeBySize",
                         "Error: Wrong split algorithm. %s" % alcaSkimSubscription["split_algo"])

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
            self.assertEqual(mergeSubscription["split_algo"], "ParentlessMergeBySize",
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
            self.assertEqual(mergeSubscription["split_algo"], "ParentlessMergeBySize",
                             "Error: Wrong split algorithm. %s" % mergeSubscription["split_algo"])

    @attr("integration")
    def testTier1PromptRecoCustomConfig(self):
        """
        _testTier1PromptRecoCustomConfig_

        Creates a Tier1 PromptReco workflow with a custom cmssw
        configuration file, which has ALCARECO as one of the outputs
        but no AlcaSkims have been defined. Besides the usual checks,
        this means that the ALCARECO output must be merged.
        """
        self.setupPromptSkimConfigObjects()
        testArguments = getTestArguments()
        testArguments["ConfigURL"] = 'http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/COMP/T0/operations/skimConfigs/fakeRecoConfig.py?revision=1.1'
        testArguments["CouchURL"] = os.environ["COUCHURL"]
        testArguments["AlcaSkims"] = []

        testWorkload = tier1promptrecoWorkload("TestWorkload", testArguments)
        testWorkload.setSpecUrl("somespec")
        testWorkload.setOwnerDetails("dballest@fnal.gov", "T0")

        testWMBSHelper = WMBSHelper(testWorkload, "Reco", "SomeBlock")
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper.createSubscription(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        recoWorkflow = Workflow(name = "TestWorkload",
                                task = "/TestWorkload/Reco")
        recoWorkflow.load()
        self.assertEqual(len(recoWorkflow.outputMap.keys()), 4,
                         "Error: Wrong number of WF outputs in the Reco WF.")

        goldenOutputMods = ["recoOutput", "alcaRecoOutput", "aodOutput"]
        for goldenOutputMod in goldenOutputMods:
            mergedOutput = recoWorkflow.outputMap[goldenOutputMod][0]["merged_output_fileset"]
            unmergedOutput = recoWorkflow.outputMap[goldenOutputMod][0]["output_fileset"]
            mergedOutput.loadData()
            unmergedOutput.loadData()

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

        goldenOutputMods = ["recoOutput", "alcaRecoOutput", "aodOutput"]
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

        topLevelFileset = Fileset(name = "TestWorkload-Reco-SomeBlock")
        topLevelFileset.loadData()

        recoSubscription = Subscription(fileset = topLevelFileset, workflow = recoWorkflow)
        recoSubscription.loadData()

        self.assertEqual(recoSubscription["type"], "Processing",
                         "Error: Wrong subscription type.")
        self.assertEqual(recoSubscription["split_algo"], "EventBased",
                         "Error: Wrong split algorithm. %s" % recoSubscription["split_algo"])

        unmergedOutputs = ["recoOutput", "alcaRecoOutput", "aodOutput"]
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

        goldenOutputMods = ["recoOutput", "alcaRecoOutput", "aodOutput"]
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

        goldenOutputMods = ["recoOutput", "alcaRecoOutput", "aodOutput"]
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

        return


    @attr("integration")
    def testTier1PromptRecoWithSkims(self):
        """
        _testTier1PromptRecoWithSkims_

        Create a T1 Prompt Reconstruction workflow with PromptSkims
        and verify it installs into WMBS correctly.
        """
        self.setupPromptSkimConfigObjects()
        testArguments = getTestArguments()
        testArguments["PromptSkims"] = self.promptSkims
        testArguments["CouchURL"] = os.environ["COUCHURL"]

        testWorkload = tier1promptrecoWorkload("TestWorkload", testArguments)
        testWorkload.setSpecUrl("somespec")
        testWorkload.setOwnerDetails("dballest@fnal.gov", "T0")

        self.commonTier1PromptRecoTest(testWorkload, testArguments)

        promptSkimWorkflow1 = Workflow(name="TestWorkload",
                                      task="/TestWorkload/Reco/RecoMergewrite_RECO/TestSkim1")
        promptSkimWorkflow1.load()

        self.assertEqual(len(promptSkimWorkflow1.outputMap.keys()), 6,
                         "Error: Wrong number of WF outputs.")

        goldenOutputMods = ["fakeSkimOut1", "fakeSkimOut2", "fakeSkimOut3",
                            "fakeSkimOut4", "fakeSkimOut5"]
        for goldenOutputMod in goldenOutputMods:
            mergedOutput = promptSkimWorkflow1.outputMap[goldenOutputMod][0]["merged_output_fileset"]
            unmergedOutput = promptSkimWorkflow1.outputMap[goldenOutputMod][0]["output_fileset"]
            mergedOutput.loadData()
            unmergedOutput.loadData()

            self.assertEqual(mergedOutput.name, "/TestWorkload/Reco/RecoMergewrite_RECO/TestSkim1/TestSkim1Merge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/Reco/RecoMergewrite_RECO/TestSkim1/unmerged-%s" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong: %s" % unmergedOutput.name)

        logArchOutput = promptSkimWorkflow1.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = promptSkimWorkflow1.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name, "/TestWorkload/Reco/RecoMergewrite_RECO/TestSkim1/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Reco/RecoMergewrite_RECO/TestSkim1/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        promptSkimWorkflow2 = Workflow(name = "TestWorkload",
                                       task = "/TestWorkload/Reco/RecoMergewrite_AOD/TestSkim2")
        promptSkimWorkflow2.load()

        self.assertEqual(len(promptSkimWorkflow2.outputMap.keys()), 2,
                         "Error: Wrong number of WF outputs.")

        goldenOutputMods = ["SKIMStreamLogError"]
        for goldenOutputMod in goldenOutputMods:
            mergedOutput = promptSkimWorkflow2.outputMap[goldenOutputMod][0]["merged_output_fileset"]
            unmergedOutput = promptSkimWorkflow2.outputMap[goldenOutputMod][0]["output_fileset"]
            mergedOutput.loadData()
            unmergedOutput.loadData()

            self.assertEqual(mergedOutput.name, "/TestWorkload/Reco/RecoMergewrite_AOD/TestSkim2/TestSkim2Merge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/Reco/RecoMergewrite_AOD/TestSkim2/unmerged-%s" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong: %s" % unmergedOutput.name)

        logArchOutput = promptSkimWorkflow2.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = promptSkimWorkflow2.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name, "/TestWorkload/Reco/RecoMergewrite_AOD/TestSkim2/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Reco/RecoMergewrite_AOD/TestSkim2/unmerged-logArchive",
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

        goldenOutputMods = ["SKIMStreamLogError"]
        for goldenOutputMod in goldenOutputMods:
            mergeWorkflow = Workflow(name = "TestWorkload",
                                     task = "/TestWorkload/Reco/RecoMergewrite_AOD/TestSkim2/TestSkim2Merge%s" % goldenOutputMod)
            mergeWorkflow.load()

            self.assertEqual(len(mergeWorkflow.outputMap.keys()), 2,
                             "Error: Wrong number of WF outputs %d." % len(mergeWorkflow.outputMap.keys()))

            mergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["merged_output_fileset"]
            unmergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["output_fileset"]

            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()

            self.assertEqual(mergedMergeOutput.name, "/TestWorkload/Reco/RecoMergewrite_AOD/TestSkim2/TestSkim2Merge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong.")
            self.assertEqual(unmergedMergeOutput.name, "/TestWorkload/Reco/RecoMergewrite_AOD/TestSkim2/TestSkim2Merge%s/merged-Merged" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong.")

            logArchOutput = mergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = mergeWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()

            self.assertEqual(logArchOutput.name, "/TestWorkload/Reco/RecoMergewrite_AOD/TestSkim2/TestSkim2Merge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
            self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Reco/RecoMergewrite_AOD/TestSkim2/TestSkim2Merge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")


        mergedRecoFileset = Fileset(name = "/TestWorkload/Reco/RecoMergewrite_RECO/merged-Merged")
        mergedRecoFileset.loadData()

        promptSkim1Subscription = Subscription(fileset = mergedRecoFileset, workflow = promptSkimWorkflow1)
        promptSkim1Subscription.loadData()

        self.assertEqual(promptSkim1Subscription["type"], "Skim",
                         "Error: Wrong subscription type.")
        self.assertEqual(promptSkim1Subscription["split_algo"], "FileBased",
                         "Error: Wrong split algorithm. %s" % promptSkim1Subscription["split_algo"])

        mergedAodFileset = Fileset(name = "/TestWorkload/Reco/RecoMergewrite_AOD/merged-Merged")
        mergedAodFileset.loadData()

        promptSkim2Subscription = Subscription(fileset = mergedAodFileset, workflow = promptSkimWorkflow2)
        promptSkim2Subscription.loadData()

        self.assertEqual(promptSkim2Subscription["type"], "Skim",
                         "Error: Wrong subscription type.")
        self.assertEqual(promptSkim2Subscription["split_algo"], "FileBased",
                         "Error: Wrong split algorithm. %s" % promptSkim1Subscription["split_algo"])

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

        unmergedOutputs = ["SKIMStreamLogError"]
        for unmergedOutput in unmergedOutputs:
            unmergedPromptSkim = Fileset(name = "/TestWorkload/Reco/RecoMergewrite_AOD/TestSkim2/unmerged-%s" % unmergedOutput)
            unmergedPromptSkim.loadData()
            promptSkimMergeWorkflow = Workflow(name = "TestWorkload",
                                               task = "/TestWorkload/Reco/RecoMergewrite_AOD/TestSkim2/TestSkim2Merge%s" % unmergedOutput)
            promptSkimMergeWorkflow.load()
            mergeSubscription = Subscription(fileset = unmergedPromptSkim, workflow = promptSkimMergeWorkflow)
            mergeSubscription.loadData()

            self.assertEqual(mergeSubscription["type"], "Merge",
                             "Error: Wrong subscription type.")
            self.assertEqual(mergeSubscription["split_algo"], "ParentlessMergeBySize",
                             "Error: Wrong split algorithm. %s" % mergeSubscription["split_algo"])

        goldenOutputMods = ["fakeSkimOut1", "fakeSkimOut2", "fakeSkimOut3",
                           "fakeSkimOut4", "fakeSkimOut5"]
        for goldenOutputMod in goldenOutputMods:
            unmergedFileset = Fileset(name = "/TestWorkload/Reco/RecoMergewrite_RECO/TestSkim1/unmerged-%s" % goldenOutputMod)
            unmergedFileset.loadData()
            cleanupWorkflow = Workflow(name = "TestWorkload",
                                               task = "/TestWorkload/Reco/RecoMergewrite_RECO/TestSkim1/TestSkim1CleanupUnmerged%s" % goldenOutputMod)
            cleanupWorkflow.load()
            cleanupSubscription = Subscription(fileset = unmergedFileset, workflow = cleanupWorkflow)
            cleanupSubscription.loadData()

            self.assertEqual(cleanupSubscription["type"], "Cleanup",
                             "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong split algorithm. %s" % cleanupSubscription["split_algo"])

        goldenOutputMods = ["SKIMStreamLogError"]
        for goldenOutputMod in goldenOutputMods:
            unmergedFileset = Fileset(name = "/TestWorkload/Reco/RecoMergewrite_AOD/TestSkim2/unmerged-%s" % goldenOutputMod)
            unmergedFileset.loadData()
            cleanupWorkflow = Workflow(name = "TestWorkload",
                                               task = "/TestWorkload/Reco/RecoMergewrite_AOD/TestSkim2/TestSkim2CleanupUnmerged%s" % goldenOutputMod)
            cleanupWorkflow.load()
            cleanupSubscription = Subscription(fileset = unmergedFileset, workflow = cleanupWorkflow)
            cleanupSubscription.loadData()

            self.assertEqual(cleanupSubscription["type"], "Cleanup",
                             "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong split algorithm. %s" % cleanupSubscription["split_algo"])

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

        promptSkimLogCollect = Fileset(name = "/TestWorkload/Reco/RecoMergewrite_AOD/TestSkim2/unmerged-logArchive")
        promptSkimLogCollect.loadData()
        promptSkimLogCollectWorkflow = Workflow(name = "TestWorkload",
                                                task = "/TestWorkload/Reco/RecoMergewrite_AOD/TestSkim2/TestSkim2LogCollect")
        promptSkimLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset = promptSkimLogCollect, workflow = promptSkimLogCollectWorkflow)
        logCollectSub.loadData()

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

        goldenOutputMods = ["SKIMStreamLogError"]
        for goldenOutputMod in goldenOutputMods:
            promptSkimMergeLogCollect = Fileset(name = "/TestWorkload/Reco/RecoMergewrite_AOD/TestSkim2/TestSkim2Merge%s/merged-logArchive" % goldenOutputMod)
            promptSkimMergeLogCollect.loadData()
            promptSkimMergeLogCollectWorkflow = Workflow(name = "TestWorkload",
                                                    task = "/TestWorkload/Reco/RecoMergewrite_AOD/TestSkim2/TestSkim2Merge%s/TestSkim2%sMergeLogCollect" % (goldenOutputMod, goldenOutputMod))
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
