#!/usr/bin/env python
"""
_PromptReco_t_

Unit tests for the new Tier1 PromptReconstruction workflow.
"""
from __future__ import print_function

import os
import threading
import unittest
from pprint import pformat

from WMCore.Configuration import ConfigSection
from WMCore.DAOFactory import DAOFactory
from WMCore.Database.CMSCouch import CouchServer
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMSpec.StdSpecs.PromptReco import PromptRecoWorkloadFactory
from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMQuality.TestInitCouchApp import TestInitCouchApp


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
        self.testInit.setSchema(customModules=["WMCore.WMBS"],
                                useDefault=False)
        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("promptreco_t")
        self.testDir = self.testInit.generateWorkDir()

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)
        self.listTasksByWorkflow = self.daoFactory(classname="Workflow.LoadFromName")
        self.listFilesets = self.daoFactory(classname="Fileset.List")
        self.listSubsMapping = self.daoFactory(classname="Subscriptions.ListSubsAndFilesetsFromWorkflow")

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

        testWMBSHelper = WMBSHelper(testWorkload, "Reco", "SomeBlock", cachepath=self.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        recoWorkflow = Workflow(name="TestWorkload",
                                task="/TestWorkload/Reco")
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

        alcaSkimWorkflow = Workflow(name="TestWorkload",
                                    task="/TestWorkload/Reco/AlcaSkim")
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
            self.assertEqual(mergedOutput.name,
                             "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-Merged" % goldenOutputMod,
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

        dqmWorkflow = Workflow(name="TestWorkload",
                               task="/TestWorkload/Reco/RecoMergewrite_DQM/RecoMergewrite_DQMEndOfRunDQMHarvestMerged")
        dqmWorkflow.load()

        logArchOutput = dqmWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = dqmWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name,
                         "/TestWorkload/Reco/RecoMergewrite_DQM/RecoMergewrite_DQMEndOfRunDQMHarvestMerged/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name,
                         "/TestWorkload/Reco/RecoMergewrite_DQM/RecoMergewrite_DQMEndOfRunDQMHarvestMerged/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        goldenOutputMods = ["write_RECO", "write_AOD", "write_DQM"]
        for goldenOutputMod in goldenOutputMods:
            mergeWorkflow = Workflow(name="TestWorkload",
                                     task="/TestWorkload/Reco/RecoMerge%s" % goldenOutputMod)
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
            self.assertEqual(unmergedLogArchOutput.name,
                             "/TestWorkload/Reco/RecoMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")

        goldenOutputMods = []
        for alcaProd in testArguments["AlcaSkims"]:
            goldenOutputMods.append("ALCARECOStream%s" % alcaProd)

        for goldenOutputMod in goldenOutputMods:
            mergeWorkflow = Workflow(name="TestWorkload",
                                     task="/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s" % goldenOutputMod)
            mergeWorkflow.load()

            self.assertEqual(len(mergeWorkflow.outputMap.keys()), 2,
                             "Error: Wrong number of WF outputs %d." % len(mergeWorkflow.outputMap.keys()))

            mergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["merged_output_fileset"]
            unmergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["output_fileset"]

            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()

            self.assertEqual(mergedMergeOutput.name,
                             "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong.")
            self.assertEqual(unmergedMergeOutput.name,
                             "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong.")

            logArchOutput = mergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = mergeWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()

            self.assertEqual(logArchOutput.name,
                             "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
            self.assertEqual(unmergedLogArchOutput.name,
                             "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")

        topLevelFileset = Fileset(name="TestWorkload-Reco-SomeBlock")
        topLevelFileset.loadData()

        recoSubscription = Subscription(fileset=topLevelFileset, workflow=recoWorkflow)
        recoSubscription.loadData()

        self.assertEqual(recoSubscription["type"], "Processing",
                         "Error: Wrong subscription type.")
        self.assertEqual(recoSubscription["split_algo"], "EventBased",
                         "Error: Wrong split algorithm. %s" % recoSubscription["split_algo"])

        alcaRecoFileset = Fileset(name="/TestWorkload/Reco/unmerged-write_ALCARECO")
        alcaRecoFileset.loadData()

        alcaSkimSubscription = Subscription(fileset=alcaRecoFileset, workflow=alcaSkimWorkflow)
        alcaSkimSubscription.loadData()

        self.assertEqual(alcaSkimSubscription["type"], "Processing",
                         "Error: Wrong subscription type.")
        self.assertEqual(alcaSkimSubscription["split_algo"], "WMBSMergeBySize",
                         "Error: Wrong split algorithm. %s" % alcaSkimSubscription["split_algo"])

        mergedDQMFileset = Fileset(name="/TestWorkload/Reco/RecoMergewrite_DQM/merged-Merged")
        mergedDQMFileset.loadData()

        dqmSubscription = Subscription(fileset=mergedDQMFileset, workflow=dqmWorkflow)
        dqmSubscription.loadData()

        self.assertEqual(dqmSubscription["type"], "Harvesting",
                         "Error: Wrong subscription type.")
        self.assertEqual(dqmSubscription["split_algo"], "Harvest",
                         "Error: Wrong split algo.")

        unmergedOutputs = ["write_RECO", "write_AOD", "write_DQM"]
        for unmergedOutput in unmergedOutputs:
            unmergedDataTier = Fileset(name="/TestWorkload/Reco/unmerged-%s" % unmergedOutput)
            unmergedDataTier.loadData()
            dataTierMergeWorkflow = Workflow(name="TestWorkload",
                                             task="/TestWorkload/Reco/RecoMerge%s" % unmergedOutput)
            dataTierMergeWorkflow.load()
            mergeSubscription = Subscription(fileset=unmergedDataTier, workflow=dataTierMergeWorkflow)
            mergeSubscription.loadData()

            self.assertEqual(mergeSubscription["type"], "Merge",
                             "Error: Wrong subscription type.")
            self.assertEqual(mergeSubscription["split_algo"], "WMBSMergeBySize",
                             "Error: Wrong split algorithm. %s" % mergeSubscription["split_algo"])
        unmergedOutputs = []
        for alcaProd in testArguments["AlcaSkims"]:
            unmergedOutputs.append("ALCARECOStream%s" % alcaProd)
        for unmergedOutput in unmergedOutputs:
            unmergedAlcaSkim = Fileset(name="/TestWorkload/Reco/AlcaSkim/unmerged-%s" % unmergedOutput)
            unmergedAlcaSkim.loadData()
            alcaSkimMergeWorkflow = Workflow(name="TestWorkload",
                                             task="/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s" % unmergedOutput)
            alcaSkimMergeWorkflow.load()
            mergeSubscription = Subscription(fileset=unmergedAlcaSkim, workflow=alcaSkimMergeWorkflow)
            mergeSubscription.loadData()

            self.assertEqual(mergeSubscription["type"], "Merge",
                             "Error: Wrong subscription type.")
            self.assertEqual(mergeSubscription["split_algo"], "WMBSMergeBySize",
                             "Error: Wrong split algorithm. %s" % mergeSubscription["split_algo"])

        goldenOutputMods = ["write_RECO", "write_AOD", "write_DQM", "write_ALCARECO"]
        for goldenOutputMod in goldenOutputMods:
            unmergedFileset = Fileset(name="/TestWorkload/Reco/unmerged-%s" % goldenOutputMod)
            unmergedFileset.loadData()
            cleanupWorkflow = Workflow(name="TestWorkload",
                                       task="/TestWorkload/Reco/RecoCleanupUnmerged%s" % goldenOutputMod)
            cleanupWorkflow.load()
            cleanupSubscription = Subscription(fileset=unmergedFileset, workflow=cleanupWorkflow)
            cleanupSubscription.loadData()

            self.assertEqual(cleanupSubscription["type"], "Cleanup",
                             "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong subscription type.")

        goldenOutputMods = []
        for alcaProd in testArguments["AlcaSkims"]:
            goldenOutputMods.append("ALCARECOStream%s" % alcaProd)
        for goldenOutputMod in goldenOutputMods:
            unmergedFileset = Fileset(name="/TestWorkload/Reco/AlcaSkim/unmerged-%s" % goldenOutputMod)
            unmergedFileset.loadData()
            cleanupWorkflow = Workflow(name="TestWorkload",
                                       task="/TestWorkload/Reco/AlcaSkim/AlcaSkimCleanupUnmerged%s" % goldenOutputMod)
            cleanupWorkflow.load()
            cleanupSubscription = Subscription(fileset=unmergedFileset, workflow=cleanupWorkflow)
            cleanupSubscription.loadData()

            self.assertEqual(cleanupSubscription["type"], "Cleanup",
                             "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong subscription type.")

        recoLogCollect = Fileset(name="/TestWorkload/Reco/unmerged-logArchive")
        recoLogCollect.loadData()
        recoLogCollectWorkflow = Workflow(name="TestWorkload",
                                          task="/TestWorkload/Reco/LogCollect")
        recoLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset=recoLogCollect, workflow=recoLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algorithm.")

        alcaSkimLogCollect = Fileset(name="/TestWorkload/Reco/AlcaSkim/unmerged-logArchive")
        alcaSkimLogCollect.loadData()
        alcaSkimLogCollectWorkflow = Workflow(name="TestWorkload",
                                              task="/TestWorkload/Reco/AlcaSkim/AlcaSkimLogCollect")
        alcaSkimLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset=alcaSkimLogCollect, workflow=alcaSkimLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algorithm.")

        goldenOutputMods = ["write_RECO", "write_AOD", "write_DQM"]
        for goldenOutputMod in goldenOutputMods:
            recoMergeLogCollect = Fileset(name="/TestWorkload/Reco/RecoMerge%s/merged-logArchive" % goldenOutputMod)
            recoMergeLogCollect.loadData()
            recoMergeLogCollectWorkflow = Workflow(name="TestWorkload",
                                                   task="/TestWorkload/Reco/RecoMerge%s/Reco%sMergeLogCollect" % (
                                                       goldenOutputMod, goldenOutputMod))
            recoMergeLogCollectWorkflow.load()
            logCollectSubscription = Subscription(fileset=recoMergeLogCollect, workflow=recoMergeLogCollectWorkflow)
            logCollectSubscription.loadData()

            self.assertEqual(logCollectSub["type"], "LogCollect",
                             "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                             "Error: Wrong split algorithm.")

        goldenOutputMods = []
        for alcaProd in testArguments["AlcaSkims"]:
            goldenOutputMods.append("ALCARECOStream%s" % alcaProd)
        for goldenOutputMod in goldenOutputMods:
            alcaSkimLogCollect = Fileset(
                name="/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-logArchive" % goldenOutputMod)
            alcaSkimLogCollect.loadData()
            alcaSkimLogCollectWorkflow = Workflow(name="TestWorkload",
                                                  task="/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/AlcaSkim%sMergeLogCollect" % (
                                                      goldenOutputMod, goldenOutputMod))
            alcaSkimLogCollectWorkflow.load()
            logCollectSubscription = Subscription(fileset=alcaSkimLogCollect, workflow=alcaSkimLogCollectWorkflow)
            logCollectSubscription.loadData()

            self.assertEqual(logCollectSub["type"], "LogCollect",
                             "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                             "Error: Wrong split algorithm.")

        dqmHarvestLogCollect = Fileset(
            name="/TestWorkload/Reco/RecoMergewrite_DQM/RecoMergewrite_DQMEndOfRunDQMHarvestMerged/unmerged-logArchive")
        dqmHarvestLogCollect.loadData()
        dqmHarvestLogCollectWorkflow = Workflow(name="TestWorkload",
                                                task="/TestWorkload/Reco/RecoMergewrite_DQM/RecoMergewrite_DQMEndOfRunDQMHarvestMerged/RecoMergewrite_DQMMergedEndOfRunDQMHarvestLogCollect")
        dqmHarvestLogCollectWorkflow.load()

        logCollectSub = Subscription(fileset=dqmHarvestLogCollect, workflow=dqmHarvestLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algo.")

        return

    def testPromptRecoWithSkims(self):
        """
        _testT1PromptRecoWithSkim_

        Create a T1 Prompt Reconstruction workflow with PromptSkims
        and verify it installs into WMBS correctly.
        """
        self.setupPromptSkimConfigObject()
        testArguments = PromptRecoWorkloadFactory.getTestArguments()
        # testArguments["PromptSkims"] = [self.promptSkim]
        testArguments["CouchURL"] = os.environ["COUCHURL"]
        testArguments["CouchDBName"] = "promptreco_t"
        testArguments["EnvPath"] = os.environ.get("EnvPath", None)
        testArguments["BinPath"] = os.environ.get("BinPath", None)

        factory = PromptRecoWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)
        testWorkload.setSpecUrl("somespec")
        testWorkload.setOwnerDetails("dballest@fnal.gov", "T0")

        testWMBSHelper = WMBSHelper(testWorkload, "Reco", "SomeBlock", cachepath=self.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        recoWorkflow = Workflow(name="TestWorkload",
                                task="/TestWorkload/Reco")
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

        alcaSkimWorkflow = Workflow(name="TestWorkload",
                                    task="/TestWorkload/Reco/AlcaSkim")
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
            self.assertEqual(mergedOutput.name,
                             "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-Merged" % goldenOutputMod,
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
            mergeWorkflow = Workflow(name="TestWorkload",
                                     task="/TestWorkload/Reco/RecoMerge%s" % goldenOutputMod)
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
            self.assertEqual(unmergedLogArchOutput.name,
                             "/TestWorkload/Reco/RecoMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")

        goldenOutputMods = []
        for alcaProd in testArguments["AlcaSkims"]:
            goldenOutputMods.append("ALCARECOStream%s" % alcaProd)

        for goldenOutputMod in goldenOutputMods:
            mergeWorkflow = Workflow(name="TestWorkload",
                                     task="/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s" % goldenOutputMod)
            mergeWorkflow.load()

            self.assertEqual(len(mergeWorkflow.outputMap.keys()), 2,
                             "Error: Wrong number of WF outputs %d." % len(mergeWorkflow.outputMap.keys()))

            mergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["merged_output_fileset"]
            unmergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["output_fileset"]

            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()

            self.assertEqual(mergedMergeOutput.name,
                             "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong.")
            self.assertEqual(unmergedMergeOutput.name,
                             "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong.")

            logArchOutput = mergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = mergeWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()

            self.assertEqual(logArchOutput.name,
                             "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
            self.assertEqual(unmergedLogArchOutput.name,
                             "/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")

        topLevelFileset = Fileset(name="TestWorkload-Reco-SomeBlock")
        topLevelFileset.loadData()

        recoSubscription = Subscription(fileset=topLevelFileset, workflow=recoWorkflow)
        recoSubscription.loadData()

        self.assertEqual(recoSubscription["type"], "Processing",
                         "Error: Wrong subscription type.")
        self.assertEqual(recoSubscription["split_algo"], "EventBased",
                         "Error: Wrong split algorithm. %s" % recoSubscription["split_algo"])

        alcaRecoFileset = Fileset(name="/TestWorkload/Reco/unmerged-write_ALCARECO")
        alcaRecoFileset.loadData()

        alcaSkimSubscription = Subscription(fileset=alcaRecoFileset, workflow=alcaSkimWorkflow)
        alcaSkimSubscription.loadData()

        self.assertEqual(alcaSkimSubscription["type"], "Processing",
                         "Error: Wrong subscription type.")
        self.assertEqual(alcaSkimSubscription["split_algo"], "WMBSMergeBySize",
                         "Error: Wrong split algorithm. %s" % alcaSkimSubscription["split_algo"])

        mergedRecoFileset = Fileset(name="/TestWorkload/Reco/RecoMergewrite_RECO/merged-Merged")
        mergedRecoFileset.loadData()

        unmergedOutputs = ["write_RECO", "write_AOD", "write_DQM"]
        for unmergedOutput in unmergedOutputs:
            unmergedDataTier = Fileset(name="/TestWorkload/Reco/unmerged-%s" % unmergedOutput)
            unmergedDataTier.loadData()
            dataTierMergeWorkflow = Workflow(name="TestWorkload",
                                             task="/TestWorkload/Reco/RecoMerge%s" % unmergedOutput)
            dataTierMergeWorkflow.load()
            mergeSubscription = Subscription(fileset=unmergedDataTier, workflow=dataTierMergeWorkflow)
            mergeSubscription.loadData()

            self.assertEqual(mergeSubscription["type"], "Merge",
                             "Error: Wrong subscription type.")
            self.assertEqual(mergeSubscription["split_algo"], "WMBSMergeBySize",
                             "Error: Wrong split algorithm. %s" % mergeSubscription["split_algo"])
        unmergedOutputs = []
        for alcaProd in testArguments["AlcaSkims"]:
            unmergedOutputs.append("ALCARECOStream%s" % alcaProd)
        for unmergedOutput in unmergedOutputs:
            unmergedAlcaSkim = Fileset(name="/TestWorkload/Reco/AlcaSkim/unmerged-%s" % unmergedOutput)
            unmergedAlcaSkim.loadData()
            alcaSkimMergeWorkflow = Workflow(name="TestWorkload",
                                             task="/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s" % unmergedOutput)
            alcaSkimMergeWorkflow.load()
            mergeSubscription = Subscription(fileset=unmergedAlcaSkim, workflow=alcaSkimMergeWorkflow)
            mergeSubscription.loadData()

            self.assertEqual(mergeSubscription["type"], "Merge",
                             "Error: Wrong subscription type.")
            self.assertEqual(mergeSubscription["split_algo"], "WMBSMergeBySize",
                             "Error: Wrong split algorithm. %s" % mergeSubscription["split_algo"])

        goldenOutputMods = ["write_RECO", "write_AOD", "write_DQM", "write_ALCARECO"]
        for goldenOutputMod in goldenOutputMods:
            unmergedFileset = Fileset(name="/TestWorkload/Reco/unmerged-%s" % goldenOutputMod)
            unmergedFileset.loadData()
            cleanupWorkflow = Workflow(name="TestWorkload",
                                       task="/TestWorkload/Reco/RecoCleanupUnmerged%s" % goldenOutputMod)
            cleanupWorkflow.load()
            cleanupSubscription = Subscription(fileset=unmergedFileset, workflow=cleanupWorkflow)
            cleanupSubscription.loadData()

            self.assertEqual(cleanupSubscription["type"], "Cleanup",
                             "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong subscription type.")

        goldenOutputMods = []
        for alcaProd in testArguments["AlcaSkims"]:
            goldenOutputMods.append("ALCARECOStream%s" % alcaProd)
        for goldenOutputMod in goldenOutputMods:
            unmergedFileset = Fileset(name="/TestWorkload/Reco/AlcaSkim/unmerged-%s" % goldenOutputMod)
            unmergedFileset.loadData()
            cleanupWorkflow = Workflow(name="TestWorkload",
                                       task="/TestWorkload/Reco/AlcaSkim/AlcaSkimCleanupUnmerged%s" % goldenOutputMod)
            cleanupWorkflow.load()
            cleanupSubscription = Subscription(fileset=unmergedFileset, workflow=cleanupWorkflow)
            cleanupSubscription.loadData()

            self.assertEqual(cleanupSubscription["type"], "Cleanup",
                             "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong subscription type.")

        recoLogCollect = Fileset(name="/TestWorkload/Reco/unmerged-logArchive")
        recoLogCollect.loadData()
        recoLogCollectWorkflow = Workflow(name="TestWorkload",
                                          task="/TestWorkload/Reco/LogCollect")
        recoLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset=recoLogCollect, workflow=recoLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algorithm.")

        alcaSkimLogCollect = Fileset(name="/TestWorkload/Reco/AlcaSkim/unmerged-logArchive")
        alcaSkimLogCollect.loadData()
        alcaSkimLogCollectWorkflow = Workflow(name="TestWorkload",
                                              task="/TestWorkload/Reco/AlcaSkim/AlcaSkimLogCollect")
        alcaSkimLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset=alcaSkimLogCollect, workflow=alcaSkimLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algorithm.")

        goldenOutputMods = ["write_RECO", "write_AOD", "write_DQM"]
        for goldenOutputMod in goldenOutputMods:
            recoMergeLogCollect = Fileset(name="/TestWorkload/Reco/RecoMerge%s/merged-logArchive" % goldenOutputMod)
            recoMergeLogCollect.loadData()
            recoMergeLogCollectWorkflow = Workflow(name="TestWorkload",
                                                   task="/TestWorkload/Reco/RecoMerge%s/Reco%sMergeLogCollect" % (
                                                       goldenOutputMod, goldenOutputMod))
            recoMergeLogCollectWorkflow.load()
            logCollectSubscription = Subscription(fileset=recoMergeLogCollect, workflow=recoMergeLogCollectWorkflow)
            logCollectSubscription.loadData()

            self.assertEqual(logCollectSub["type"], "LogCollect",
                             "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                             "Error: Wrong split algorithm.")

        goldenOutputMods = []
        for alcaProd in testArguments["AlcaSkims"]:
            goldenOutputMods.append("ALCARECOStream%s" % alcaProd)
        for goldenOutputMod in goldenOutputMods:
            alcaSkimLogCollect = Fileset(
                name="/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/merged-logArchive" % goldenOutputMod)
            alcaSkimLogCollect.loadData()
            alcaSkimLogCollectWorkflow = Workflow(name="TestWorkload",
                                                  task="/TestWorkload/Reco/AlcaSkim/AlcaSkimMerge%s/AlcaSkim%sMergeLogCollect" % (
                                                      goldenOutputMod, goldenOutputMod))
            alcaSkimLogCollectWorkflow.load()
            logCollectSubscription = Subscription(fileset=alcaSkimLogCollect, workflow=alcaSkimLogCollectWorkflow)
            logCollectSubscription.loadData()

            self.assertEqual(logCollectSub["type"], "LogCollect",
                             "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                             "Error: Wrong split algorithm.")

        return

    def testMemCoresSettings(self):
        """
        _testMemCoresSettings_

        Make sure the multicore and memory setings are properly propagated to
        all tasks and steps.
        """
        testArguments = PromptRecoWorkloadFactory.getTestArguments()
        testArguments["CouchURL"] = os.environ["COUCHURL"]
        testArguments["CouchDBName"] = "promptreco_t"
        testArguments["EnableHarvesting"] = True

        factory = PromptRecoWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        # test default values
        taskPaths = ['/TestWorkload/Reco', '/TestWorkload/Reco/AlcaSkim']
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
        testArguments["Multicore"] = 6
        testArguments["Memory"] = 4600.0
        testArguments["EventStreams"] = 3
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)
        for task in taskPaths:
            taskObj = testWorkload.getTaskByPath(task)
            for step in ('cmsRun1', 'stageOut1', 'logArch1'):
                stepHelper = taskObj.getStepHelper(step)
                if task == '/TestWorkload/Reco' and step == 'cmsRun1':
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

    def testFilesets(self):
        """
        Test workflow tasks, filesets and subscriptions creation
        """
        # expected tasks, filesets, subscriptions, etc
        expOutTasks = ['/TestWorkload/Reco',
                       '/TestWorkload/Reco/AlcaSkim',
                       '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamHcalCalHOCosmics',
                       '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamTkAlCosmics0T',
                       '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamMuAlGlobalCosmics',
                       '/TestWorkload/Reco/RecoMergewrite_AOD',
                       '/TestWorkload/Reco/RecoMergewrite_DQM',
                       '/TestWorkload/Reco/RecoMergewrite_RECO']
        expWfTasks = ['/TestWorkload/Reco',
                      '/TestWorkload/Reco/AlcaSkim',
                      '/TestWorkload/Reco/AlcaSkim/AlcaSkimCleanupUnmergedALCARECOStreamHcalCalHOCosmics',
                      '/TestWorkload/Reco/AlcaSkim/AlcaSkimCleanupUnmergedALCARECOStreamMuAlGlobalCosmics',
                      '/TestWorkload/Reco/AlcaSkim/AlcaSkimCleanupUnmergedALCARECOStreamTkAlCosmics0T',
                      '/TestWorkload/Reco/AlcaSkim/AlcaSkimLogCollect',
                      '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamHcalCalHOCosmics',
                      '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamHcalCalHOCosmics/AlcaSkimALCARECOStreamHcalCalHOCosmicsMergeLogCollect',
                      '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamMuAlGlobalCosmics',
                      '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamMuAlGlobalCosmics/AlcaSkimALCARECOStreamMuAlGlobalCosmicsMergeLogCollect',
                      '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamTkAlCosmics0T',
                      '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamTkAlCosmics0T/AlcaSkimALCARECOStreamTkAlCosmics0TMergeLogCollect',
                      '/TestWorkload/Reco/LogCollect',
                      '/TestWorkload/Reco/RecoCleanupUnmergedwrite_ALCARECO',
                      '/TestWorkload/Reco/RecoCleanupUnmergedwrite_AOD',
                      '/TestWorkload/Reco/RecoCleanupUnmergedwrite_DQM',
                      '/TestWorkload/Reco/RecoCleanupUnmergedwrite_RECO',
                      '/TestWorkload/Reco/RecoMergewrite_AOD',
                      '/TestWorkload/Reco/RecoMergewrite_AOD/Recowrite_AODMergeLogCollect',
                      '/TestWorkload/Reco/RecoMergewrite_DQM',
                      '/TestWorkload/Reco/RecoMergewrite_DQM/RecoMergewrite_DQMEndOfRunDQMHarvestMerged',
                      '/TestWorkload/Reco/RecoMergewrite_DQM/RecoMergewrite_DQMEndOfRunDQMHarvestMerged/RecoMergewrite_DQMMergedEndOfRunDQMHarvestLogCollect',
                      '/TestWorkload/Reco/RecoMergewrite_DQM/Recowrite_DQMMergeLogCollect',
                      '/TestWorkload/Reco/RecoMergewrite_RECO',
                      '/TestWorkload/Reco/RecoMergewrite_RECO/Recowrite_RECOMergeLogCollect']
        expFsets = ['TestWorkload-Reco-/MinimumBias/ComissioningHI-v1/RAW',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamHcalCalHOCosmics/merged-Merged',
                    '/TestWorkload/Reco/AlcaSkim/unmerged-ALCARECOStreamHcalCalHOCosmics',
                    '/TestWorkload/Reco/unmerged-write_ALCARECO',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamHcalCalHOCosmics/merged-logArchive',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamMuAlGlobalCosmics/merged-logArchive',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamMuAlGlobalCosmics/merged-Merged',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamTkAlCosmics0T/merged-logArchive',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamTkAlCosmics0T/merged-Merged',
                    '/TestWorkload/Reco/AlcaSkim/unmerged-ALCARECOStreamMuAlGlobalCosmics',
                    '/TestWorkload/Reco/AlcaSkim/unmerged-ALCARECOStreamTkAlCosmics0T',
                    '/TestWorkload/Reco/AlcaSkim/unmerged-logArchive',
                    '/TestWorkload/Reco/RecoMergewrite_AOD/merged-logArchive',
                    '/TestWorkload/Reco/RecoMergewrite_AOD/merged-Merged',
                    '/TestWorkload/Reco/unmerged-write_AOD',
                    '/TestWorkload/Reco/unmerged-write_DQM',
                    '/TestWorkload/Reco/RecoMergewrite_DQM/merged-logArchive',
                    '/TestWorkload/Reco/RecoMergewrite_DQM/merged-Merged',
                    '/TestWorkload/Reco/RecoMergewrite_DQM/RecoMergewrite_DQMEndOfRunDQMHarvestMerged/unmerged-logArchive',
                    '/TestWorkload/Reco/RecoMergewrite_RECO/merged-logArchive',
                    '/TestWorkload/Reco/RecoMergewrite_RECO/merged-Merged',
                    '/TestWorkload/Reco/unmerged-logArchive',
                    '/TestWorkload/Reco/unmerged-write_RECO']
        subMaps = [(4,
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamHcalCalHOCosmics/merged-logArchive',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamHcalCalHOCosmics/AlcaSkimALCARECOStreamHcalCalHOCosmicsMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (10,
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamMuAlGlobalCosmics/merged-logArchive',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamMuAlGlobalCosmics/AlcaSkimALCARECOStreamMuAlGlobalCosmicsMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (7,
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamTkAlCosmics0T/merged-logArchive',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamTkAlCosmics0T/AlcaSkimALCARECOStreamTkAlCosmics0TMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (5,
                    '/TestWorkload/Reco/AlcaSkim/unmerged-ALCARECOStreamHcalCalHOCosmics',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimCleanupUnmergedALCARECOStreamHcalCalHOCosmics',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (3,
                    '/TestWorkload/Reco/AlcaSkim/unmerged-ALCARECOStreamHcalCalHOCosmics',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamHcalCalHOCosmics',
                    'WMBSMergeBySize',
                    'Merge'),
                   (11,
                    '/TestWorkload/Reco/AlcaSkim/unmerged-ALCARECOStreamMuAlGlobalCosmics',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimCleanupUnmergedALCARECOStreamMuAlGlobalCosmics',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (9,
                    '/TestWorkload/Reco/AlcaSkim/unmerged-ALCARECOStreamMuAlGlobalCosmics',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamMuAlGlobalCosmics',
                    'WMBSMergeBySize',
                    'Merge'),
                   (8,
                    '/TestWorkload/Reco/AlcaSkim/unmerged-ALCARECOStreamTkAlCosmics0T',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimCleanupUnmergedALCARECOStreamTkAlCosmics0T',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (6,
                    '/TestWorkload/Reco/AlcaSkim/unmerged-ALCARECOStreamTkAlCosmics0T',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimMergeALCARECOStreamTkAlCosmics0T',
                    'WMBSMergeBySize',
                    'Merge'),
                   (12,
                    '/TestWorkload/Reco/AlcaSkim/unmerged-logArchive',
                    '/TestWorkload/Reco/AlcaSkim/AlcaSkimLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (15,
                    '/TestWorkload/Reco/RecoMergewrite_AOD/merged-logArchive',
                    '/TestWorkload/Reco/RecoMergewrite_AOD/Recowrite_AODMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (20,
                    '/TestWorkload/Reco/RecoMergewrite_DQM/merged-logArchive',
                    '/TestWorkload/Reco/RecoMergewrite_DQM/Recowrite_DQMMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (18,
                    '/TestWorkload/Reco/RecoMergewrite_DQM/merged-Merged',
                    '/TestWorkload/Reco/RecoMergewrite_DQM/RecoMergewrite_DQMEndOfRunDQMHarvestMerged',
                    'Harvest',
                    'Harvesting'),
                   (19,
                    '/TestWorkload/Reco/RecoMergewrite_DQM/RecoMergewrite_DQMEndOfRunDQMHarvestMerged/unmerged-logArchive',
                    '/TestWorkload/Reco/RecoMergewrite_DQM/RecoMergewrite_DQMEndOfRunDQMHarvestMerged/RecoMergewrite_DQMMergedEndOfRunDQMHarvestLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (23,
                    '/TestWorkload/Reco/RecoMergewrite_RECO/merged-logArchive',
                    '/TestWorkload/Reco/RecoMergewrite_RECO/Recowrite_RECOMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (25,
                    '/TestWorkload/Reco/unmerged-logArchive',
                    '/TestWorkload/Reco/LogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (2,
                    '/TestWorkload/Reco/unmerged-write_ALCARECO',
                    '/TestWorkload/Reco/AlcaSkim',
                    'WMBSMergeBySize',
                    'Processing'),
                   (13,
                    '/TestWorkload/Reco/unmerged-write_ALCARECO',
                    '/TestWorkload/Reco/RecoCleanupUnmergedwrite_ALCARECO',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (16,
                    '/TestWorkload/Reco/unmerged-write_AOD',
                    '/TestWorkload/Reco/RecoCleanupUnmergedwrite_AOD',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (14,
                    '/TestWorkload/Reco/unmerged-write_AOD',
                    '/TestWorkload/Reco/RecoMergewrite_AOD',
                    'WMBSMergeBySize',
                    'Merge'),
                   (21,
                    '/TestWorkload/Reco/unmerged-write_DQM',
                    '/TestWorkload/Reco/RecoCleanupUnmergedwrite_DQM',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (17,
                    '/TestWorkload/Reco/unmerged-write_DQM',
                    '/TestWorkload/Reco/RecoMergewrite_DQM',
                    'WMBSMergeBySize',
                    'Merge'),
                   (24,
                    '/TestWorkload/Reco/unmerged-write_RECO',
                    '/TestWorkload/Reco/RecoCleanupUnmergedwrite_RECO',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (22,
                    '/TestWorkload/Reco/unmerged-write_RECO',
                    '/TestWorkload/Reco/RecoMergewrite_RECO',
                    'WMBSMergeBySize',
                    'Merge'),
                   (1,
                    'TestWorkload-Reco-/MinimumBias/ComissioningHI-v1/RAW',
                    '/TestWorkload/Reco',
                    'EventBased',
                    'Processing')]

        testArguments = PromptRecoWorkloadFactory.getTestArguments()
        testArguments["CouchURL"] = os.environ["COUCHURL"]
        testArguments["CouchDBName"] = "promptreco_t"
        testArguments["EnableHarvesting"] = True

        factory = PromptRecoWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        testWMBSHelper = WMBSHelper(testWorkload, "Reco", blockName=testArguments['InputDataset'],
                                    cachepath=self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        print("Tasks producing output:\n%s" % pformat(testWorkload.listOutputProducingTasks()))
        self.assertItemsEqual(testWorkload.listOutputProducingTasks(), expOutTasks)

        workflows = self.listTasksByWorkflow.execute(workflow="TestWorkload")
        print("List of workflow tasks:\n%s" % pformat([item['task'] for item in workflows]))
        self.assertItemsEqual([item['task'] for item in workflows], expWfTasks)

        # returns a tuple of id, name, open and last_update
        filesets = self.listFilesets.execute()
        print("List of filesets:\n%s" % pformat([item[1] for item in filesets]))
        self.assertItemsEqual([item[1] for item in filesets], expFsets)

        subscriptions = self.listSubsMapping.execute(workflow="TestWorkload", returnTuple=True)
        print("List of subscriptions:\n%s" % pformat(subscriptions))
        self.assertItemsEqual(subscriptions, subMaps)


if __name__ == '__main__':
    unittest.main()
