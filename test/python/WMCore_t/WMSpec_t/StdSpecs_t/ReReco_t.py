#!/usr/bin/env python
"""
_ReReco_t_

Unit tests for the ReReco workflow.
"""

import unittest
import os
import threading

from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow

from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMCore.WMSpec.StdSpecs.DataProcessing import getTestArguments
from WMCore.WMSpec.StdSpecs.ReReco import rerecoWorkload

from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.Database.CMSCouch import CouchServer, Document

class ReRecoTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Initialize the database and couch.
        """
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("rereco_t", "ConfigCache")
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        self.testDir = self.testInit.generateWorkDir()
        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("rereco_t")
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

    def injectReRecoConfig(self):
        """
        _injectReRecoConfig_

        Inject a ReReco config document that we can use to set the outputModules
        """

        newConfig = Document()
        newConfig["info"] = None
        newConfig["config"] = None
        newConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e580f"
        newConfig["pset_hash"] = "7c856ad35f9f544839d8525ca10259a7"
        newConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
        newConfig["pset_tweak_details"] ={"process": {"outputModules_": ['RECOoutput', 'DQMoutput'],
                                                      "RECOoutput": {'dataset': {'filterName': 'RECOoutputFilter',
                                                                                 'dataTier': 'RECO'}},
                                                      "DQMoutput": {'dataset' : {'filterName': 'DQMoutputFilter',
                                                                                 'dataTier': 'DQM'}}}}
        result = self.configDatabase.commitOne(newConfig)
        return result[0]["id"]


    def injectSkimConfig(self):
        """
        _injectSkimConfig_

        Create a bogus config cache document for the skims and inject it into
        couch.  Return the ID of the document.
        """
        newConfig = Document()
        newConfig["info"] = None
        newConfig["config"] = None
        newConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e580f"
        newConfig["pset_hash"] = "7c856ad35f9f544839d8525ca10259a7"
        newConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
        newConfig["pset_tweak_details"] ={"process": {"outputModules_": ["SkimA", "SkimB"],
                                                      "SkimA": {"dataset": {"filterName": "SkimAFilter",
                                                                            "dataTier": "RAW-RECO"}},
                                                      "SkimB": {"dataset": {"filterName": "SkimBFilter",
                                                                            "dataTier": "USER"}}}}
        result = self.configDatabase.commitOne(newConfig)
        return result[0]["id"]

    def testReReco(self):
        """
        _testReReco_

        Verify that ReReco workflows can be created and inserted into WMBS
        correctly.  The ReReco workflow is just a DataProcessing workflow with
        skims tacked on.  We'll test the skims and DQMHarvest here.
        """
        skimConfig = self.injectSkimConfig()
        recoConfig = self.injectReRecoConfig()
        dataProcArguments = getTestArguments()
        dataProcArguments['ProcessingString']  = 'ProcString'
        dataProcArguments['ConfigCacheID'] = recoConfig
        dataProcArguments["SkimConfigs"] = [{"SkimName": "SomeSkim",
                                             "SkimInput": "RECOoutput",
                                             "SkimSplitAlgo": "FileBased",
                                             "SkimSplitArgs": {"files_per_job": 1,
                                                               "include_parents": True},
                                             "ConfigCacheID": skimConfig,
                                             "Scenario": None}]
        dataProcArguments["CouchURL"] = os.environ["COUCHURL"]
        dataProcArguments["CouchDBName"] = "rereco_t"

        testWorkload = rerecoWorkload("TestWorkload", dataProcArguments)
        testWorkload.setSpecUrl("somespec")
        testWorkload.setOwnerDetails("sfoulkes@fnal.gov", "DMWM")

        self.assertEqual(testWorkload.data.tasks.DataProcessing.tree.children.DataProcessingMergeRECOoutput.\
                         tree.children.SomeSkim.tree.children.SomeSkimMergeSkimB.steps.cmsRun1.output.modules.\
                         Merged.mergedLFNBase,
                         '/store/data/WMAgentCommissioning10/MinimumBias/USER/SkimBFilter-ProcString-v2')

        testWMBSHelper = WMBSHelper(testWorkload, "DataProcessing", "SomeBlock", cachepath = self.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        skimWorkflow = Workflow(name = "TestWorkload",
                                task = "/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim")
        skimWorkflow.load()

        self.assertEqual(len(skimWorkflow.outputMap.keys()), 3,
                         "Error: Wrong number of WF outputs.")

        goldenOutputMods = ["SkimA", "SkimB"]
        for goldenOutputMod in goldenOutputMods:
            mergedOutput = skimWorkflow.outputMap[goldenOutputMod][0]["merged_output_fileset"]
            unmergedOutput = skimWorkflow.outputMap[goldenOutputMod][0]["output_fileset"]

            mergedOutput.loadData()
            unmergedOutput.loadData()

            self.assertEqual(mergedOutput.name, "/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/unmerged-%s" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong: %s" % unmergedOutput.name)

        logArchOutput = skimWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = skimWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name, "/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        for goldenOutputMod in goldenOutputMods:
            mergeWorkflow = Workflow(name = "TestWorkload",
                                     task = "/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMerge%s" % goldenOutputMod)
            mergeWorkflow.load()

            self.assertEqual(len(mergeWorkflow.outputMap.keys()), 2,
                             "Error: Wrong number of WF outputs.")

            mergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["merged_output_fileset"]
            unmergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["output_fileset"]

            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()

            self.assertEqual(mergedMergeOutput.name, "/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong.")
            self.assertEqual(unmergedMergeOutput.name, "/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong.")

            logArchOutput = mergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = mergeWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()

            self.assertEqual(logArchOutput.name, "/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
            self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")

        topLevelFileset = Fileset(name = "/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/merged-Merged")
        topLevelFileset.loadData()

        skimSubscription = Subscription(fileset = topLevelFileset, workflow = skimWorkflow)
        skimSubscription.loadData()

        self.assertEqual(skimSubscription["type"], "Skim",
                         "Error: Wrong subscription type.")
        self.assertEqual(skimSubscription["split_algo"], "FileBased",
                         "Error: Wrong split algo.")

        for skimOutput in ["A", "B"]:
            unmerged = Fileset(name = "/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/unmerged-Skim%s" % skimOutput)
            unmerged.loadData()
            mergeWorkflow = Workflow(name = "TestWorkload",
                                      task = "/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMergeSkim%s" % skimOutput)
            mergeWorkflow.load()
            mergeSubscription = Subscription(fileset = unmerged, workflow = mergeWorkflow)
            mergeSubscription.loadData()

            self.assertEqual(mergeSubscription["type"], "Merge",
                             "Error: Wrong subscription type.")
            self.assertEqual(mergeSubscription["split_algo"], "ParentlessMergeBySize",
                             "Error: Wrong split algo.")

        for skimOutput in ["A", "B"]:
            unmerged = Fileset(name = "/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/unmerged-Skim%s" % skimOutput)
            unmerged.loadData()
            cleanupWorkflow = Workflow(name = "TestWorkload",
                                      task = "/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimCleanupUnmergedSkim%s" % skimOutput)
            cleanupWorkflow.load()
            cleanupSubscription = Subscription(fileset = unmerged, workflow = cleanupWorkflow)
            cleanupSubscription.loadData()

            self.assertEqual(cleanupSubscription["type"], "Cleanup",
                             "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong split algo.")

        for skimOutput in ["A", "B"]:
            skimMergeLogCollect = Fileset(name = "/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMergeSkim%s/merged-logArchive" % skimOutput)
            skimMergeLogCollect.loadData()
            skimMergeLogCollectWorkflow = Workflow(name = "TestWorkload",
                                                   task = "/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/SomeSkim/SomeSkimMergeSkim%s/SomeSkimSkim%sMergeLogCollect" % (skimOutput, skimOutput))
            skimMergeLogCollectWorkflow.load()
            logCollectSub = Subscription(fileset = skimMergeLogCollect, workflow = skimMergeLogCollectWorkflow)
            logCollectSub.loadData()

            self.assertEqual(logCollectSub["type"], "LogCollect",
                             "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                             "Error: Wrong split algo.")

        dqmWorkflow = Workflow(name = "TestWorkload",
                               task = "/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput/DataProcessingMergeDQMoutputEndOfRunDQMHarvestMerged")
        dqmWorkflow.load()

        topLevelFileset = Fileset(name = "/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput/merged-Merged")
        topLevelFileset.loadData()

        dqmSubscription = Subscription(fileset = topLevelFileset, workflow = dqmWorkflow)
        dqmSubscription.loadData()

        self.assertEqual(dqmSubscription["type"], "Harvesting",
                         "Error: Wrong subscription type.")
        self.assertEqual(dqmSubscription["split_algo"], "Harvest",
                         "Error: Wrong split algo.")

        logArchOutput = dqmWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = dqmWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name, "/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput/DataProcessingMergeDQMoutputEndOfRunDQMHarvestMerged/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput/DataProcessingMergeDQMoutputEndOfRunDQMHarvestMerged/unmerged-logArchive",
                     "Error: LogArchive output fileset is wrong.")

        dqmHarvestLogCollect = Fileset(name = "/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput/DataProcessingMergeDQMoutputEndOfRunDQMHarvestMerged/unmerged-logArchive")
        dqmHarvestLogCollect.loadData()
        dqmHarvestLogCollectWorkflow = Workflow(name = "TestWorkload",
                                               task = "/TestWorkload/DataProcessing/DataProcessingMergeDQMoutput/DataProcessingMergeDQMoutputEndOfRunDQMHarvestMerged/DataProcessingMergeDQMoutputMergedEndOfRunDQMHarvestLogCollect")
        dqmHarvestLogCollectWorkflow.load()

        logCollectSub = Subscription(fileset = dqmHarvestLogCollect, workflow = dqmHarvestLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algo.")

        return

    def testReRecoDroppingRECO(self):
        """
        _testReRecoDroppingRECO_

        Verify that ReReco workflows can be created and inserted into WMBS
        correctly.  The ReReco workflow is just a DataProcessing workflow with
        skims tacked on. This tests run on unmerged RECO output
        """
        skimConfig = self.injectSkimConfig()
        recoConfig = self.injectReRecoConfig()
        dataProcArguments = getTestArguments()
        dataProcArguments['ProcessingString']  = 'ProcString'
        dataProcArguments['ConfigCacheID'] = recoConfig
        dataProcArguments["SkimConfigs"] = [{"SkimName": "SomeSkim",
                                             "SkimInput": "RECOoutput",
                                             "SkimSplitAlgo": "FileBased",
                                             "SkimSplitArgs": {"files_per_job": 1,
                                                               "include_parents": True},
                                             "ConfigCacheID": skimConfig,
                                             "Scenario": None}]
        dataProcArguments["CouchURL"] = os.environ["COUCHURL"]
        dataProcArguments["CouchDBName"] = "rereco_t"
        dataProcArguments["TransientOutputModules"] = ["RECOoutput"]

        testWorkload = rerecoWorkload("TestWorkload", dataProcArguments)
        testWorkload.setSpecUrl("somespec")
        testWorkload.setOwnerDetails("sfoulkes@fnal.gov", "DMWM")

        self.assertEqual(testWorkload.data.tasks.DataProcessing.tree.children.\
                         SomeSkim.tree.children.SomeSkimMergeSkimB.steps.cmsRun1.output.modules.\
                         Merged.mergedLFNBase,
                         '/store/data/WMAgentCommissioning10/MinimumBias/USER/SkimBFilter-ProcString-v2')

        testWMBSHelper = WMBSHelper(testWorkload, "DataProcessing", "SomeBlock", cachepath = self.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        skimWorkflow = Workflow(name = "TestWorkload",
                                task = "/TestWorkload/DataProcessing/SomeSkim")
        skimWorkflow.load()

        self.assertEqual(len(skimWorkflow.outputMap.keys()), 3,
                         "Error: Wrong number of WF outputs.")

        goldenOutputMods = ["SkimA", "SkimB"]
        for goldenOutputMod in goldenOutputMods:
            mergedOutput = skimWorkflow.outputMap[goldenOutputMod][0]["merged_output_fileset"]
            unmergedOutput = skimWorkflow.outputMap[goldenOutputMod][0]["output_fileset"]

            mergedOutput.loadData()
            unmergedOutput.loadData()

            self.assertEqual(mergedOutput.name, "/TestWorkload/DataProcessing/SomeSkim/SomeSkimMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/DataProcessing/SomeSkim/unmerged-%s" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong: %s" % unmergedOutput.name)

        logArchOutput = skimWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = skimWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name, "/TestWorkload/DataProcessing/SomeSkim/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/DataProcessing/SomeSkim/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        for goldenOutputMod in goldenOutputMods:
            mergeWorkflow = Workflow(name = "TestWorkload",
                                     task = "/TestWorkload/DataProcessing/SomeSkim/SomeSkimMerge%s" % goldenOutputMod)
            mergeWorkflow.load()

            self.assertEqual(len(mergeWorkflow.outputMap.keys()), 2,
                             "Error: Wrong number of WF outputs.")

            mergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["merged_output_fileset"]
            unmergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["output_fileset"]

            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()

            self.assertEqual(mergedMergeOutput.name, "/TestWorkload/DataProcessing/SomeSkim/SomeSkimMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong.")
            self.assertEqual(unmergedMergeOutput.name, "/TestWorkload/DataProcessing/SomeSkim/SomeSkimMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong.")

            logArchOutput = mergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = mergeWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()

            self.assertEqual(logArchOutput.name, "/TestWorkload/DataProcessing/SomeSkim/SomeSkimMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
            self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/DataProcessing/SomeSkim/SomeSkimMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")

        topLevelFileset = Fileset(name = "/TestWorkload/DataProcessing/unmerged-RECOoutput")
        topLevelFileset.loadData()

        skimSubscription = Subscription(fileset = topLevelFileset, workflow = skimWorkflow)
        skimSubscription.loadData()

        self.assertEqual(skimSubscription["type"], "Skim",
                         "Error: Wrong subscription type.")
        self.assertEqual(skimSubscription["split_algo"], "FileBased",
                         "Error: Wrong split algo.")

        for skimOutput in ["A", "B"]:
            unmerged = Fileset(name = "/TestWorkload/DataProcessing/SomeSkim/unmerged-Skim%s" % skimOutput)
            unmerged.loadData()
            mergeWorkflow = Workflow(name = "TestWorkload",
                                      task = "/TestWorkload/DataProcessing/SomeSkim/SomeSkimMergeSkim%s" % skimOutput)
            mergeWorkflow.load()
            mergeSubscription = Subscription(fileset = unmerged, workflow = mergeWorkflow)
            mergeSubscription.loadData()

            self.assertEqual(mergeSubscription["type"], "Merge",
                             "Error: Wrong subscription type.")
            self.assertEqual(mergeSubscription["split_algo"], "ParentlessMergeBySize",
                             "Error: Wrong split algo.")

        for skimOutput in ["A", "B"]:
            unmerged = Fileset(name = "/TestWorkload/DataProcessing/SomeSkim/unmerged-Skim%s" % skimOutput)
            unmerged.loadData()
            cleanupWorkflow = Workflow(name = "TestWorkload",
                                      task = "/TestWorkload/DataProcessing/SomeSkim/SomeSkimCleanupUnmergedSkim%s" % skimOutput)
            cleanupWorkflow.load()
            cleanupSubscription = Subscription(fileset = unmerged, workflow = cleanupWorkflow)
            cleanupSubscription.loadData()

            self.assertEqual(cleanupSubscription["type"], "Cleanup",
                             "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong split algo.")

        for skimOutput in ["A", "B"]:
            skimMergeLogCollect = Fileset(name = "/TestWorkload/DataProcessing/SomeSkim/SomeSkimMergeSkim%s/merged-logArchive" % skimOutput)
            skimMergeLogCollect.loadData()
            skimMergeLogCollectWorkflow = Workflow(name = "TestWorkload",
                                                   task = "/TestWorkload/DataProcessing/SomeSkim/SomeSkimMergeSkim%s/SomeSkimSkim%sMergeLogCollect" % (skimOutput, skimOutput))
            skimMergeLogCollectWorkflow.load()
            logCollectSub = Subscription(fileset = skimMergeLogCollect, workflow = skimMergeLogCollectWorkflow)
            logCollectSub.loadData()

            self.assertEqual(logCollectSub["type"], "LogCollect",
                             "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                             "Error: Wrong split algo.")

        return

if __name__ == '__main__':
    unittest.main()
