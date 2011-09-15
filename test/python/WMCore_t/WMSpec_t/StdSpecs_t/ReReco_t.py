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
        return

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

    def DISABLEDtestReReco(self):
        """
        _testReReco_

        Verify that ReReco workflows can be created and inserted into WMBS
        correctly.  The ReReco workflow is just a DataProcessing workflow with
        skims tacked on.  We'll only test the skims here.
        """
        skimConfig = self.injectSkimConfig()
        dataProcArguments = getTestArguments()
        dataProcArguments["SkimConfigs"] = [{"SkimName": "SomeSkim",
                                             "SkimInput": "outputRECORECO",
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
        
        testWMBSHelper = WMBSHelper(testWorkload, "SomeBlock")
        testWMBSHelper.createSubscription()

        skimWorkflow = Workflow(name = "TestWorkload",
                                task = "/TestWorkload/DataProcessing/DataProcessingMergeoutputRECORECO/SomeSkim")
        skimWorkflow.load()

        self.assertEqual(len(skimWorkflow.outputMap.keys()), 3,
                         "Error: Wrong number of WF outputs.")

        goldenOutputMods = ["SkimA", "SkimB"]
        for goldenOutputMod in goldenOutputMods:
            mergedOutput = skimWorkflow.outputMap[goldenOutputMod][0]["merged_output_fileset"]
            unmergedOutput = skimWorkflow.outputMap[goldenOutputMod][0]["output_fileset"]

            mergedOutput.loadData()
            unmergedOutput.loadData()

            self.assertEqual(mergedOutput.name, "/TestWorkload/DataProcessing/DataProcessingMergeoutputRECORECO/SomeSkim/SomeSkimMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/DataProcessing/DataProcessingMergeoutputRECORECO/SomeSkim/unmerged-%s" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong: %s" % unmergedOutput.name)

        logArchOutput = skimWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = skimWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name, "/TestWorkload/DataProcessing/DataProcessingMergeoutputRECORECO/SomeSkim/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/DataProcessing/DataProcessingMergeoutputRECORECO/SomeSkim/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        for goldenOutputMod in goldenOutputMods:
            mergeWorkflow = Workflow(name = "TestWorkload",
                                     task = "/TestWorkload/DataProcessing/DataProcessingMergeoutputRECORECO/SomeSkim/SomeSkimMerge%s" % goldenOutputMod)
            mergeWorkflow.load()

            self.assertEqual(len(mergeWorkflow.outputMap.keys()), 2,
                             "Error: Wrong number of WF outputs.")

            mergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["merged_output_fileset"]
            unmergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["output_fileset"]

            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()

            self.assertEqual(mergedMergeOutput.name, "/TestWorkload/DataProcessing/DataProcessingMergeoutputRECORECO/SomeSkim/SomeSkimMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong.")
            self.assertEqual(unmergedMergeOutput.name, "/TestWorkload/DataProcessing/DataProcessingMergeoutputRECORECO/SomeSkim/SomeSkimMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong.")

            logArchOutput = mergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = mergeWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()

            self.assertEqual(logArchOutput.name, "/TestWorkload/DataProcessing/DataProcessingMergeoutputRECORECO/SomeSkim/SomeSkimMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
            self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/DataProcessing/DataProcessingMergeoutputRECORECO/SomeSkim/SomeSkimMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")            

        topLevelFileset = Fileset(name = "/TestWorkload/DataProcessing/DataProcessingMergeoutputRECORECO/merged-Merged")
        topLevelFileset.loadData()

        skimSubscription = Subscription(fileset = topLevelFileset, workflow = skimWorkflow)
        skimSubscription.loadData()

        self.assertEqual(skimSubscription["type"], "Skim",
                         "Error: Wrong subscription type.")
        self.assertEqual(skimSubscription["split_algo"], "FileBased",
                         "Error: Wrong split algo.")

        for skimOutput in ["A", "B"]:
            unmerged = Fileset(name = "/TestWorkload/DataProcessing/DataProcessingMergeoutputRECORECO/SomeSkim/unmerged-Skim%s" % skimOutput)
            unmerged.loadData()
            mergeWorkflow = Workflow(name = "TestWorkload",
                                      task = "/TestWorkload/DataProcessing/DataProcessingMergeoutputRECORECO/SomeSkim/SomeSkimMergeSkim%s" % skimOutput)
            mergeWorkflow.load()
            mergeSubscription = Subscription(fileset = unmerged, workflow = mergeWorkflow)
            mergeSubscription.loadData()

            self.assertEqual(mergeSubscription["type"], "Merge",
                             "Error: Wrong subscription type.")
            self.assertEqual(mergeSubscription["split_algo"], "ParentlessMergeBySize",
                             "Error: Wrong split algo.")

        for skimOutput in ["A", "B"]:
            unmerged = Fileset(name = "/TestWorkload/DataProcessing/DataProcessingMergeoutputRECORECO/SomeSkim/unmerged-Skim%s" % skimOutput)
            unmerged.loadData()
            cleanupWorkflow = Workflow(name = "TestWorkload",
                                      task = "/TestWorkload/DataProcessing/DataProcessingMergeoutputRECORECO/SomeSkim/SomeSkimCleanupUnmergedSkim%s" % skimOutput)
            cleanupWorkflow.load()
            cleanupSubscription = Subscription(fileset = unmerged, workflow = cleanupWorkflow)
            cleanupSubscription.loadData()

            self.assertEqual(cleanupSubscription["type"], "Cleanup",
                             "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong split algo.")

        for skimOutput in ["A", "B"]:
            skimMergeLogCollect = Fileset(name = "/TestWorkload/DataProcessing/DataProcessingMergeoutputRECORECO/SomeSkim/SomeSkimMergeSkim%s/merged-logArchive" % skimOutput)
            skimMergeLogCollect.loadData()
            skimMergeLogCollectWorkflow = Workflow(name = "TestWorkload",
                                                   task = "/TestWorkload/DataProcessing/DataProcessingMergeoutputRECORECO/SomeSkim/SomeSkimMergeSkim%s/SomeSkimSkim%sMergeLogCollect" % (skimOutput, skimOutput))
            skimMergeLogCollectWorkflow.load()
            logCollectSub = Subscription(fileset = skimMergeLogCollect, workflow = skimMergeLogCollectWorkflow)
            logCollectSub.loadData()
            
            self.assertEqual(logCollectSub["type"], "LogCollect",
                             "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "EndOfRun",
                             "Error: Wrong split algo.")

        return

if __name__ == '__main__':
    unittest.main()
