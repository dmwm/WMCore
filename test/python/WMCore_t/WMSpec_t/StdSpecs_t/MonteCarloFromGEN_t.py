#!/usr/bin/env python
"""
_MonteCarloFromGEN_t_

Unit tests for the MonteCarloFromGEN workflow.
"""

import unittest
import os
import threading

from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow

from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMCore.WMSpec.StdSpecs.MonteCarloFromGEN import getTestArguments, monteCarloFromGENWorkload

from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.Database.CMSCouch import CouchServer, Document

class MonteCarloFromGENTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Initialize the database.
        """
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("mclhe_t", "ConfigCache")
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)        

        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("mclhe_t")        
        return

    def tearDown(self):
        """
        _tearDown_

        Clear out the database.
        """
        self.testInit.clearDatabase()
        return

    def injectConfig(self):
        """
        _injectConfig_

        Create a bogus config cache document and inject it into couch.  Return
        the ID of the document.
        """
        newConfig = Document()
        newConfig["info"] = None
        newConfig["config"] = None
        newConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e580f"
        newConfig["pset_hash"] = "7c856ad35f9f544839d8525ca10259a7"
        newConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
        newConfig["pset_tweak_details"] ={"process": {"outputModules_": ["outputRECORECO", "outputALCARECOALCARECO"],
                                                      "outputRECORECO": {"dataset": {"filterName": "FilterRECO",
                                                                                     "dataTier": "RECO"}},
                                                      "outputALCARECOALCARECO": {"dataset": {"filterName": "FilterALCARECO",
                                                                                             "dataTier": "ALCARECO"}}}}
        result = self.configDatabase.commitOne(newConfig)
        return result[0]["id"]

    def DISABLEDtestMonteCarloFromGEN(self):
        """
        _testMonteCarloFromGEN_

        Create a MonteCarloFromGEN workflow and verify it installs into WMBS
        correctly.
        """
        arguments = getTestArguments()
        arguments["ProcConfigCacheID"] = self.injectConfig()
        arguments["CouchDBName"] = "mclhe_t"
        testWorkload = monteCarloFromGENWorkload("TestWorkload", arguments)
        testWorkload.setSpecUrl("somespec")
        testWorkload.setOwnerDetails("sfoulkes@fnal.gov", "DMWM")
        
        testWMBSHelper = WMBSHelper(testWorkload, "SomeBlock")
        testWMBSHelper.createSubscription()

        procWorkflow = Workflow(name = "TestWorkload",
                                task = "/TestWorkload/MonteCarloFromGEN")
        procWorkflow.load()

        self.assertEqual(len(procWorkflow.outputMap.keys()), 3,
                         "Error: Wrong number of WF outputs.")

        goldenOutputMods = ["outputRECORECO", "outputALCARECOALCARECO"]
        for goldenOutputMod in goldenOutputMods:
            mergedOutput = procWorkflow.outputMap[goldenOutputMod][0]["merged_output_fileset"]
            unmergedOutput = procWorkflow.outputMap[goldenOutputMod][0]["output_fileset"]

            mergedOutput.loadData()
            unmergedOutput.loadData()

            self.assertEqual(mergedOutput.name, "/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/MonteCarloFromGEN/unmerged-%s" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong.")

        logArchOutput = procWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = procWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name, "/TestWorkload/MonteCarloFromGEN/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/MonteCarloFromGEN/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        for goldenOutputMod in goldenOutputMods:
            mergeWorkflow = Workflow(name = "TestWorkload",
                                     task = "/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMerge%s" % goldenOutputMod)
            mergeWorkflow.load()

            self.assertEqual(len(mergeWorkflow.outputMap.keys()), 2,
                             "Error: Wrong number of WF outputs.")

            mergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["merged_output_fileset"]
            unmergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["output_fileset"]

            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()

            self.assertEqual(mergedMergeOutput.name, "/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong.")
            self.assertEqual(unmergedMergeOutput.name, "/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong.")

            logArchOutput = mergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = mergeWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()

            self.assertEqual(logArchOutput.name, "/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
            self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")            

        topLevelFileset = Fileset(name = "TestWorkload-MonteCarloFromGEN-SomeBlock")
        topLevelFileset.loadData()

        procSubscription = Subscription(fileset = topLevelFileset, workflow = procWorkflow)
        procSubscription.loadData()

        self.assertEqual(procSubscription["type"], "Processing",
                         "Error: Wrong subscription type.")
        self.assertEqual(procSubscription["split_algo"], "LumiBased",
                         "Error: Wrong split algo.")

        unmergedReco = Fileset(name = "/TestWorkload/MonteCarloFromGEN/unmerged-outputRECORECO")
        unmergedReco.loadData()
        recoMergeWorkflow = Workflow(name = "TestWorkload",
                                     task = "/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeoutputRECORECO")
        recoMergeWorkflow.load()
        mergeSubscription = Subscription(fileset = unmergedReco, workflow = recoMergeWorkflow)
        mergeSubscription.loadData()

        self.assertEqual(mergeSubscription["type"], "Merge",
                         "Error: Wrong subscription type.")
        self.assertEqual(mergeSubscription["split_algo"], "WMBSMergeBySize",
                         "Error: Wrong split algo.")

        unmergedAlca = Fileset(name = "/TestWorkload/MonteCarloFromGEN/unmerged-outputALCARECOALCARECO")
        unmergedAlca.loadData()
        alcaMergeWorkflow = Workflow(name = "TestWorkload",
                                     task = "/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeoutputALCARECOALCARECO")
        alcaMergeWorkflow.load()
        mergeSubscription = Subscription(fileset = unmergedAlca, workflow = alcaMergeWorkflow)
        mergeSubscription.loadData()

        self.assertEqual(mergeSubscription["type"], "Merge",
                         "Error: Wrong subscription type.")
        self.assertEqual(mergeSubscription["split_algo"], "WMBSMergeBySize",
                         "Error: Wrong split algo.")

        for procOutput in ["outputRECORECO", "outputALCARECOALCARECO"]:
            unmerged = Fileset(name = "/TestWorkload/MonteCarloFromGEN/unmerged-%s" % procOutput)
            unmerged.loadData()
            cleanupWorkflow = Workflow(name = "TestWorkload",
                                      task = "/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENCleanupUnmerged%s" % procOutput)
            cleanupWorkflow.load()
            cleanupSubscription = Subscription(fileset = unmerged, workflow = cleanupWorkflow)
            cleanupSubscription.loadData()

            self.assertEqual(cleanupSubscription["type"], "Cleanup",
                             "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong split algo.")

        procLogCollect = Fileset(name = "/TestWorkload/MonteCarloFromGEN/unmerged-logArchive")
        procLogCollect.loadData()
        procLogCollectWorkflow = Workflow(name = "TestWorkload",
                                          task = "/TestWorkload/MonteCarloFromGEN/LogCollect")
        procLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset = procLogCollect, workflow = procLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "EndOfRun",
                         "Error: Wrong split algo.")

        procLogCollect = Fileset(name = "/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeoutputRECORECO/merged-logArchive")
        procLogCollect.loadData()
        procLogCollectWorkflow = Workflow(name = "TestWorkload",
                                          task = "/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeoutputRECORECO/MonteCarloFromGENoutputRECORECOMergeLogCollect")
        procLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset = procLogCollect, workflow = procLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "EndOfRun",
                         "Error: Wrong split algo.")

        procLogCollect = Fileset(name = "/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeoutputALCARECOALCARECO/merged-logArchive")
        procLogCollect.loadData()
        procLogCollectWorkflow = Workflow(name = "TestWorkload",
                                          task = "/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeoutputALCARECOALCARECO/MonteCarloFromGENoutputALCARECOALCARECOMergeLogCollect")
        procLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset = procLogCollect, workflow = procLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "EndOfRun",
                         "Error: Wrong split algo.")                

        return

if __name__ == '__main__':
    unittest.main()
