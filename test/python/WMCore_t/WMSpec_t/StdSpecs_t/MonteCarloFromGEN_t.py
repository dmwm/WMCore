#!/usr/bin/env python
"""
_MonteCarloFromGEN_t_

Unit tests for the MonteCarloFromGEN workflow.
"""

import unittest
import os

from WMCore.Database.CMSCouch import CouchServer, Document
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMSpec.StdSpecs.MonteCarloFromGEN import MonteCarloFromGENWorkloadFactory
from WMCore.WorkQueue.WMBSHelper import WMBSHelper

from WMQuality.TestInitCouchApp import TestInitCouchApp

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
        self.testDir = self.testInit.generateWorkDir()
        return

    def tearDown(self):
        """
        _tearDown_

        Clear out the database.
        """
        self.testInit.clearDatabase()
        self.testInit.tearDownCouch()
        self.testInit.delWorkDir()
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
        newConfig["pset_tweak_details"] = {"process": {"outputModules_": ["outputRECORECO", "outputALCARECOALCARECO"],
                                                      "outputRECORECO": {"dataset": {"filterName": "FilterRECO",
                                                                                     "dataTier": "RECO"}},
                                                      "outputALCARECOALCARECO": {"dataset": {"filterName": "FilterALCARECO",
                                                                                             "dataTier": "ALCARECO"}}}}
        result = self.configDatabase.commitOne(newConfig)
        return result[0]["id"]

    def testMonteCarloFromGEN(self):
        """
        _testMonteCarloFromGEN_

        Create a MonteCarloFromGEN workflow and verify it installs into WMBS
        correctly.
        """
        arguments = MonteCarloFromGENWorkloadFactory.getTestArguments()
        arguments["ConfigCacheID"] = self.injectConfig()
        arguments["CouchDBName"] = "mclhe_t"
        arguments["PrimaryDataset"] = "WaitThisIsNotMinimumBias"

        factory = MonteCarloFromGENWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", arguments)

        self.assertEqual(len(testWorkload.listOutputDatasets()), 2)
        print testWorkload.listOutputDatasets()
        self.assertTrue("/WaitThisIsNotMinimumBias/None-FilterRECO-v0/RECO" in testWorkload.listOutputDatasets())
        self.assertTrue("/WaitThisIsNotMinimumBias/None-FilterALCARECO-v0/ALCARECO" in testWorkload.listOutputDatasets())

        testWMBSHelper = WMBSHelper(testWorkload, "MonteCarloFromGEN", "SomeBlock", cachepath = self.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        procWorkflow = Workflow(name = "TestWorkload",
                                task = "/TestWorkload/MonteCarloFromGEN")
        procWorkflow.load()

        self.assertEqual(len(procWorkflow.outputMap.keys()), 3,
                         "Error: Wrong number of WF outputs.")
        self.assertEqual(procWorkflow.wfType, 'production')

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

        self.assertEqual(procSubscription["type"], "Production",
                         "Error: Wrong subscription type: %s" % procSubscription["type"])
        self.assertEqual(procSubscription["split_algo"], "EventAwareLumiBased",
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
        self.assertEqual(mergeSubscription["split_algo"], "ParentlessMergeBySize",
                         "Error: Wrong split algo: %s" % mergeSubscription["split_algo"])

        unmergedAlca = Fileset(name = "/TestWorkload/MonteCarloFromGEN/unmerged-outputALCARECOALCARECO")
        unmergedAlca.loadData()
        alcaMergeWorkflow = Workflow(name = "TestWorkload",
                                     task = "/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeoutputALCARECOALCARECO")
        alcaMergeWorkflow.load()
        mergeSubscription = Subscription(fileset = unmergedAlca, workflow = alcaMergeWorkflow)
        mergeSubscription.loadData()

        self.assertEqual(mergeSubscription["type"], "Merge",
                         "Error: Wrong subscription type.")
        self.assertEqual(mergeSubscription["split_algo"], "ParentlessMergeBySize",
                         "Error: Wrong split algo: %s" % mergeSubscription["split_algo"])

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
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
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
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
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
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algo.")

        return

if __name__ == '__main__':
    unittest.main()
