#!/usr/bin/env python
"""
_PrivateMC_t_

Unit tests for the PrivateMC workflow.
"""

import unittest
import os

from WMCore.Database.CMSCouch import CouchServer, Document
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMSpec.StdSpecs.PrivateMC import getTestArguments, PrivateMCWorkloadFactory
from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMQuality.TestInitCouchApp import TestInitCouchApp


class PrivateMCTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Initialize the database.
        """
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("privatemc_t", "ConfigCache")
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)

        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("privatemc_t")
        self.testDir = self.testInit.generateWorkDir()
        return

    def injectAnalysisConfig(self):
        """
        Create a bogus config cache document for the analysis workflow and
        inject it into couch.  Return the ID of the document.
        """

        newConfig = Document()
        newConfig["info"] = None
        newConfig["config"] = None
        newConfig["pset_hash"] = "21cb400c6ad63c3a97fa93f8e8785127"
        newConfig["owner"] = {"group": "Analysis", "user": "mmascher"}
        newConfig["pset_tweak_details"] ={"process": {"outputModules_": ["OutputA", "OutputB"],
                                                      "OutputA": {"dataset": {"filterName": "OutputAFilter",
                                                                              "dataTier": "RECO"}},
                                                      "OutputB": {"dataset": {"filterName": "OutputBFilter",
                                                                              "dataTier": "USER"}}}}

        result = self.configDatabase.commitOne(newConfig)
        return result[0]["id"]

    def tearDown(self):
        """
        _tearDown_

        Clear out the database.
        """
        self.testInit.tearDownCouch()
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()
        return

    def testPrivateMC(self):
        """
        _testAnalysis_
        """
        defaultArguments = getTestArguments()
        defaultArguments["CouchURL"] = os.environ["COUCHURL"]
        defaultArguments["CouchDBName"] = "privatemc_t"
        defaultArguments["AnalysisConfigCacheDoc"] = self.injectAnalysisConfig()
        defaultArguments["ProcessingVersion"] = 1

        processingFactory = PrivateMCWorkloadFactory()
        testWorkload = processingFactory("TestWorkload", defaultArguments)
        testWorkload.setSpecUrl("somespec")
        testWorkload.setOwnerDetails("marco.mascheroni@cern.ch", "DMWM")

        testWMBSHelper = WMBSHelper(testWorkload, "PrivateMC", "SomeBlock", cachepath = self.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper.createSubscription(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)
        procWorkflow = Workflow(name = "TestWorkload",
                              task = "/TestWorkload/PrivateMC")
        procWorkflow.load()
        self.assertEqual(len(procWorkflow.outputMap.keys()), 3,
                                  "Error: Wrong number of WF outputs: %s" % len(procWorkflow.outputMap.keys()))

        logArchOutput = procWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]#Actually Analysis does not have a merge task
        unmergedLogArchOutput = procWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()
        self.assertEqual(logArchOutput.name, "/TestWorkload/PrivateMC/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/PrivateMC/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        goldenOutputMods = ["OutputA", "OutputB"]
        for goldenOutputMod in goldenOutputMods:
            mergedOutput = procWorkflow.outputMap[goldenOutputMod][0]["merged_output_fileset"]
            unmergedOutput = procWorkflow.outputMap[goldenOutputMod][0]["output_fileset"]

            mergedOutput.loadData()
            unmergedOutput.loadData()

            self.assertEqual(mergedOutput.name, "/TestWorkload/PrivateMC/unmerged-%s" % goldenOutputMod,
                             "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/PrivateMC/unmerged-%s" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong.")

        topLevelFileset = Fileset(name = "TestWorkload-PrivateMC-SomeBlock")
        topLevelFileset.loadData()
        procSubscription = Subscription(fileset = topLevelFileset, workflow = procWorkflow)
        procSubscription.loadData()
        self.assertEqual(procSubscription["type"], "PrivateMC",
                         "Error: Wrong subscription type.")
        self.assertEqual(procSubscription["split_algo"], "EventBased",
                         "Error: Wrong split algo.")

        procLogCollect = Fileset(name = "/TestWorkload/PrivateMC/unmerged-logArchive")
        procLogCollect.loadData()
        procLogCollectWorkflow = Workflow(name = "TestWorkload",
                                          task = "/TestWorkload/PrivateMC/LogCollect")
        procLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset = procLogCollect, workflow = procLogCollectWorkflow)
        logCollectSub.loadData()
        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algo.")


if __name__ == '__main__':
    unittest.main()
