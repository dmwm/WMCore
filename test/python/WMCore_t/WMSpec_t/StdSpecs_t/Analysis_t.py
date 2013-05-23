#!/usr/bin/env python
"""
_Analysis_t_

Unit tests for the analysis workflow.
"""

import unittest
import os

from WMCore.WMSpec.StdSpecs.Analysis import getTestArguments, AnalysisWorkloadFactory

from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.Database.CMSCouch import CouchServer, Document

from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow


class AnalysisTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Initialize the database.
        """
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("analysis_t", "ConfigCache")
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)

        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("analysis_t")
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
        newConfig["pset_tweak_details"] = {
                                           "process": {
                                                       "maxEvents": {"parameters_": ["input"], "input": 10},
                                                       "outputModules_": ["output"],
                                                       "parameters_": ["outputModules_"],
                                                       "source": {"parameters_": ["fileNames"], "fileNames": []},
                                                       "output": {"parameters_": ["fileName"], "fileName": "outfile.root"},
                                                       "options": {"parameters_": ["wantSummary"], "wantSummary": True}
                                                      }
        }
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


    def testAnalysis(self):
        """
        _testAnalysis_
        """
        defaultArguments = getTestArguments()
        defaultArguments["CouchUrl"] = os.environ["COUCHURL"]
        defaultArguments["CouchDBName"] = "analysis_t"
        defaultArguments["AnalysisConfigCacheDoc"] = self.injectAnalysisConfig()
        defaultArguments["ProcessingVersion"] = 1

        analysisProcessingFactory = AnalysisWorkloadFactory()
        testWorkload = analysisProcessingFactory("TestWorkload", defaultArguments)
        testWorkload.setSpecUrl("somespec")
        testWorkload.setOwnerDetails("marco.mascheroni@cern.ch", "DMWM")

        testWMBSHelper = WMBSHelper(testWorkload, "Analysis", "SomeBlock", cachepath = self.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)
        procWorkflow = Workflow(name = "TestWorkload",
                              task = "/TestWorkload/Analysis")
        procWorkflow.load()
        self.assertEqual(len(procWorkflow.outputMap.keys()), 2,
                                  "Error: Wrong number of WF outputs.")

        logArchOutput = procWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]#Actually Analysis does not have a merge task
        unmergedLogArchOutput = procWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()
        self.assertEqual(logArchOutput.name, "/TestWorkload/Analysis/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Analysis/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        output = procWorkflow.outputMap["output"][0]["output_fileset"]
        mergedOutput = procWorkflow.outputMap["output"][0]["merged_output_fileset"]
        output.loadData()
        mergedOutput.loadData()
        self.assertEqual(output.name, "/TestWorkload/Analysis/unmerged-output",
                             "Error: Unmerged output fileset is wrong: " + output.name)
        self.assertEqual(mergedOutput.name, "/TestWorkload/Analysis/unmerged-output",
                             "Error: Unmerged output fileset is wrong: " + mergedOutput.name)

        topLevelFileset = Fileset(name = "TestWorkload-Analysis-SomeBlock")
        topLevelFileset.loadData()
        procSubscription = Subscription(fileset = topLevelFileset, workflow = procWorkflow)
        procSubscription.loadData()
        self.assertEqual(procSubscription["type"], "Analysis",
                         "Error: Wrong subscription type.")
        self.assertEqual(procSubscription["split_algo"], "EventBased",
                         "Error: Wrong split algo.")


        procLogCollect = Fileset(name = "/TestWorkload/Analysis/unmerged-logArchive")
        procLogCollect.loadData()
        procLogCollectWorkflow = Workflow(name = "TestWorkload",
                                          task = "/TestWorkload/Analysis/LogCollect")
        procLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset = procLogCollect, workflow = procLogCollectWorkflow)
        logCollectSub.loadData()
        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algo.")


if __name__ == '__main__':
    unittest.main()
