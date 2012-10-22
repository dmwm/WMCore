#!/usr/bin/env python
"""
_DataProcessing_t_

Unit tests for the data processing workflow.
"""

import unittest
import os
import threading

from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow

from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMCore.WMSpec.StdSpecs.DataProcessing import getTestArguments, dataProcessingWorkload

from WMQuality.TestInit import TestInit

class DataProcessingTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Initialize the database.
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        return

    def tearDown(self):
        """
        _tearDown_

        Clear out the database.
        """
        self.testInit.clearDatabase()
        return

    def testDataProcessing(self):
        """
        _testDataProcessing_

        Create a data processing workflow and verify it installs into WMBS
        correctly.
        """
        testWorkload = dataProcessingWorkload("TestWorkload", getTestArguments())
        testWorkload.setSpecUrl("somespec")
        testWorkload.setOwnerDetails("sfoulkes@fnal.gov", "DMWM")

        testWMBSHelper = WMBSHelper(testWorkload, "DataProcessing", "SomeBlock")
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper.createSubscription(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        procWorkflow = Workflow(name = "TestWorkload",
                                task = "/TestWorkload/DataProcessing")
        procWorkflow.load()

        self.assertEqual(len(procWorkflow.outputMap.keys()), 3,
                         "Error: Wrong number of WF outputs.")

        goldenOutputMods = ["RECOoutput", "ALCARECOoutput"]
        for goldenOutputMod in goldenOutputMods:
            mergedOutput = procWorkflow.outputMap[goldenOutputMod][0]["merged_output_fileset"]
            unmergedOutput = procWorkflow.outputMap[goldenOutputMod][0]["output_fileset"]

            mergedOutput.loadData()
            unmergedOutput.loadData()

            self.assertEqual(mergedOutput.name, "/TestWorkload/DataProcessing/DataProcessingMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/DataProcessing/unmerged-%s" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong.")

        logArchOutput = procWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = procWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name, "/TestWorkload/DataProcessing/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/DataProcessing/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        for goldenOutputMod in goldenOutputMods:
            mergeWorkflow = Workflow(name = "TestWorkload",
                                     task = "/TestWorkload/DataProcessing/DataProcessingMerge%s" % goldenOutputMod)
            mergeWorkflow.load()

            self.assertEqual(len(mergeWorkflow.outputMap.keys()), 2,
                             "Error: Wrong number of WF outputs.")

            mergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["merged_output_fileset"]
            unmergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["output_fileset"]

            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()

            self.assertEqual(mergedMergeOutput.name, "/TestWorkload/DataProcessing/DataProcessingMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong.")
            self.assertEqual(unmergedMergeOutput.name, "/TestWorkload/DataProcessing/DataProcessingMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong.")

            logArchOutput = mergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = mergeWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()

            self.assertEqual(logArchOutput.name, "/TestWorkload/DataProcessing/DataProcessingMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
            self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/DataProcessing/DataProcessingMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")

        topLevelFileset = Fileset(name = "TestWorkload-DataProcessing-SomeBlock")
        topLevelFileset.loadData()

        procSubscription = Subscription(fileset = topLevelFileset, workflow = procWorkflow)
        procSubscription.loadData()

        self.assertEqual(procSubscription["type"], "Processing",
                         "Error: Wrong subscription type.")
        self.assertEqual(procSubscription["split_algo"], "LumiBased",
                         "Error: Wrong split algo.")

        unmergedReco = Fileset(name = "/TestWorkload/DataProcessing/unmerged-RECOoutput")
        unmergedReco.loadData()
        recoMergeWorkflow = Workflow(name = "TestWorkload",
                                     task = "/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput")
        recoMergeWorkflow.load()
        mergeSubscription = Subscription(fileset = unmergedReco, workflow = recoMergeWorkflow)
        mergeSubscription.loadData()

        self.assertEqual(mergeSubscription["type"], "Merge",
                         "Error: Wrong subscription type.")
        self.assertEqual(mergeSubscription["split_algo"], "ParentlessMergeBySize",
                         "Error: Wrong split algo.")

        unmergedAlca = Fileset(name = "/TestWorkload/DataProcessing/unmerged-ALCARECOoutput")
        unmergedAlca.loadData()
        alcaMergeWorkflow = Workflow(name = "TestWorkload",
                                     task = "/TestWorkload/DataProcessing/DataProcessingMergeALCARECOoutput")
        alcaMergeWorkflow.load()
        mergeSubscription = Subscription(fileset = unmergedAlca, workflow = alcaMergeWorkflow)
        mergeSubscription.loadData()

        self.assertEqual(mergeSubscription["type"], "Merge",
                         "Error: Wrong subscription type.")
        self.assertEqual(mergeSubscription["split_algo"], "ParentlessMergeBySize",
                         "Error: Wrong split algo.")

        for procOutput in ["RECOoutput", "ALCARECOoutput"]:
            unmerged = Fileset(name = "/TestWorkload/DataProcessing/unmerged-%s" % procOutput)
            unmerged.loadData()
            cleanupWorkflow = Workflow(name = "TestWorkload",
                                      task = "/TestWorkload/DataProcessing/DataProcessingCleanupUnmerged%s" % procOutput)
            cleanupWorkflow.load()
            cleanupSubscription = Subscription(fileset = unmerged, workflow = cleanupWorkflow)
            cleanupSubscription.loadData()

            self.assertEqual(cleanupSubscription["type"], "Cleanup",
                             "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong split algo.")

        procLogCollect = Fileset(name = "/TestWorkload/DataProcessing/unmerged-logArchive")
        procLogCollect.loadData()
        procLogCollectWorkflow = Workflow(name = "TestWorkload",
                                          task = "/TestWorkload/DataProcessing/LogCollect")
        procLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset = procLogCollect, workflow = procLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algo.")

        procLogCollect = Fileset(name = "/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/merged-logArchive")
        procLogCollect.loadData()
        procLogCollectWorkflow = Workflow(name = "TestWorkload",
                                          task = "/TestWorkload/DataProcessing/DataProcessingMergeRECOoutput/DataProcessingRECOoutputMergeLogCollect")
        procLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset = procLogCollect, workflow = procLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algo.")

        procLogCollect = Fileset(name = "/TestWorkload/DataProcessing/DataProcessingMergeALCARECOoutput/merged-logArchive")
        procLogCollect.loadData()
        procLogCollectWorkflow = Workflow(name = "TestWorkload",
                                          task = "/TestWorkload/DataProcessing/DataProcessingMergeALCARECOoutput/DataProcessingALCARECOoutputMergeLogCollect")
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
