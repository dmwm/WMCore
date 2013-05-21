#!/usr/bin/env python
"""
_StoreResults_t_

Unit tests for the StoreResults workflow.
"""

import unittest
import os
import threading

from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow

from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMCore.WMSpec.StdSpecs.StoreResults import getTestArguments, storeResultsWorkload

from WMQuality.TestInit import TestInit

class StoreResultsTest(unittest.TestCase):
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
        self.testDir = self.testInit.generateWorkDir()
        return

    def tearDown(self):
        """
        _tearDown_

        Clear out the database.
        """
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()
        return

    def testStoreResults(self):
        """
        _testStoreResults_

        Create a StoreResults workflow and verify it installs into WMBS
        correctly.
        """
        arguments = getTestArguments()
        arguments.update({'CmsPath' :"/uscmst1/prod/sw/cms"})
        testWorkload = storeResultsWorkload("TestWorkload", arguments)
        testWorkload.setSpecUrl("somespec")
        testWorkload.setOwnerDetails("ewv@fnal.gov", "DMWM")

        testWMBSHelper = WMBSHelper(testWorkload, "StoreResults", "SomeBlock", cachepath = self.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        testWorkflow = Workflow(name = "TestWorkload",
                                task = "/TestWorkload/StoreResults")
        testWorkflow.load()

        self.assertEqual(len(testWorkflow.outputMap.keys()), 2,
                         "Error: Wrong number of WF outputs.")

        goldenOutputMods = ["Merged"]
        for goldenOutputMod in goldenOutputMods:
            mergedOutput = testWorkflow.outputMap[goldenOutputMod][0]["merged_output_fileset"]
            unmergedOutput = testWorkflow.outputMap[goldenOutputMod][0]["output_fileset"]

            mergedOutput.loadData()
            unmergedOutput.loadData()

            self.assertEqual(mergedOutput.name, "/TestWorkload/StoreResults/merged-%s" % goldenOutputMod,
                             "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/StoreResults/merged-%s" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong: %s."%unmergedOutput.name)

        logArchOutput = testWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = testWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()


        self.assertEqual(logArchOutput.name, "/TestWorkload/StoreResults/merged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/StoreResults/merged-logArchive",
                         "Error: LogArchive output fileset is wrong.")


        topLevelFileset = Fileset(name = "TestWorkload-StoreResults-SomeBlock")
        topLevelFileset.loadData()

        procSubscription = Subscription(fileset = topLevelFileset, workflow = testWorkflow)
        procSubscription.loadData()

        self.assertEqual(procSubscription["type"], "Merge",
                         "Error: Wrong subscription type.")
        self.assertEqual(procSubscription["split_algo"], "ParentlessMergeBySize",
                         "Error: Wrong split algo.")


        return

if __name__ == '__main__':
    unittest.main()
