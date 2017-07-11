#!/usr/bin/env python
"""
_StoreResults_t_

Unit tests for the StoreResults workflow.
"""
from __future__ import print_function

import threading
import unittest
from pprint import pformat

from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMSpec.StdSpecs.StoreResults import StoreResultsWorkloadFactory
from WMCore.WorkQueue.WMBSHelper import WMBSHelper
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
        self.testInit.setSchema(customModules=["WMCore.WMBS"],
                                useDefault=False)
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
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()
        return

    def testStoreResults(self):
        """
        _testStoreResults_

        Create a StoreResults workflow and verify it installs into WMBS
        correctly.
        """
        arguments = StoreResultsWorkloadFactory.getTestArguments()
        arguments.update({'CmsPath': "/uscmst1/prod/sw/cms"})

        factory = StoreResultsWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", arguments)

        testWMBSHelper = WMBSHelper(testWorkload, "StoreResults", "SomeBlock", cachepath=self.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        testWorkflow = Workflow(name="TestWorkload",
                                task="/TestWorkload/StoreResults")
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
                             "Error: Unmerged output fileset is wrong: %s." % unmergedOutput.name)

        logArchOutput = testWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = testWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name, "/TestWorkload/StoreResults/merged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/StoreResults/merged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        topLevelFileset = Fileset(name="TestWorkload-StoreResults-SomeBlock")
        topLevelFileset.loadData()

        procSubscription = Subscription(fileset=topLevelFileset, workflow=testWorkflow)
        procSubscription.loadData()

        self.assertEqual(procSubscription["type"], "Merge",
                         "Error: Wrong subscription type.")
        self.assertEqual(procSubscription["split_algo"], "ParentlessMergeBySize",
                         "Error: Wrong split algo.")

        return

    def testFilesets(self):
        """
        Test workflow tasks, filesets and subscriptions creation
        """
        # expected tasks, filesets, subscriptions, etc
        expOutTasks = ['/TestWorkload/StoreResults']
        expWfTasks = ['/TestWorkload/StoreResults',
                      '/TestWorkload/StoreResults/StoreResultsLogCollect']
        expFsets = ['TestWorkload-StoreResults-/MinimumBias/ComissioningHI-v1/RAW',
                    '/TestWorkload/StoreResults/merged-Merged',
                    '/TestWorkload/StoreResults/merged-logArchive']
        subMaps = [(2,
                    '/TestWorkload/StoreResults/merged-logArchive',
                    '/TestWorkload/StoreResults/StoreResultsLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (1,
                    'TestWorkload-StoreResults-/MinimumBias/ComissioningHI-v1/RAW',
                    '/TestWorkload/StoreResults',
                    'ParentlessMergeBySize',
                    'Merge')]

        testArguments = StoreResultsWorkloadFactory.getTestArguments()
        testArguments.update({'CmsPath': "/uscmst1/prod/sw/cms"})

        factory = StoreResultsWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        testWMBSHelper = WMBSHelper(testWorkload, "StoreResults", blockName=testArguments['InputDataset'],
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
