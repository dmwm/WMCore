#!/usr/bin/env python
"""
_StoreResults_t_

Unit tests for the StoreResults workflow.
"""
from __future__ import print_function
from future.utils import viewitems

import threading
import unittest

from Utils.PythonVersion import PY3

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
        if PY3:
            self.assertItemsEqual = self.assertCountEqual
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

        factory = StoreResultsWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", arguments)

        testWMBSHelper = WMBSHelper(testWorkload, "StoreResults", "SomeBlock", cachepath=self.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        testWorkflow = Workflow(name="TestWorkload",
                                task="/TestWorkload/StoreResults")
        testWorkflow.load()

        self.assertEqual(len(testWorkflow.outputMap), 2,
                         "Error: Wrong number of WF outputs.")
        goldenOutputMods = {"Merged": "USER"}
        for goldenOutputMod, tier in viewitems(goldenOutputMods):
            fset = goldenOutputMod + tier
            mergedOutput = testWorkflow.outputMap[fset][0]["merged_output_fileset"]
            unmergedOutput = testWorkflow.outputMap[fset][0]["output_fileset"]

            mergedOutput.loadData()
            unmergedOutput.loadData()

            self.assertEqual(mergedOutput.name, "/TestWorkload/StoreResults/merged-%s" % fset,
                             "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/StoreResults/merged-%s" % fset,
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
                    '/TestWorkload/StoreResults/merged-MergedUSER',
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

        factory = StoreResultsWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        testWMBSHelper = WMBSHelper(testWorkload, "StoreResults", blockName=testArguments['InputDataset'],
                                    cachepath=self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        self.assertItemsEqual(testWorkload.listOutputProducingTasks(), expOutTasks)

        workflows = self.listTasksByWorkflow.execute(workflow="TestWorkload")
        self.assertItemsEqual([item['task'] for item in workflows], expWfTasks)

        # returns a tuple of id, name, open and last_update
        filesets = self.listFilesets.execute()
        self.assertItemsEqual([item[1] for item in filesets], expFsets)

        subscriptions = self.listSubsMapping.execute(workflow="TestWorkload", returnTuple=True)
        self.assertItemsEqual(subscriptions, subMaps)


if __name__ == '__main__':
    unittest.main()
