#!/usr/bin/env python
"""
_Repack_t_

Unit tests for the Tier0 Repack workflow.
"""

from __future__ import division, print_function
from future.utils import viewitems

import threading
import unittest
from copy import deepcopy

from Utils.PythonVersion import PY3

from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMSpec.StdSpecs.Repack import RepackWorkloadFactory
from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMQuality.TestInitCouchApp import TestInitCouchApp

REQUEST = {
    'MaxSizeSingleLumi': 12 * 1024 * 1024 * 1024,
    'MaxSizeMultiLumi': 8 * 1024 * 1024 * 1024,
    'MinInputSize': 2.1 * 1024 * 1024 * 1024,
    'MaxInputSize': 4 * 1024 * 1024 * 1024,
    'MaxEdmSize': 12 * 1024 * 1024 * 1024,
    'MaxOverSize': 8 * 1024 * 1024 * 1024,
    'MaxInputEvents': 3 * 1000 * 1000,
    'MaxInputFiles': 1000,
    'MaxLatency': 24 * 3600,
    'MinMergeSize': 2.1 * 1024 * 1024 * 1024,
    'MaxMergeEvents': 3 * 1000 * 1000,
    'RunNumber': 123456,
    'AcquisitionEra': "TestAcquisitionEra",
    'ValidStatus': "VALID",
    'Outputs': [{'dataTier': "RAW",
                 'eventContent': "All",
                 'selectEvents': ["Path1:HLT,Path2:HLT"],
                 'primaryDataset': "PrimaryDataset1"},
                {'dataTier': "RAW",
                 'eventContent': "All",
                 'selectEvents': ["Path3:HLT,Path4:HLT"],
                 'primaryDataset': "PrimaryDataset2"}]
}


class RepackTests(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Initialize the database and couch.
        """
        self.testInit = TestInitCouchApp(__file__)
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

    def testRepack(self):
        """
        _testRepack_

        Create a Repack workflow
        and verify it installs into WMBS correctly.
        """
        testArguments = RepackWorkloadFactory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))

        factory = RepackWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)
        testWorkload.setSpecUrl("somespec")
        testWorkload.setOwnerDetails("Dirk.Hufnagel@cern.ch", "T0")

        testWMBSHelper = WMBSHelper(testWorkload, "Repack", cachepath=self.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        repackWorkflow = Workflow(name="TestWorkload",
                                  task="/TestWorkload/Repack")
        repackWorkflow.load()
        self.assertEqual(len(repackWorkflow.outputMap), len(testArguments["Outputs"]) + 1,
                         "Error: Wrong number of WF outputs in the Repack WF.")

        goldenOutputMods = {"write_PrimaryDataset1_RAW": "RAW", "write_PrimaryDataset2_RAW": "RAW"}
        for goldenOutputMod, tier in viewitems(goldenOutputMods):
            fset = goldenOutputMod + tier
            mergedOutput = repackWorkflow.outputMap[fset][0]["merged_output_fileset"]
            unmergedOutput = repackWorkflow.outputMap[fset][0]["output_fileset"]
            mergedOutput.loadData()
            unmergedOutput.loadData()

            if goldenOutputMod != "write_PrimaryDataset1_RAW":
                self.assertEqual(mergedOutput.name,
                                 "/TestWorkload/Repack/RepackMerge%s/merged-Merged%s" % (goldenOutputMod, tier),
                                 "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/Repack/unmerged-%s" % fset,
                             "Error: Unmerged output fileset is wrong: %s" % unmergedOutput.name)

        logArchOutput = repackWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = repackWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name, "/TestWorkload/Repack/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Repack/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        for goldenOutputMod, tier in viewitems(goldenOutputMods):
            mergeWorkflow = Workflow(name="TestWorkload",
                                     task="/TestWorkload/Repack/RepackMerge%s" % goldenOutputMod)
            mergeWorkflow.load()

            self.assertEqual(len(mergeWorkflow.outputMap), 3,
                             "Error: Wrong number of WF outputs.")

            mergedMergeOutput = mergeWorkflow.outputMap["Merged%s" % tier][0]["merged_output_fileset"]
            unmergedMergeOutput = mergeWorkflow.outputMap["Merged%s" % tier][0]["output_fileset"]

            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()

            self.assertEqual(mergedMergeOutput.name,
                             "/TestWorkload/Repack/RepackMerge%s/merged-Merged%s" % (goldenOutputMod, tier),
                             "Error: Merged output fileset is wrong.")
            self.assertEqual(unmergedMergeOutput.name,
                             "/TestWorkload/Repack/RepackMerge%s/merged-Merged%s" % (goldenOutputMod, tier),
                             "Error: Unmerged output fileset is wrong.")

            logArchOutput = mergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = mergeWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()

            self.assertEqual(logArchOutput.name,
                             "/TestWorkload/Repack/RepackMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
            self.assertEqual(unmergedLogArchOutput.name,
                             "/TestWorkload/Repack/RepackMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")

        topLevelFileset = Fileset(name="TestWorkload-Repack")
        topLevelFileset.loadData()

        repackSubscription = Subscription(fileset=topLevelFileset, workflow=repackWorkflow)
        repackSubscription.loadData()

        self.assertEqual(repackSubscription["type"], "Repack",
                         "Error: Wrong subscription type.")
        self.assertEqual(repackSubscription["split_algo"], "Repack",
                         "Error: Wrong split algorithm. %s" % repackSubscription["split_algo"])

        unmergedOutputs = {"write_PrimaryDataset1_RAW": "RAW", "write_PrimaryDataset2_RAW": "RAW"}
        for unmergedOutput, tier in viewitems(unmergedOutputs):
            fset = unmergedOutput + tier
            unmergedDataTier = Fileset(name="/TestWorkload/Repack/unmerged-%s" % fset)
            unmergedDataTier.loadData()
            dataTierMergeWorkflow = Workflow(name="TestWorkload",
                                             task="/TestWorkload/Repack/RepackMerge%s" % unmergedOutput)
            dataTierMergeWorkflow.load()
            mergeSubscription = Subscription(fileset=unmergedDataTier, workflow=dataTierMergeWorkflow)
            mergeSubscription.loadData()

            self.assertEqual(mergeSubscription["type"], "Merge",
                             "Error: Wrong subscription type.")
            self.assertEqual(mergeSubscription["split_algo"], "RepackMerge",
                             "Error: Wrong split algorithm. %s" % mergeSubscription["split_algo"])

        for goldenOutputMod, tier in viewitems(goldenOutputMods):
            fset = goldenOutputMod + tier
            unmergedFileset = Fileset(name="/TestWorkload/Repack/unmerged-%s" % fset)
            unmergedFileset.loadData()
            cleanupWorkflow = Workflow(name="TestWorkload",
                                       task="/TestWorkload/Repack/RepackCleanupUnmerged%s" % goldenOutputMod)
            cleanupWorkflow.load()
            cleanupSubscription = Subscription(fileset=unmergedFileset, workflow=cleanupWorkflow)
            cleanupSubscription.loadData()

            self.assertEqual(cleanupSubscription["type"], "Cleanup",
                             "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong subscription type.")

        repackLogCollect = Fileset(name="/TestWorkload/Repack/unmerged-logArchive")
        repackLogCollect.loadData()
        repackLogCollectWorkflow = Workflow(name="TestWorkload",
                                            task="/TestWorkload/Repack/LogCollect")
        repackLogCollectWorkflow.load()
        logCollectSub = Subscription(fileset=repackLogCollect, workflow=repackLogCollectWorkflow)
        logCollectSub.loadData()

        self.assertEqual(logCollectSub["type"], "LogCollect",
                         "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                         "Error: Wrong split algorithm.")

        for goldenOutputMod, tier in viewitems(goldenOutputMods):
            repackMergeLogCollect = Fileset(
                name="/TestWorkload/Repack/RepackMerge%s/merged-logArchive" % goldenOutputMod)
            repackMergeLogCollect.loadData()
            repackMergeLogCollectWorkflow = Workflow(name="TestWorkload",
                                                     task="/TestWorkload/Repack/RepackMerge%s/Repack%sMergeLogCollect" % (
                                                         goldenOutputMod, goldenOutputMod))
            repackMergeLogCollectWorkflow.load()
            logCollectSubscription = Subscription(fileset=repackMergeLogCollect, workflow=repackMergeLogCollectWorkflow)
            logCollectSubscription.loadData()

            self.assertEqual(logCollectSub["type"], "LogCollect",
                             "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                             "Error: Wrong split algorithm.")

        return

    def testMemCoresSettings(self):
        """
        _testMemCoresSettings_
        
        Make sure the multicore and memory setings are properly propagated to
        all tasks and steps.
        """
        testArguments = RepackWorkloadFactory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))

        factory = RepackWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        # test default values
        taskPaths = ['/TestWorkload/Repack']
        for task in taskPaths:
            taskObj = testWorkload.getTaskByPath(task)
            for step in ('cmsRun1', 'stageOut1', 'logArch1'):
                stepHelper = taskObj.getStepHelper(step)
                self.assertEqual(stepHelper.getNumberOfCores(), 1)
                self.assertEqual(stepHelper.getNumberOfStreams(), 0)
            # then test Memory requirements
            perfParams = taskObj.jobSplittingParameters()['performance']
            self.assertEqual(perfParams['memoryRequirement'], 2300.0)

        # now test case where args are provided
        testArguments["Multicore"] = 6
        testArguments["Memory"] = 4600.0
        testArguments["EventStreams"] = 3
        testArguments["Outputs"] = deepcopy(REQUEST['Outputs'])

        factory = RepackWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        for task in taskPaths:
            taskObj = testWorkload.getTaskByPath(task)
            for step in ('cmsRun1', 'stageOut1', 'logArch1'):
                stepHelper = taskObj.getStepHelper(step)
                if task == '/TestWorkload/Repack' and step == 'cmsRun1':
                    self.assertEqual(stepHelper.getNumberOfCores(), testArguments["Multicore"])
                    self.assertEqual(stepHelper.getNumberOfStreams(), testArguments["EventStreams"])
                elif step in ('stageOut1', 'logArch1'):
                    self.assertEqual(stepHelper.getNumberOfCores(), 1)
                    self.assertEqual(stepHelper.getNumberOfStreams(), 0)
                else:
                    self.assertEqual(stepHelper.getNumberOfCores(), 1, "%s should be single-core" % task)
                    self.assertEqual(stepHelper.getNumberOfStreams(), 0)
            # then test Memory requirements
            perfParams = taskObj.jobSplittingParameters()['performance']
            self.assertEqual(perfParams['memoryRequirement'], testArguments["Memory"])

        return

    def testFilesets(self):
        """
        Test workflow tasks, filesets and subscriptions creation
        """
        # expected tasks, filesets, subscriptions, etc
        expOutTasks = ['/TestWorkload/Repack',
                       '/TestWorkload/Repack/RepackMergewrite_PrimaryDataset1_RAW',
                       '/TestWorkload/Repack/RepackMergewrite_PrimaryDataset2_RAW']
        expWfTasks = ['/TestWorkload/Repack',
                      '/TestWorkload/Repack/LogCollect',
                      '/TestWorkload/Repack/RepackCleanupUnmergedwrite_PrimaryDataset1_RAW',
                      '/TestWorkload/Repack/RepackCleanupUnmergedwrite_PrimaryDataset2_RAW',
                      '/TestWorkload/Repack/RepackMergewrite_PrimaryDataset1_RAW',
                      '/TestWorkload/Repack/RepackMergewrite_PrimaryDataset1_RAW/Repackwrite_PrimaryDataset1_RAWMergeLogCollect',
                      '/TestWorkload/Repack/RepackMergewrite_PrimaryDataset2_RAW',
                      '/TestWorkload/Repack/RepackMergewrite_PrimaryDataset2_RAW/Repackwrite_PrimaryDataset2_RAWMergeLogCollect']
        expFsets = ['TestWorkload-Repack-StreamerFiles',
                    '/TestWorkload/Repack/RepackMergewrite_PrimaryDataset1_RAW/merged-logArchive',
                    '/TestWorkload/Repack/RepackMergewrite_PrimaryDataset1_RAW/merged-MergedRAW',
                    '/TestWorkload/Repack/RepackMergewrite_PrimaryDataset1_RAW/merged-MergedErrorRAW',
                    '/TestWorkload/Repack/RepackMergewrite_PrimaryDataset2_RAW/merged-logArchive',
                    '/TestWorkload/Repack/RepackMergewrite_PrimaryDataset2_RAW/merged-MergedRAW',
                    '/TestWorkload/Repack/RepackMergewrite_PrimaryDataset2_RAW/merged-MergedErrorRAW',
                    '/TestWorkload/Repack/unmerged-write_PrimaryDataset1_RAWRAW',
                    '/TestWorkload/Repack/unmerged-write_PrimaryDataset2_RAWRAW',
                    '/TestWorkload/Repack/unmerged-logArchive']
        subMaps = [(4,
                    '/TestWorkload/Repack/RepackMergewrite_PrimaryDataset1_RAW/merged-logArchive',
                    '/TestWorkload/Repack/RepackMergewrite_PrimaryDataset1_RAW/Repackwrite_PrimaryDataset1_RAWMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (7,
                    '/TestWorkload/Repack/RepackMergewrite_PrimaryDataset2_RAW/merged-logArchive',
                    '/TestWorkload/Repack/RepackMergewrite_PrimaryDataset2_RAW/Repackwrite_PrimaryDataset2_RAWMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (8,
                    '/TestWorkload/Repack/unmerged-logArchive',
                    '/TestWorkload/Repack/LogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (2,
                    '/TestWorkload/Repack/unmerged-write_PrimaryDataset1_RAWRAW',
                    '/TestWorkload/Repack/RepackCleanupUnmergedwrite_PrimaryDataset1_RAW',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (3,
                    '/TestWorkload/Repack/unmerged-write_PrimaryDataset1_RAWRAW',
                    '/TestWorkload/Repack/RepackMergewrite_PrimaryDataset1_RAW',
                    'RepackMerge',
                    'Merge'),
                   (5,
                    '/TestWorkload/Repack/unmerged-write_PrimaryDataset2_RAWRAW',
                    '/TestWorkload/Repack/RepackCleanupUnmergedwrite_PrimaryDataset2_RAW',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (6,
                    '/TestWorkload/Repack/unmerged-write_PrimaryDataset2_RAWRAW',
                    '/TestWorkload/Repack/RepackMergewrite_PrimaryDataset2_RAW',
                    'RepackMerge',
                    'Merge'),
                   (1,
                    'TestWorkload-Repack-StreamerFiles',
                    '/TestWorkload/Repack',
                    'Repack',
                    'Repack')]

        testArguments = RepackWorkloadFactory.getTestArguments()
        testArguments.update(deepcopy(REQUEST))

        factory = RepackWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        testWMBSHelper = WMBSHelper(testWorkload, "Repack", blockName='StreamerFiles',
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
