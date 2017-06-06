#!/usr/bin/env python
"""
_Repack_t_

Unit tests for the Tier0 Repack workflow.
"""

from __future__ import division

import unittest

from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow

from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMCore.WMSpec.StdSpecs.Repack import RepackWorkloadFactory

from WMQuality.TestInitCouchApp import TestInitCouchApp


class Repack(unittest.TestCase):
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
        testArguments['MaxSizeSingleLumi'] = 12 * 1024 * 1024 * 1024
        testArguments['MaxSizeMultiLumi'] = 8 * 1024 * 1024 * 1024
        testArguments['MinInputSize'] = 2.1 * 1024 * 1024 * 1024
        testArguments['MaxInputSize'] = 4 * 1024 * 1024 * 1024
        testArguments['MaxEdmSize'] = 12 * 1024 * 1024 * 1024
        testArguments['MaxOverSize'] = 8 * 1024 * 1024 * 1024
        testArguments['MaxInputEvents'] = 3 * 1000 * 1000
        testArguments['MaxInputFiles'] = 1000
        testArguments['MaxLatency'] = 24 * 3600
        testArguments['MinMergeSize'] = 2.1 * 1024 * 1024 * 1024
        testArguments['MaxMergeEvents'] = 3 * 1000 * 1000
        testArguments['RunNumber'] = 123456
        testArguments['AcquisitionEra'] = "TestAcquisitionEra"
        testArguments['ValidStatus'] = "VALID"

        testArguments['Outputs'] = []
        testArguments['Outputs'].append( { 'dataTier' : "RAW",
                                           'eventContent' : "All",
                                           'selectEvents' : ["Path1:HLT,Path2:HLT"],
                                           'primaryDataset' : "PrimaryDataset1" } )
        testArguments['Outputs'].append( { 'dataTier' : "RAW",
                                           'eventContent' : "All",
                                           'selectEvents' : ["Path3:HLT,Path4:HLT"],
                                           'primaryDataset' : "PrimaryDataset2" } )

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
        self.assertEqual(len(repackWorkflow.outputMap.keys()), len(testArguments["Outputs"]) + 1,
                         "Error: Wrong number of WF outputs in the Repack WF.")

        goldenOutputMods = ["write_PrimaryDataset1_RAW", "write_PrimaryDataset2_RAW"]
        for goldenOutputMod in goldenOutputMods:
            mergedOutput = repackWorkflow.outputMap[goldenOutputMod][0]["merged_output_fileset"]
            unmergedOutput = repackWorkflow.outputMap[goldenOutputMod][0]["output_fileset"]
            mergedOutput.loadData()
            unmergedOutput.loadData()

            if goldenOutputMod != "write_PrimaryDataset1_RAW":
                self.assertEqual(mergedOutput.name, "/TestWorkload/Repack/RepackMerge%s/merged-Merged" % goldenOutputMod,
                                 "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/Repack/unmerged-%s" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong: %s" % unmergedOutput.name)

        logArchOutput = repackWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
        unmergedLogArchOutput = repackWorkflow.outputMap["logArchive"][0]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()

        self.assertEqual(logArchOutput.name, "/TestWorkload/Repack/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Repack/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        goldenOutputMods = ["write_PrimaryDataset1_RAW", "write_PrimaryDataset2_RAW"]
        for goldenOutputMod in goldenOutputMods:
            mergeWorkflow = Workflow(name="TestWorkload",
                                     task="/TestWorkload/Repack/RepackMerge%s" % goldenOutputMod)
            mergeWorkflow.load()

            self.assertEqual(len(mergeWorkflow.outputMap.keys()), 3,
                             "Error: Wrong number of WF outputs.")

            mergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["merged_output_fileset"]
            unmergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["output_fileset"]

            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()

            self.assertEqual(mergedMergeOutput.name, "/TestWorkload/Repack/RepackMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Merged output fileset is wrong.")
            self.assertEqual(unmergedMergeOutput.name, "/TestWorkload/Repack/RepackMerge%s/merged-Merged" % goldenOutputMod,
                             "Error: Unmerged output fileset is wrong.")

            logArchOutput = mergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = mergeWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()

            self.assertEqual(logArchOutput.name, "/TestWorkload/Repack/RepackMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
            self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Repack/RepackMerge%s/merged-logArchive" % goldenOutputMod,
                             "Error: LogArchive output fileset is wrong.")

        topLevelFileset = Fileset(name="TestWorkload-Repack")
        topLevelFileset.loadData()

        repackSubscription = Subscription(fileset=topLevelFileset, workflow=repackWorkflow)
        repackSubscription.loadData()

        self.assertEqual(repackSubscription["type"], "Repack",
                         "Error: Wrong subscription type.")
        self.assertEqual(repackSubscription["split_algo"], "Repack",
                         "Error: Wrong split algorithm. %s" % repackSubscription["split_algo"])

        unmergedOutputs = ["write_PrimaryDataset1_RAW", "write_PrimaryDataset2_RAW"]
        for unmergedOutput in unmergedOutputs:
            unmergedDataTier = Fileset(name="/TestWorkload/Repack/unmerged-%s" % unmergedOutput)
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

        goldenOutputMods = ["write_PrimaryDataset1_RAW", "write_PrimaryDataset2_RAW"]
        for goldenOutputMod in goldenOutputMods:
            unmergedFileset = Fileset(name="/TestWorkload/Repack/unmerged-%s" % goldenOutputMod)
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

        goldenOutputMods = ["write_PrimaryDataset1_RAW", "write_PrimaryDataset2_RAW"]
        for goldenOutputMod in goldenOutputMods:
            repackMergeLogCollect = Fileset(name="/TestWorkload/Repack/RepackMerge%s/merged-logArchive" % goldenOutputMod)
            repackMergeLogCollect.loadData()
            repackMergeLogCollectWorkflow = Workflow(name="TestWorkload",
                                                     task="/TestWorkload/Repack/RepackMerge%s/Repack%sMergeLogCollect" % (goldenOutputMod, goldenOutputMod))
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
        testArguments['MaxSizeSingleLumi'] = 12 * 1024 * 1024 * 1024
        testArguments['MaxSizeMultiLumi'] = 8 * 1024 * 1024 * 1024
        testArguments['MinInputSize'] = 2.1 * 1024 * 1024 * 1024
        testArguments['MaxInputSize'] = 4 * 1024 * 1024 * 1024
        testArguments['MaxEdmSize'] = 12 * 1024 * 1024 * 1024
        testArguments['MaxOverSize'] = 8 * 1024 * 1024 * 1024
        testArguments['MaxInputEvents'] = 3 * 1000 * 1000
        testArguments['MaxInputFiles'] = 1000
        testArguments['MaxLatency'] = 24 * 3600
        testArguments['MinMergeSize'] = 2.1 * 1024 * 1024 * 1024
        testArguments['MaxMergeEvents'] = 3 * 1000 * 1000
        testArguments['RunNumber'] = 123456
        testArguments['AcquisitionEra'] = "TestAcquisitionEra"
        testArguments['ValidStatus'] = "VALID"

        testArguments['Outputs'] = []
        testArguments['Outputs'].append( { 'dataTier' : "RAW",
                                           'eventContent' : "All",
                                           'selectEvents' : ["Path1:HLT,Path2:HLT"],
                                           'primaryDataset' : "PrimaryDataset1" } )
        testArguments['Outputs'].append( { 'dataTier' : "RAW",
                                           'eventContent' : "All",
                                           'selectEvents' : ["Path3:HLT,Path4:HLT"],
                                           'primaryDataset' : "PrimaryDataset2" } )

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
        testArguments = RepackWorkloadFactory.getTestArguments()
        testArguments['MaxSizeSingleLumi'] = 12 * 1024 * 1024 * 1024
        testArguments['MaxSizeMultiLumi'] = 8 * 1024 * 1024 * 1024
        testArguments['MinInputSize'] = 2.1 * 1024 * 1024 * 1024
        testArguments['MaxInputSize'] = 4 * 1024 * 1024 * 1024
        testArguments['MaxEdmSize'] = 12 * 1024 * 1024 * 1024
        testArguments['MaxOverSize'] = 8 * 1024 * 1024 * 1024
        testArguments['MaxInputEvents'] = 3 * 1000 * 1000
        testArguments['MaxInputFiles'] = 1000
        testArguments['MaxLatency'] = 24 * 3600
        testArguments['MinMergeSize'] = 2.1 * 1024 * 1024 * 1024
        testArguments['MaxMergeEvents'] = 3 * 1000 * 1000
        testArguments['RunNumber'] = 123456
        testArguments['AcquisitionEra'] = "TestAcquisitionEra"
        testArguments['ValidStatus'] = "VALID"

        testArguments['Outputs'] = []
        testArguments['Outputs'].append( { 'dataTier' : "RAW",
                                           'eventContent' : "All",
                                           'selectEvents' : ["Path1:HLT,Path2:HLT"],
                                           'primaryDataset' : "PrimaryDataset1" } )
        testArguments['Outputs'].append( { 'dataTier' : "RAW",
                                           'eventContent' : "All",
                                           'selectEvents' : ["Path3:HLT,Path4:HLT"],
                                           'primaryDataset' : "PrimaryDataset2" } )

        testArguments["Multicore"] = 6
        testArguments["Memory"] = 4600.0
        testArguments["EventStreams"] = 3

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


if __name__ == '__main__':
    unittest.main()
