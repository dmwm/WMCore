#!/usr/bin/env python
"""
_WMWorkload_t_

Unittest for WMWorkload class
"""

import os
import unittest

from WMCore.WMSpec.WMWorkload import WMWorkload, WMWorkloadHelper
from WMCore.WMSpec.WMTask import WMTask, WMTaskHelper

class WMWorkloadTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        """
        self.persistFile = "%s/WMWorkloadPersistencyTest.pkl" % os.getcwd()
        return

    def tearDown(self):
        """
        _tearDown_

        """
        if os.path.exists(self.persistFile):
            os.remove(self.persistFile)
        return

    def testInstantiation(self):
        """
        _testInstantiation_

        Verify that the WMWorkload class and the WMWorkloadHelper class can
        be instantiated.
        """
        workload = WMWorkload("workload1")
        helper = WMWorkloadHelper(WMWorkload("workload2"))
        return

    def testB(self):
        """adding Tasks"""

        workload = WMWorkloadHelper(WMWorkload("workload1"))

        task1 = WMTask("task1")
        task2 = WMTaskHelper(WMTask("task2"))

        # direct addition of task
        workload.addTask(task1)
        workload.addTask(task2)

        self.assertEqual(workload.listAllTaskNodes(), ["task1", "task2"])

        # using factory method to create new task when added
        task3 = workload.newTask("task3")

        task4 = workload.newTask("task4")

        workload.newTask("task5")
        workload.removeTask("task5")

        self.assertEqual(workload.listAllTaskNodes(),
                         ["task1", "task2", "task3", "task4"])

        # prevent adding duplicate tasks
        self.assertRaises(RuntimeError, workload.addTask, task1)
        self.assertRaises(RuntimeError, workload.newTask, "task4")

        self.assertEqual(workload.listAllTaskNodes(),
                         ["task1", "task2", "task3", "task4"])


        self.assertEqual(workload.getTask("task1").name(), "task1")
        self.assertEqual(workload.getTask("task2").name(), "task2")
        self.assertEqual(workload.getTask("task3").name(), "task3")
        self.assertEqual(workload.getTask("task4").name(), "task4")


    def testC(self):
        """test persistency"""

        workload = WMWorkloadHelper(WMWorkload("workload1"))
        workload.newTask("task1")
        workload.newTask("task2")
        workload.newTask("task3")
        workload.newTask("task4")

        workload.save(self.persistFile)

        workload2 = WMWorkloadHelper(WMWorkload("workload2"))
        workload2.load(self.persistFile)

        self.assertEqual(
            workload.listAllTaskNames(),
            workload2.listAllTaskNames()
            )
        # probably need to flesh this out a bit more

    def testD_Owner(self):
        """Test setOwner/getOwner function. """

        workload = WMWorkloadHelper(WMWorkload("workload1"))

        workload.setOwner(name = "Lumumba")
        self.assertEqual(workload.data.owner.name, "Lumumba")
        result = workload.getOwner()

        ownerProps = {'capital': 'Kinshasa',
                      'adversary': 'Katanga',
                      'removedby': 'Kabila'}

        workload.setOwner(name = "Mobutu", ownerProperties = ownerProps)
        result = workload.getOwner()

        for key in ownerProps.keys():
            self.assertEqual(result[key], ownerProps[key])

    def testWhiteBlacklists(self):
        """
        _testWhiteBlacklists_

        Verify that setting site/block/run black and white lists through the
        workload helper class works as expected.  For site black and white lists
        the workload helper class should update the lists for all tasks.  For
        run and block lists only tasks that have input datasets defined should
        be updated.
        """
        testWorkload = WMWorkloadHelper(WMWorkload("TestWorkload"))

        procTestTask = testWorkload.newTask("ProcessingTask")
        procTestTaskCMSSW = procTestTask.makeStep("cmsRun1")
        procTestTaskCMSSW.setStepType("CMSSW")

        procTestTask.addInputDataset(primary = "PrimaryDataset",
                                     processed = "ProcessedDataset",
                                     tier = "DataTier",
                                     block_whitelist = ["Block1", "Block2"],
                                     black_blacklist = ["Block3"],
                                     run_whitelist = [1, 2],
                                     run_blacklist = [3])

        mergeTestTask = procTestTask.addTask("MergeTask")
        mergeTestTask.setInputReference(procTestTaskCMSSW, outputModule = "output")

        weirdTestTask = mergeTestTask.addTask("WeirdTask")
        weirdTestTask.addInputDataset(primary = "PrimaryDatasetB",
                                      processed = "ProcessedDatasetB",
                                      tier = "DataTierB",
                                      block_whitelist = ["BlockA", "BlockB"],
                                      black_blacklist = ["BlockC"],
                                      run_whitelist = [11, 12],
                                      run_blacklist = [13])
        
        testWorkload.setSiteWhitelist(["T1_US_FNAL", "T0_CH_CERN"])
        testWorkload.setSiteBlacklist(["T1_DE_KIT"])
        testWorkload.setBlockWhitelist(["Block4"])
        testWorkload.setBlockBlacklist(["Block5", "Block6"])
        testWorkload.setRunWhitelist([4])
        testWorkload.setRunBlacklist([5, 6])

        for task in [procTestTask, mergeTestTask, weirdTestTask]:
            self.assertEqual(len(task.siteWhitelist()), 2,
                             "Error: Wrong number of sites in white list.")
            self.assertEqual(len(task.siteBlacklist()), 1,
                             "Error: Wrong number of sites in black list.")
            
            self.assertTrue("T1_US_FNAL" in task.siteWhitelist(),
                            "Error: Site missing from white list.")
            self.assertTrue("T0_CH_CERN" in task.siteWhitelist(),
                            "Error: Site missing from white list.")
            self.assertTrue("T1_DE_KIT" in task.siteBlacklist(),
                            "Error: Site missing from black list.")

        for task in [procTestTask, weirdTestTask]:
            self.assertEqual(len(task.inputBlockWhitelist()), 1,
                             "Error: Wrong number of blocks in white list.")
            self.assertEqual(len(task.inputBlockBlacklist()), 2,
                             "Error: Wrong number of blocks in black list.")
            self.assertEqual(len(task.inputRunWhitelist()), 1,
                             "Error: Wrong number of runs in white list.")
            self.assertEqual(len(task.inputRunBlacklist()), 2,
                             "Error: Wrong number of runs in black list.")

            self.assertTrue("Block4" in task.inputBlockWhitelist(),
                            "Error: Block missing from white list.")
            self.assertTrue("Block5" in task.inputBlockBlacklist(),
                            "Error: Block missing from black list.")
            self.assertTrue("Block6" in task.inputBlockBlacklist(),
                            "Error: Block missing from black list.")

            self.assertTrue(4 in task.inputRunWhitelist(),
                            "Error: Run missing from white list.")
            self.assertTrue(5 in task.inputRunBlacklist(),
                            "Error: Run missing from black list.")
            self.assertTrue(6 in task.inputRunBlacklist(),
                            "Error: Run missing from black list.")

        self.assertEqual(mergeTestTask.inputBlockWhitelist(), None,
                         "Error: Block white list should be empty.")
        self.assertEqual(mergeTestTask.inputBlockBlacklist(), None,
                         "Error: Block black list should be empty.")
        self.assertEqual(mergeTestTask.inputRunWhitelist(), None,
                         "Error: Run white list should be empty.")
        self.assertEqual(mergeTestTask.inputRunBlacklist(), None,
                         "Error: Run black list should be empty.")
            
        return

    def testUpdatingMergeParameters(self):
        """
        _testUpdatingMergeParameters_

        Verify that the setMergeParameters() method updates all of the merge
        tasks in the workflow as well as the minimum merge size for processing
        tasks.
        """
        testWorkload = WMWorkloadHelper(WMWorkload("TestWorkload"))

        procTask = testWorkload.newTask("ProcessingTask")
        procTask.setSplittingAlgorithm("FileBased", files_per_job = 1)
        procTask.setTaskType("Processing")
        procTaskCMSSW = procTask.makeStep("cmsRun1")
        procTaskCMSSW.setStepType("CMSSW")
        procTaskCMSSWHelper = procTaskCMSSW.getTypeHelper()
        procTaskCMSSWHelper.setMinMergeSize(1)

        mergeTask = procTask.addTask("MergeTask")
        mergeTask.setInputReference(procTaskCMSSW, outputModule = "output")
        mergeTask.setTaskType("Merge")
        mergeTask.setSplittingAlgorithm("WMBSMergeBySize", max_merge_size = 2,
                                        max_merge_events = 2, min_merge_size = 2)
        mergeTaskCMSSW = mergeTask.makeStep("cmsRun1")
        mergeTaskCMSSW.setStepType("CMSSW")

        skimTask = mergeTask.addTask("SkimTask")
        skimTask.setTaskType("Skim")
        skimTask.setInputReference(mergeTaskCMSSW, outputModule = "merged")
        skimTask.setSplittingAlgorithm("TwoFileBased", files_per_job = 1)                
        skimTaskCMSSW = skimTask.makeStep("cmsRun1")
        skimTaskCMSSW.setStepType("CMSSW")
        skimTaskCMSSWHelper = skimTaskCMSSW.getTypeHelper()
        skimTaskCMSSWHelper.setMinMergeSize(3)

        testWorkload.setMergeParameters(minSize = 10, maxSize = 100, maxEvents = 1000)

        procSplitParams = procTask.jobSplittingParameters()
        self.assertEqual(len(procSplitParams.keys()), 4,
                         "Error: Wrong number of params for proc task.")
        self.assertEqual(procSplitParams["algorithm"], "FileBased",
                         "Error: Wrong job splitting algo for proc task.")
        self.assertEqual(procSplitParams["files_per_job"], 1,
                         "Error: Wrong number of files per job.")
        self.assertEqual(procSplitParams["siteWhitelist"], [],
                         "Error: Site white list was updated.")
        self.assertEqual(procSplitParams["siteBlacklist"], [],
                         "Error: Site black list was updated.")

        skimSplitParams = skimTask.jobSplittingParameters()
        self.assertEqual(len(skimSplitParams.keys()), 4,
                         "Error: Wrong number of params for skim task.")
        self.assertEqual(skimSplitParams["algorithm"], "TwoFileBased",
                         "Error: Wrong job splitting algo for skim task.")
        self.assertEqual(skimSplitParams["files_per_job"], 1,
                         "Error: Wrong number of files per job.")
        self.assertEqual(skimSplitParams["siteWhitelist"], [],
                         "Error: Site white list was updated.")
        self.assertEqual(skimSplitParams["siteBlacklist"], [],
                         "Error: Site black list was updated.")

        mergeSplitParams = mergeTask.jobSplittingParameters()
        self.assertEqual(len(mergeSplitParams.keys()), 6,
                         "Error: Wrong number of params for merge task.")
        self.assertEqual(mergeSplitParams["algorithm"], "WMBSMergeBySize",
                         "Error: Wrong job splitting algo for merge task.")
        self.assertEqual(mergeSplitParams["min_merge_size"], 10,
                         "Error: Wrong min merge size.")
        self.assertEqual(mergeSplitParams["max_merge_size"], 100,
                         "Error: Wrong max merge size.")
        self.assertEqual(mergeSplitParams["max_merge_events"], 1000,
                         "Error: Wrong max merge events.")        
        self.assertEqual(mergeSplitParams["siteWhitelist"], [],
                         "Error: Site white list was updated.")
        self.assertEqual(mergeSplitParams["siteBlacklist"], [],
                         "Error: Site black list was updated.")
        
        self.assertEqual(procTaskCMSSWHelper.minMergeSize(), 10,
                         "Error: Min merge size for proc task is wrong.")
        self.assertEqual(skimTaskCMSSWHelper.minMergeSize(), 10,
                         "Error: Min merge size for skim task is wrong.")        
        return

    def testUpdatingLFNAndDataset(self):
        """
        _testUpdatingLFNAndDataset_

        Verify that after chaning the acquisition era, processing version and
        merge/unmerged LFN bases that all the output datasets are named
        correctly and that the metadata contained in each output module is
        correct.  Also verify that the listOutputDatasets() method works
        correctly.
        """
        testWorkload = WMWorkloadHelper(WMWorkload("TestWorkload"))

        procTask = testWorkload.newTask("ProcessingTask")
        procTask.setTaskType("Processing")
        procTaskCMSSW = procTask.makeStep("cmsRun1")
        procTaskCMSSW.setStepType("CMSSW")
        procTaskCMSSWHelper = procTaskCMSSW.getTypeHelper()
        procTaskCMSSWHelper.setMinMergeSize(1)
        procTask.applyTemplates()

        procTaskCMSSWHelper.addOutputModule("OutputA",
                                            primaryDataset = "bogusPrimary",
                                            processedDataset = "bogusProcessed",
                                            dataTier = "DataTierA",
                                            lfnBase = "bogusUnmerged",
                                            mergedLFNBase = "bogusMerged",
                                            filterName = None)
        procTaskCMSSWHelper.addOutputModule("OutputB",
                                            primaryDataset = "bogusPrimary",
                                            processedDataset = "bogusProcessed",
                                            dataTier = "DataTierB",
                                            lfnBase = "bogusUnmerged",
                                            mergedLFNBase = "bogusMerged",
                                            filterName = None)

        mergeTask = procTask.addTask("MergeTask")
        mergeTask.setTaskType("Merge")
        mergeTaskCMSSW = mergeTask.makeStep("cmsRun1")
        mergeTaskCMSSW.setStepType("CMSSW")
        mergeTaskCMSSWHelper = mergeTaskCMSSW.getTypeHelper()
        mergeTask.applyTemplates()

        mergeTaskCMSSWHelper.addOutputModule("Merged",
                                             primaryDataset = "bogusPrimary",
                                             processedDataset = "bogusProcessed",
                                             dataTier = "DataTierA",
                                             lfnBase = "bogusUnmerged",
                                             mergedLFNBase = "bogusMerges",
                                             filterName = None)

        skimTask = mergeTask.addTask("SkimTask")
        skimTask.setTaskType("Skim")
        skimTaskCMSSW = skimTask.makeStep("cmsRun1")
        skimTaskCMSSW.setStepType("CMSSW")
        skimTaskCMSSWHelper = skimTaskCMSSW.getTypeHelper()        
        skimTask.applyTemplates()

        skimTaskCMSSWHelper.addOutputModule("SkimA",
                                            primaryDataset = "bogusPrimary",
                                            processedDataset = "bogusProcessed",
                                            dataTier = "DataTierA",
                                            lfnBase = "bogusUnmerged",
                                            mergedLFNBase = "bogusMerged",
                                            filterName = "bogusFilter")

        testWorkload.setAcquisitionEra("TestAcqEra")

        outputModules = [procTaskCMSSWHelper.getOutputModule("OutputA"),
                         procTaskCMSSWHelper.getOutputModule("OutputB"),
                         mergeTaskCMSSWHelper.getOutputModule("Merged"),
                         skimTaskCMSSWHelper.getOutputModule("SkimA")]
        for outputModule in outputModules:
            self.assertEqual(outputModule.primaryDataset, "bogusPrimary",
                             "Error: Primary dataset was modified.")
            dataTier = outputModule.dataTier
            filterName = outputModule.filterName

            if filterName == None:
                procDataset = "TestAcqEra-None"
                self.assertEqual(outputModule.processedDataset, procDataset,
                                 "Error: Processed dataset is incorrect.")
            else:
                procDataset = "TestAcqEra-%s-None" % filterName
                self.assertEqual(outputModule.processedDataset, procDataset,
                                 "Error: Processed dataset is incorrect.")

            mergedLFN = "/store/data/%s/%s" % (dataTier, procDataset)
            unmergedLFN = "/store/unmerged/%s/%s" % (dataTier, procDataset)

            self.assertEqual(outputModule.lfnBase, unmergedLFN,
                             "Error: Incorrect unmerged LFN.")
            self.assertEqual(outputModule.mergedLFNBase, mergedLFN,
                             "Error: Incorrect merged LFN.")

        testWorkload.setProcessingVersion("vTest")

        for outputModule in outputModules:
            self.assertEqual(outputModule.primaryDataset, "bogusPrimary",
                             "Error: Primary dataset was modified.")

            dataTier = outputModule.dataTier
            filterName = outputModule.filterName

            if filterName == None:
                procDataset = "TestAcqEra-vTest"
                self.assertEqual(outputModule.processedDataset, procDataset,
                                 "Error: Processed dataset is incorrect.")
            else:
                procDataset = "TestAcqEra-%s-vTest" % filterName
                self.assertEqual(outputModule.processedDataset, procDataset,
                                 "Error: Processed dataset is incorrect.")

            mergedLFN = "/store/data/%s/%s" % (dataTier, procDataset)
            unmergedLFN = "/store/unmerged/%s/%s" % (dataTier, procDataset)

            self.assertEqual(outputModule.lfnBase, unmergedLFN,
                             "Error: Incorrect unmerged LFN.")
            self.assertEqual(outputModule.mergedLFNBase, mergedLFN,
                             "Error: Incorrect merged LFN.")

        testWorkload.setLFNBase("/store/temp/WMAgent/merged",
                                "/store/temp/WMAgent/unmerged")

        for outputModule in outputModules:
            self.assertEqual(outputModule.primaryDataset, "bogusPrimary",
                             "Error: Primary dataset was modified.")

            dataTier = outputModule.dataTier
            filterName = outputModule.filterName

            if filterName == None:
                procDataset = "TestAcqEra-vTest"
                self.assertEqual(outputModule.processedDataset, procDataset,
                                 "Error: Processed dataset is incorrect.")
            else:
                procDataset = "TestAcqEra-%s-vTest" % filterName
                self.assertEqual(outputModule.processedDataset, procDataset,
                                 "Error: Processed dataset is incorrect.")

            mergedLFN = "/store/temp/WMAgent/merged/%s/%s" % (dataTier, procDataset)
            unmergedLFN = "/store/temp/WMAgent/unmerged/%s/%s" % (dataTier, procDataset)

            self.assertEqual(outputModule.lfnBase, unmergedLFN,
                             "Error: Incorrect unmerged LFN.")
            self.assertEqual(outputModule.mergedLFNBase, mergedLFN,
                             "Error: Incorrect merged LFN.")

        outputDatasets = testWorkload.listOutputDatasets()
        self.assertEqual(len(outputDatasets), 3,
                         "Error: Wrong number of output datasets.")
        self.assertTrue("/bogusPrimary/TestAcqEra-vTest/DataTierA" in outputDatasets,
                        "Error: A dataset is missing")
        self.assertTrue("/bogusPrimary/TestAcqEra-vTest/DataTierB" in outputDatasets,
                        "Error: A dataset is missing")
        self.assertTrue("/bogusPrimary/TestAcqEra-bogusFilter-vTest/DataTierA" in outputDatasets,
                        "Error: A dataset is missing")        
        return

    def testUpdatingSplitParameters(self):
        """
        _testUpdatingSplitParameters_

        Verify that the method to update all the splitting algorithms for a
        given type of task works correctly.
        """
        testWorkload = WMWorkloadHelper(WMWorkload("TestWorkload"))

        procTask = testWorkload.newTask("ProcessingTask")
        procTask.setSplittingAlgorithm("FileBased", files_per_job = 1)
        procTask.setTaskType("Processing")

        mergeTask = procTask.addTask("MergeTask")
        mergeTask.setTaskType("Merge")
        mergeTask.setSplittingAlgorithm("WMBSMergeBySize", max_merge_size = 2,
                                        max_merge_events = 2, min_merge_size = 2)

        skimTask = mergeTask.addTask("SkimTask")
        skimTask.setTaskType("Skim")
        skimTask.setSplittingAlgorithm("TwoFileBased", files_per_job = 1)                

        testWorkload.setJobSplittingParameters("Processing", "TwoFileBased",
                                               {"files_per_job": 2})
        testWorkload.setJobSplittingParameters("Skim", "RunBased",
                                               {"max_files": 21,
                                                "some_other_param": "value"})

        procSplitParams = procTask.jobSplittingParameters()
        self.assertEqual(len(procSplitParams.keys()), 4,
                         "Error: Wrong number of params for proc task.")
        self.assertEqual(procSplitParams["algorithm"], "TwoFileBased",
                         "Error: Wrong job splitting algo for proc task.")
        self.assertEqual(procSplitParams["files_per_job"], 2,
                         "Error: Wrong number of files per job.")
        self.assertEqual(procSplitParams["siteWhitelist"], [],
                         "Error: Site white list was updated.")
        self.assertEqual(procSplitParams["siteBlacklist"], [],
                         "Error: Site black list was updated.")

        skimSplitParams = skimTask.jobSplittingParameters()
        self.assertEqual(len(skimSplitParams.keys()), 5,
                         "Error: Wrong number of params for skim task.")
        self.assertEqual(skimSplitParams["algorithm"], "RunBased",
                         "Error: Wrong job splitting algo for skim task.")
        self.assertEqual(skimSplitParams["max_files"], 21,
                         "Error: Wrong number of files per job.")
        self.assertEqual(skimSplitParams["some_other_param"], "value",
                         "Error: Wrong other param.")
        self.assertEqual(skimSplitParams["siteWhitelist"], [],
                         "Error: Site white list was updated.")
        self.assertEqual(skimSplitParams["siteBlacklist"], [],
                         "Error: Site black list was updated.")

        mergeSplitParams = mergeTask.jobSplittingParameters()
        self.assertEqual(len(mergeSplitParams.keys()), 6,
                         "Error: Wrong number of params for merge task.")
        self.assertEqual(mergeSplitParams["algorithm"], "WMBSMergeBySize",
                         "Error: Wrong job splitting algo for merge task.")
        self.assertEqual(mergeSplitParams["min_merge_size"], 2,
                         "Error: Wrong min merge size.")
        self.assertEqual(mergeSplitParams["max_merge_size"], 2,
                         "Error: Wrong max merge size.")
        self.assertEqual(mergeSplitParams["max_merge_events"], 2,
                         "Error: Wrong max merge events.")        
        self.assertEqual(mergeSplitParams["siteWhitelist"], [],
                         "Error: Site white list was updated.")
        self.assertEqual(mergeSplitParams["siteBlacklist"], [],
                         "Error: Site black list was updated.")        
        return

if __name__ == '__main__':
    unittest.main()
