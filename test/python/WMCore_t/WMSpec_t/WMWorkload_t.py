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

        workload.setOwnerDetails(name = "Lumumba", group = "Hoodoo")
        self.assertEqual(workload.data.owner.name, "Lumumba")
        self.assertEqual(workload.data.owner.group, "Hoodoo")

        result = workload.getOwner()

        ownerProps = {'capital': 'Kinshasa',
                      'adversary': 'Katanga',
                      'removedby': 'Kabila'}

        workload.setOwnerDetails(name = "Mobutu", group = "DMWM", ownerProperties = ownerProps)
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
                                     tier = "DATATIER",
                                     block_whitelist = ["Block1", "Block2"],
                                     black_blacklist = ["Block3"],
                                     run_whitelist = [1, 2],
                                     run_blacklist = [3])

        mergeTestTask = procTestTask.addTask("MergeTask")
        mergeTestTask.setInputReference(procTestTaskCMSSW, outputModule = "output")

        weirdTestTask = mergeTestTask.addTask("WeirdTask")
        weirdTestTask.addInputDataset(primary = "PrimaryDatasetB",
                                      processed = "ProcessedDatasetB",
                                      tier = "DATATIERB",
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
        procTaskStageOut = procTask.makeStep("StageOut1")
        procTaskStageOut.setStepType("StageOut")
        procTaskStageOutHelper = procTaskStageOut.getTypeHelper()
        procTaskStageOutHelper.setMinMergeSize(1)

        mergeTask = procTask.addTask("MergeTask")
        mergeTask.setInputReference(procTaskCMSSW, outputModule = "output")
        mergeTask.setTaskType("Merge")
        mergeTask.setSplittingAlgorithm("WMBSMergeBySize", max_merge_size = 2,
                                        max_merge_events = 2, min_merge_size = 2)
        mergeTaskStageOut = mergeTask.makeStep("StageOut1")
        mergeTaskStageOut.setStepType("StageOut")
        mergeTaskCMSSW = mergeTask.makeStep("cmsRun1")
        mergeTaskCMSSW.setStepType("CMSSW")

        skimTask = mergeTask.addTask("SkimTask")
        skimTask.setTaskType("Skim")
        skimTask.setInputReference(mergeTaskCMSSW, outputModule = "merged")
        skimTask.setSplittingAlgorithm("TwoFileBased", files_per_job = 1)
        skimTaskStageOut = skimTask.makeStep("StageOut1")
        skimTaskStageOut.setStepType("StageOut")
        skimTaskStageOutHelper = skimTaskStageOut.getTypeHelper()
        skimTaskStageOutHelper.setMinMergeSize(3)
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

        self.assertEqual(procTaskStageOutHelper.minMergeSize(), 10,
                         "Error: Min merge size for proc task is wrong.")
        self.assertEqual(skimTaskStageOutHelper.minMergeSize(), 10,
                         "Error: Min merge size for skim task is wrong.")

        testWorkload.setJobSplittingParameters("/TestWorkload/ProcessingTask", "EventBased",
                                               {"events_per_job": 2})
        testWorkload.setMergeParameters(minSize = 20, maxSize = 100, maxEvents = 1000)

        self.assertEqual(procTaskStageOutHelper.minMergeSize(), -1,
                         "Error: Min merge size for proc task is wrong.")
        self.assertEqual(skimTaskStageOutHelper.minMergeSize(), 20,
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
        procTask.applyTemplates()

        acquisitionEra = "TestAcqEra"
        primaryDataset = "bogusPrimary"
        procEra        = "vTest"

        procTaskCMSSWHelper.addOutputModule("OutputA",
                                            primaryDataset = primaryDataset,
                                            processedDataset = "bogusProcessed",
                                            dataTier = "DATATIERA",
                                            lfnBase = "bogusUnmerged",
                                            mergedLFNBase = "bogusMerged",
                                            filterName = None)
        procTaskCMSSWHelper.addOutputModule("OutputB",
                                            primaryDataset = primaryDataset,
                                            processedDataset = "bogusProcessed",
                                            dataTier = "DATATIERB",
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
                                             dataTier = "DATATIERA",
                                             lfnBase = "bogusMerged",
                                             mergedLFNBase = "bogusMerged",
                                             filterName = None)

        skimTask = mergeTask.addTask("SkimTask")
        skimTask.setTaskType("Skim")
        skimTaskCMSSW = skimTask.makeStep("cmsRun1")
        skimTaskCMSSW.setStepType("CMSSW")
        skimTaskCMSSWHelper = skimTaskCMSSW.getTypeHelper()
        skimTask.applyTemplates()



        skimTaskCMSSWHelper.addOutputModule("SkimA",
                                            primaryDataset = primaryDataset,
                                            processedDataset = "bogusProcessed",
                                            dataTier = "DATATIERA",
                                            lfnBase = "bogusUnmerged",
                                            mergedLFNBase = "bogusMerged",
                                            filterName = "bogusFilter")

        testWorkload.setAcquisitionEra(acquisitionEra)

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
                procDataset = "%s-None" % (acquisitionEra)
                self.assertEqual(outputModule.processedDataset, procDataset,
                                 "Error: Processed dataset is incorrect.")
            else:
                procDataset = "%s-%s-None" % (acquisitionEra, filterName)
                self.assertEqual(outputModule.processedDataset, procDataset,
                                 "Error: Processed dataset is incorrect.")

            mergedLFN = "/store/data/%s/%s/%s/%s" % (acquisitionEra, primaryDataset, dataTier, 'None')
            unmergedLFN = "/store/unmerged/%s/%s/%s/%s" % (acquisitionEra, primaryDataset, dataTier, 'None')

            if outputModule._internal_name == "Merged":
                self.assertEqual(outputModule.lfnBase, mergedLFN,
                                 "Error: Incorrect unmerged LFN %s." % outputModule.lfnBase)
                self.assertEqual(outputModule.mergedLFNBase, mergedLFN,
                                 "Error: Incorrect merged LFN %s." % outputModule.mergedLFNBase)
            else:
                self.assertEqual(outputModule.lfnBase, unmergedLFN,
                                 "Error: Incorrect unmerged LFN %s." % outputModule.lfnBase)
                self.assertEqual(outputModule.mergedLFNBase, mergedLFN,
                                 "Error: Incorrect merged LFN %s." % outputModule.mergedLFNBase)

        testWorkload.setProcessingVersion(procEra)

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

            mergedLFN = "/store/data/%s/%s/%s/%s" % (acquisitionEra, primaryDataset, dataTier, procEra)
            unmergedLFN = "/store/unmerged/%s/%s/%s/%s" % (acquisitionEra, primaryDataset, dataTier, procEra)

            if outputModule._internal_name == "Merged":
                self.assertEqual(outputModule.lfnBase, mergedLFN,
                                 "Error: Incorrect unmerged LFN.")
                self.assertEqual(outputModule.mergedLFNBase, mergedLFN,
                                 "Error: Incorrect merged LFN.")
            else:
                self.assertEqual(outputModule.lfnBase, unmergedLFN,
                                 "Error: Incorrect unmerged LFN %s." % outputModule.lfnBase)
                self.assertEqual(outputModule.mergedLFNBase, mergedLFN,
                                 "Error: Incorrect merged LFN %s." % outputModule.mergedLFNBase)

        mergedLFNBase   = "/store/temp/merged"
        unmergedLFNBase = "/store/temp/unmerged"
        testWorkload.setLFNBase(mergedLFNBase, unmergedLFNBase)

        self.assertEqual(testWorkload.getLFNBases(), (mergedLFNBase,
                                                      unmergedLFNBase),
                         "Error: Wrong LFN bases.")

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

            mergedLFN = "%s/%s/%s/%s/%s" % (mergedLFNBase, acquisitionEra, primaryDataset, dataTier, procEra)
            unmergedLFN = "%s/%s/%s/%s/%s" % (unmergedLFNBase, acquisitionEra, primaryDataset, dataTier, procEra)

            if outputModule._internal_name == "Merged":
                self.assertEqual(outputModule.lfnBase, mergedLFN,
                                 "Error: Incorrect unmerged LFN.")
                self.assertEqual(outputModule.mergedLFNBase, mergedLFN,
                                 "Error: Incorrect merged LFN.")
            else:
                self.assertEqual(outputModule.lfnBase, unmergedLFN,
                                 "Error: Incorrect unmerged LFN %s." % outputModule.lfnBase)
                self.assertEqual(outputModule.mergedLFNBase, mergedLFN,
                                 "Error: Incorrect merged LFN %s." % outputModule.mergedLFNBase)

        outputDatasets = testWorkload.listOutputDatasets()
        self.assertEqual(len(outputDatasets), 3,
                         "Error: Wrong number of output datasets.")
        self.assertTrue("/bogusPrimary/TestAcqEra-vTest/DATATIERA" in outputDatasets,
                        "Error: A dataset is missing")
        self.assertTrue("/bogusPrimary/TestAcqEra-vTest/DATATIERB" in outputDatasets,
                        "Error: A dataset is missing")
        self.assertTrue("/bogusPrimary/TestAcqEra-bogusFilter-vTest/DATATIERA" in outputDatasets,
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
        procTaskCmssw = procTask.makeStep("cmsRun1")
        procTaskCmssw.setStepType("CMSSW")
        procTaskStageOut = procTask.makeStep("StageOut1")
        procTaskStageOut.setStepType("StageOut")
        procTaskStageOut.getTypeHelper().setMinMergeSize(2)
        procTask.applyTemplates()

        mergeTask = procTask.addTask("MergeTask")
        mergeTask.setTaskType("Merge")
        mergeTask.setSplittingAlgorithm("WMBSMergeBySize", max_merge_size = 2,
                                        max_merge_events = 2, min_merge_size = 2)

        skimTask = mergeTask.addTask("SkimTask")
        skimTask.setTaskType("Skim")
        skimTaskCmssw = skimTask.makeStep("cmsRun1")
        skimTaskCmssw.setStepType("CMSSW")
        skimTask.setSplittingAlgorithm("TwoFileBased", files_per_job = 1)
        skimTask.applyTemplates()

        testWorkload.setJobSplittingParameters("/TestWorkload/ProcessingTask", "TwoFileBased",
                                               {"files_per_job": 2})
        testWorkload.setJobSplittingParameters("/TestWorkload/ProcessingTask/MergeTask/SkimTask", "RunBased",
                                               {"max_files": 21,
                                                "some_other_param": "value"})

        self.assertEqual(testWorkload.startPolicy(), "Block",
                         "Error: Wrong start policy.")
        self.assertEqual(testWorkload.startPolicyParameters()["SliceType"], "NumberOfFiles",
                         "Errror: Wrong slice type.")
        self.assertEqual(testWorkload.startPolicyParameters()["SliceSize"], 2,
                         "Errror: Wrong slice size.")

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

        stepHelper = procTask.getStepHelper("StageOut1")
        self.assertEqual(stepHelper.minMergeSize(), 2,
                         "Error: Wrong min merge size: %s" % stepHelper.minMergeSize())

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
        self.assertEqual(mergeSplitParams["algorithm"], "ParentlessMergeBySize",
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

        testWorkload.setJobSplittingParameters("/TestWorkload/ProcessingTask", "EventBased",
                                               {"events_per_job": 4})

        self.assertEqual(testWorkload.startPolicy(), "Block",
                         "Error: Wrong start policy.")
        self.assertEqual(testWorkload.startPolicyParameters()["SliceType"], "NumberOfEvents",
                         "Errror: Wrong slice type.")
        self.assertEqual(testWorkload.startPolicyParameters()["SliceSize"], 4,
                         "Errror: Wrong slice size.")

        stepHelper = procTask.getStepHelper("StageOut1")
        self.assertEqual(stepHelper.minMergeSize(), -1,
                         "Error: Wrong min merge size.")

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

        testWorkload.setJobSplittingParameters("/TestWorkload/ProcessingTask", "FileBased",
                                               {"files_per_job": 4})

        stepHelper = procTask.getStepHelper("StageOut1")
        self.assertEqual(stepHelper.minMergeSize(), 2,
                         "Error: Wrong min merge size.")
        return

    def testListJobSplittingParametersByTask(self):
        """
        _testListJobSplittingParametersByTask_

        Verify that the listJobSplittingParametersByTask() method works
        correctly.
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

        testWorkload.setJobSplittingParameters("/TestWorkload/ProcessingTask", "TwoFileBased",
                                               {"files_per_job": 2})
        testWorkload.setJobSplittingParameters("/TestWorkload/ProcessingTask/MergeTask/SkimTask", "RunBased",
                                               {"max_files": 21,
                                                "some_other_param": "value"})

        results = testWorkload.listJobSplittingParametersByTask()

        self.assertEqual(len(results.keys()), 3, \
               "Error: Wrong number of tasks.")
        self.assertTrue("/TestWorkload/ProcessingTask" in results.keys(),
                        "Error: Task is missing.")
        self.assertTrue("/TestWorkload/ProcessingTask/MergeTask" in results.keys(),
                        "Error: Task is missing.")
        self.assertTrue("/TestWorkload/ProcessingTask/MergeTask/SkimTask" in results.keys(),
                        "Error: Task is missing.")

        self.assertEqual(results["/TestWorkload/ProcessingTask"], {"files_per_job": 2, "algorithm": "TwoFileBased", "type": "Processing"},
                         "Error: Wrong splitting parameters.")
        self.assertEqual(results["/TestWorkload/ProcessingTask/MergeTask/SkimTask"],
                         {"max_files": 21, "algorithm": "RunBased", "some_other_param": "value", "type": "Skim"},
                         "Error: Wrong splitting parameters.")
        self.assertEqual(results["/TestWorkload/ProcessingTask/MergeTask"],
                         {"algorithm": "ParentlessMergeBySize", "max_merge_size": 2,
                          "max_merge_events": 2, "min_merge_size": 2, "type": "Merge"},
                         "Error: Wrong splitting parameters.")

        return

    def testUpdatingTimeouts(self):
        """
        _testUpdatingTimeouts_

        Verify that task timeouts are set correctly.
        """
        testWorkload = WMWorkloadHelper(WMWorkload("TestWorkload"))

        procTask = testWorkload.newTask("ProcessingTask")
        procTask.setTaskType("Processing")
        mergeTask = procTask.addTask("MergeTask")
        mergeTask.setTaskType("Merge")

        testWorkload.setTaskTimeOut("/TestWorkload/ProcessingTask", 60)
        testWorkload.setTaskTimeOut("/TestWorkload/ProcessingTask/MergeTask", 30)

        self.assertEqual(testWorkload.listTimeOutsByTask(),
                         {"/TestWorkload/ProcessingTask": 60,
                          "/TestWorkload/ProcessingTask/MergeTask": 30},
                         "Error: Timeouts not set correctly.")
        return

    def testTruncate(self):
        """
        _testTruncate_

        Verify that the truncate method works correctly.
        """
        testWorkload = WMWorkloadHelper(WMWorkload("TestWorkload"))
        procTask = testWorkload.newTask("ProcessingTask")
        procTask.setSplittingAlgorithm("FileBased", files_per_job = 1)
        procTask.setTaskType("Processing")
        procTaskCmssw = procTask.makeStep("cmsRun1")
        procTaskCmssw.setStepType("CMSSW")
        procTask.applyTemplates()
        procTaskCmsswHelper = procTaskCmssw.getTypeHelper()
        procTaskCmsswHelper.addOutputModule("output",
                                            primaryDataset = "primary",
                                            processedDataset = "processed",
                                            dataTier = "tier",
                                            filterName = "filter",
                                            lfnBase = "/store/data",
                                            mergedLFNBase = "/store/unmerged")

        procTaskStageOut = procTask.makeStep("StageOut1")
        procTaskStageOut.setStepType("StageOut")
        procTaskStageOut.getTypeHelper().setMinMergeSize(2)
        procTask.applyTemplates()

        mergeTask = procTask.addTask("MergeTask")
        mergeTask.setTaskType("Merge")
        mergeTask.setSplittingAlgorithm("WMBSMergeBySize", max_merge_size = 2,
                                        max_merge_events = 2, min_merge_size = 2)
        mergeTaskCmssw = mergeTask.makeStep("cmsRun1")
        mergeTaskCmssw.setStepType("CMSSW")
        mergeTask.applyTemplates()
        mergeTask.setInputReference(procTask, outputModule = "output")

        cleanupTask = procTask.addTask("CleanupTask")
        cleanupTask.setTaskType("Cleanup")
        cleanupTask.setSplittingAlgorithm("SiblingProcessingBased", files_per_job = 50)
        cleanupTaskCmssw = cleanupTask.makeStep("cmsRun1")
        cleanupTaskCmssw.setStepType("CMSSW")
        cleanupTask.applyTemplates()
        cleanupTask.setInputReference(procTask, outputModule = "output")

        skimTask = mergeTask.addTask("SkimTask")
        skimTask.setTaskType("Skim")
        skimTaskCmssw = skimTask.makeStep("cmsRun1")
        skimTaskCmssw.setStepType("CMSSW")
        skimTask.setSplittingAlgorithm("TwoFileBased", files_per_job = 1)
        skimTask.applyTemplates()

        testWorkload.truncate("TestWorkload", "/TestWorkload/ProcessingTask",
                              "somecouchurl", "somedatabase")
        testWorkload.truncate("TestWorkloadResubmit", "/TestWorkload/ProcessingTask/MergeTask",
                              "somecouchurl", "somedatabase")

        self.assertEqual(len(testWorkload.getTopLevelTask()), 2,
                         "Error: There should be two top level tasks.")
        goldenTasks = [mergeTask.getPathName(), cleanupTask.getPathName()]
        for topLevelTask in testWorkload.getTopLevelTask():
            self.assertTrue(topLevelTask.getPathName() in goldenTasks,
                            "Error: Extra top level task.")
            goldenTasks.remove(topLevelTask.getPathName())

        self.assertEqual(testWorkload.name(), "TestWorkloadResubmit",
                         "Error: The workload name is wrong.")

        self.assertEqual(len(testWorkload.listAllTaskPathNames()), 3,
                         "Error: There should only be three tasks")
        self.assertEqual(len(testWorkload.listAllTaskNames()), 3,
                         "Error: There should only be three tasks")
        self.assertTrue("/TestWorkloadResubmit/MergeTask" in testWorkload.listAllTaskPathNames(),
                        "Error: Merge task is missing.")
        self.assertTrue("/TestWorkloadResubmit/CleanupTask" in testWorkload.listAllTaskPathNames(),
                        "Error: Cleanup task is missing.")
        self.assertTrue("/TestWorkloadResubmit/MergeTask/SkimTask" in testWorkload.listAllTaskPathNames(),
                        "Error: Skim task is missing.")
        self.assertTrue("MergeTask" in testWorkload.listAllTaskNames(),
                        "Error: Merge task is missing.")
        self.assertTrue("CleanupTask" in testWorkload.listAllTaskNames(),
                        "Error: Cleanup task is missing.")
        self.assertTrue("SkimTask" in testWorkload.listAllTaskNames(),
                        "Error: Skim task is missing.")
        self.assertEqual("ResubmitBlock", testWorkload.startPolicy(),
                         "Error: Start policy is wrong.")
        self.assertEqual(mergeTask.getInputACDC(),
                         {"database": "somedatabase", "fileset": "/TestWorkload/ProcessingTask/MergeTask",
                          "collection": "TestWorkload", "server": "somecouchurl"})

        return

if __name__ == '__main__':
    unittest.main()
