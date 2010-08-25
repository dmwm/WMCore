#!/usr/bin/env python
"""
_WMTask_t_

Unit tests for the WMTask class.
"""

import unittest

from WMCore.WMSpec.WMTask import WMTask, WMTaskHelper, makeWMTask
from WMCore.WMSpec.WMStep import makeWMStep

class WMTaskTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testInstantiation(self):
        """
        _testInstantiation_

        Verify that the WMTask and the WMTaskHelper classes can be
        instantiated.
        """
        task1 = WMTask("task1")
        task2 = makeWMTask("task2")
        return

    def testTreeBuilding(self):
        """
        _testTreeBuilding_

        Verify that tasks can be created and arranged in a hierarchy.
        """
        task1 = makeWMTask("task1")
        task2a = task1.addTask("task2a")
        task2b = task1.addTask("task2b")
        task2c = task1.addTask("task2c")

        goldenTasks = ["task2a", "task2b", "task2c"]
        for childTask in task1.childTaskIterator():
            assert childTask.name() in goldenTasks, \
                   "Error: Unknown child task: %s" % childTask.name()

            goldenTasks.remove(childTask.name())

        assert len(goldenTasks) == 0, \
               "Error: Missing tasks."
        return

    def testAddingSteps(self):
        """
        _testAddingSteps_

        Verify that adding steps to a task works correctly.
        """
        task1 = makeWMTask("task1")

        task2a = task1.addTask("task2a")
        task2b = task1.addTask("task2b")
        task2c = task1.addTask("task2c")
        
        task3 = task2a.addTask("task3")
        
        step1 = makeWMStep("step1")
        step1.addStep("step1a")
        step1.addStep("step1b")
        step1.addStep("step1c")

        step2 = makeWMStep("step2")
        step2.addStep("step2a")
        step2.addStep("step2b")
        step2.addStep("step2c")

        step3 = makeWMStep("step3")
        step3.addStep("step3a")
        step3.addStep("step3b")
        step3.addStep("step3c")

        step4 = makeWMStep("step4")
        step4.addStep("step4a")
        step4.addStep("step4b")
        step4.addStep("step4c")

        task1.setStep(step1)
        task2a.setStep(step2)
        task2b.setStep(step3)
        task2c.setStep(step4)

        self.assertEqual(task1.getStep("step1a").name(), "step1a")
        self.assertEqual(task2a.getStep("step2b").name(), "step2b")
        self.assertEqual(task2b.getStep("step3c").name(), "step3c")
        self.assertEqual(task2c.getStep("step4a").name(), "step4a")

        self.assertEqual(task1.getStep("step2"), None)
        self.assertEqual(task2a.getStep("step4"), None)
        self.assertEqual(task2b.getStep("step2"), None)
        self.assertEqual(task2c.getStep("step1"), None)
        
        self.assertEqual(task1.listNodes(), ['task1', 'task2a', 'task3', 'task2b', 'task2c'])
        return

    def testSiteWhiteBlacklists(self):
        """
        _testSiteWhiteBlacklists_

        Verify that methods for setting and retrieving site white and black
        lists work correctly.
        """
        testTask = makeWMTask("TestTask")
        testTask.setSiteWhitelist(["T1_US_FNAL", "T1_DE_KIT"])
        testTask.setSiteBlacklist(["T1_IT_IN2P3", "T1_CH_CERN", "T2_US_NEBRASKA"])

        taskWhitelist = testTask.siteWhitelist()

        assert len(taskWhitelist) == 2, \
               "Error: Wrong number of sites in white list."
        assert "T1_US_FNAL" in taskWhitelist, \
               "Error: Site missing from white list."
        assert "T1_DE_KIT" in taskWhitelist, \
               "Error: Site missing from white list."

        taskBlacklist = testTask.siteBlacklist()

        assert len(taskBlacklist) == 3, \
               "Error: Wrong number if sites in black list."
        assert "T1_IT_IN2P3" in taskBlacklist, \
               "Error: Site missing from black list."
        assert "T1_CH_CERN" in taskBlacklist, \
               "Error: Site missing from black list."
        assert "T2_US_NEBRASKA" in taskBlacklist, \
               "Error: Site missing from black list."

        return
               
    def testJobSplittingArgs(self):
        """
        _testJobSplittingArgs_

        Verify that setting job splitting arguments for a task works correctly.
        When pulling the job splitting arguments out of a task the site white
        list and black list should also be included as they need to be passed to
        the job splitting code.
        """
        testTask = makeWMTask("TestTask")
        testTask.setTaskType("Processing")

        assert testTask.taskType() == "Processing", \
               "Error: Wrong task type."
        
        testTask.setSplittingAlgorithm("MadeUpAlgo", events_per_job = 100,
                                       max_job_size = 24,
                                       one_more_param = "Hello")
        testTask.setSiteWhitelist(["T1_US_FNAL", "T1_CH_CERN"])
        testTask.setSiteBlacklist(["T2_US_PERDUE", "T2_US_UCSD", "T1_TW_ASGC"])

        assert testTask.jobSplittingAlgorithm() == "MadeUpAlgo", \
               "Error: Wrong job splitting algorithm name."

        algoParams = testTask.jobSplittingParameters()

        assert len(algoParams.keys()) == 6, \
               "Error: Wrong number of algo parameters."

        assert "algorithm" in algoParams.keys(), \
               "Error: Missing algo parameter."
        assert algoParams["algorithm"] == "MadeUpAlgo", \
               "Error: Parameter has wrong value."
        assert "events_per_job" in algoParams.keys(), \
               "Error: Missing algo parameter."
        assert algoParams["events_per_job"] == 100, \
               "Error: Parameter has wrong value."
        assert "max_job_size" in algoParams.keys(), \
               "Error: Missing algo parameter."
        assert algoParams["max_job_size"] == 24, \
               "Error: Parameter has wrong value."
        assert "one_more_param" in algoParams.keys(), \
               "Error: Missing algo parameter."
        assert algoParams["one_more_param"] == "Hello", \
               "Error: Parameter has wrong value."

        return

    def testInputDataset(self):
        """
        _testInputDataset_

        Verify that the addInputDataset() method works correctly and that the
        run/block black and white lists can be changed after calling
        addInputDataset().
        """
        testTask = makeWMTask("TestTask")

        assert testTask.getInputDatasetPath() == None, \
               "Error: Input dataset path should be None."
        assert testTask.inputDatasetDBSURL() == None, \
               "Error: Input DBS URL should be None."
        assert testTask.inputBlockWhitelist() == None, \
               "Error: Input block white list should be None."
        assert testTask.inputBlockBlacklist() == None, \
               "Error: Input block black list should be None."
        assert testTask.inputRunWhitelist() == None, \
               "Error: Input run white list should be None."
        assert testTask.inputRunBlacklist() == None, \
               "Error: Input run black list should be None."

        testTask.addInputDataset(primary = "PrimaryDataset",
                                 processed = "ProcessedDataset",
                                 tier = "DataTier",
                                 dbsurl = "DBSURL",
                                 block_whitelist = ["Block1", "Block2"],
                                 block_blacklist = ["Block3", "Block4", "Block5"],
                                 run_whitelist = [1, 2, 3],
                                 run_blacklist = [4, 5])

        assert testTask.inputDatasetPath() == "/PrimaryDataset/ProcessedDataset/DataTier", \
               "Error: Input dataset path is wrong."
        assert testTask.inputDatasetDBSURL() == "DBSURL", \
               "Error: Input DBS URL is wrong."

        assert len(testTask.inputBlockWhitelist()) == 2, \
               "Error: Wrong number of blocks in white list."
        assert "Block1" in testTask.inputBlockWhitelist(), \
               "Error: Block missing from white list."
        assert "Block2" in testTask.inputBlockWhitelist(), \
               "Error: Block missing from white list."

        assert len(testTask.inputBlockBlacklist()) == 3, \
               "Error: Wrong number of blocks in black list."
        assert "Block3" in testTask.inputBlockBlacklist(), \
               "Error: Block missing from black list."
        assert "Block4" in testTask.inputBlockBlacklist(), \
               "Error: Block missing from black list."
        assert "Block5" in testTask.inputBlockBlacklist(), \
               "Error: Block missing from black list."

        assert len(testTask.inputRunWhitelist()) == 3, \
               "Error: Wrong number of runs in white list."
        assert 1 in testTask.inputRunWhitelist(), \
               "Error: Run is missing from white list."
        assert 2 in testTask.inputRunWhitelist(), \
               "Error: Run is missing from white list."
        assert 3 in testTask.inputRunWhitelist(), \
               "Error: Run is missing from white list."

        assert len(testTask.inputRunBlacklist()) == 2, \
               "Error: Wrong number of runs in black list."
        assert 4 in testTask.inputRunBlacklist(), \
               "Error: Run is missing from black list."
        assert 5 in testTask.inputRunBlacklist(), \
               "Error: Run is missing from black list."

        testTask.setInputBlockWhitelist(["Block6"])

        assert len(testTask.inputBlockWhitelist()) == 1, \
               "Error: Wrong number of blocks in white list."
        assert "Block6" in testTask.inputBlockWhitelist(), \
               "Error: Block missing from white list."
        
        testTask.setInputBlockBlacklist(["Block7", "Block8"])

        assert len(testTask.inputBlockBlacklist()) == 2, \
               "Error: Wrong number of blocks in black list."
        assert "Block7" in testTask.inputBlockBlacklist(), \
               "Error: Block missing from black list."
        assert "Block8" in testTask.inputBlockBlacklist(), \
               "Error: Block missing from black list."
        
        testTask.setInputRunWhitelist([6])

        assert len(testTask.inputRunWhitelist()) == 1, \
               "Error: Wrong number of runs in white list."
        assert 6 in testTask.inputRunWhitelist(), \
               "Error: Run missing from white list."

        testTask.setInputRunBlacklist([7, 8])

        assert len(testTask.inputRunBlacklist()) == 2, \
               "Error: Wrong number of runs in black list."
        assert 7 in testTask.inputRunBlacklist(), \
               "Error: Run missing from black list."
        assert 8 in testTask.inputRunBlacklist(), \
               "Error: Run missing from black list."
               
        return

if __name__ == '__main__':
    unittest.main()
