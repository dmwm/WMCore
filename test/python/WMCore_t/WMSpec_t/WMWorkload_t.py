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
            assert len(task.siteWhitelist()) == 2, \
                   "Error: Wrong number of sites in white list."
            assert len(task.siteBlacklist()) == 1, \
                   "Error: Wrong number of sites in black list."
            
            assert "T1_US_FNAL" in task.siteWhitelist(), \
                   "Error: Site missing from white list."
            assert "T0_CH_CERN" in task.siteWhitelist(), \
                   "Error: Site missing from white list."
            assert "T1_DE_KIT" in task.siteBlacklist(), \
                   "Error: Site missing from black list."

        for task in [procTestTask, weirdTestTask]:
            assert len(task.inputBlockWhitelist()) == 1, \
                   "Error: Wrong number of blocks in white list."
            assert len(task.inputBlockBlacklist()) == 2, \
                   "Error: Wrong number of blocks in black list."
            assert len(task.inputRunWhitelist()) == 1, \
                   "Error: Wrong number of runs in white list."
            assert len(task.inputRunBlacklist()) == 2, \
                   "Error: Wrong number of runs in black list."

            assert "Block4" in task.inputBlockWhitelist(), \
                   "Error: Block missing from white list."
            assert "Block5" in task.inputBlockBlacklist(), \
                   "Error: Block missing from black list."
            assert "Block6" in task.inputBlockBlacklist(), \
                   "Error: Block missing from black list."

            assert 4 in task.inputRunWhitelist(), \
                   "Error: Run missing from white list."
            assert 5 in task.inputRunBlacklist(), \
                   "Error: Run missing from black list."
            assert 6 in task.inputRunBlacklist(), \
                   "Error: Run missing from black list."

        assert mergeTestTask.inputBlockWhitelist() == None, \
               "Error: Block white list should be empty."
        assert mergeTestTask.inputBlockBlacklist() == None, \
               "Error: Block black list should be empty."
        assert mergeTestTask.inputRunWhitelist() == None, \
               "Error: Run white list should be empty."
        assert mergeTestTask.inputRunBlacklist() == None, \
               "Error: Run black list should be empty."
            
        return

if __name__ == '__main__':
    unittest.main()
