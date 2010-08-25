#!/usr/bin/env python
"""
_WMTask unittest_
"""

import unittest
from WMCore.WMSpec.WMTask import WMTask, WMTaskHelper, makeWMTask
from WMCore.WMSpec.WMStep import makeWMStep


class WMTaskTest(unittest.TestCase):
    """
    TestCase for WMTask object
    """
    def testA(self):
        """instantiation"""

        try:
            task1 = WMTask("task1")
        except Exception, ex:
            msg = "Error instantiating WMTask:\n"
            msg += str(ex)
            self.fail(msg)

        try:
            task2 = makeWMTask("task2")
        except Exception, ex:
            msg = "Error instantiating WMTaskHelper:\n"
            msg += str(ex)
            self.fail(msg)



    def testB(self):
        """tree building"""

        task1 = makeWMTask("task1")


        task2a = task1.addTask("task2a")
        task2b = task1.addTask("task2b")
        task2c = task1.addTask("task2c")


        #for x in task1.nodeIterator():
        #    print x


    def testC(self):
        """adding Steps"""


        task1 = makeWMTask("task1")


        task2a = task1.addTask("task2a")
        task2b = task1.addTask("task2b")
        task2c = task1.addTask("task2c")


        step1 = makeWMStep("step1")
        #step1.setTopOfTree()
        step1.addStep("step1a")
        step1.addStep("step1b")
        step1.addStep("step1c")


        step2 = makeWMStep("step2")
        #step2.setTopOfTree()
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


        #print task1.data

    def testD(self):
        """
        Test Case for setSplittingAlgorithm
        """
        task1 = makeWMTask("task1")

        self.assertEqual(task1.setSplittingAlgorithm("EventBased", events_per_job = 100),None)
        self.assertEqual(task1.jobSplittingAlgorithm(), "EventBased")
        self.assertEqual(task1.jobSplittingParameters(), {'events_per_job': 100, 'algorithm': 'EventBased'})

if __name__ == '__main__':
    unittest.main()
