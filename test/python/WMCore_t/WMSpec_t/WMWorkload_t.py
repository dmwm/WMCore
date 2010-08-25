#!/usr/bin/env python
"""
_WMWorkload Test_

Unittest for WMWorkload class

"""


__revision__ = "$Id: WMWorkload_t.py,v 1.4 2010/04/20 16:03:31 mnorman Exp $"
__version__ = "$Revision: 1.4 $"

import os
import unittest

from WMCore.WMSpec.WMWorkload import WMWorkload, WMWorkloadHelper
from WMCore.WMSpec.WMTask import WMTask, WMTaskHelper


class WMWorkloadTest(unittest.TestCase):
    """
    _WMWorkloadTest_

    """
    def setUp(self):
        """setup"""
        self.persistFile = "%s/WMWorkloadPersistencyTest.pkl" % os.getcwd()

    def tearDown(self):
        """cleanup"""
        if os.path.exists(self.persistFile):
            os.remove(self.persistFile)


    def testA(self):
        """instantiation"""

        try:
            workload = WMWorkload("workload1")
        except Exception, ex:
            msg = "Failed to instantiate WMWorkload:\n"
            msg += str(ex)
            self.fail(msg)


        try:
            helper = WMWorkloadHelper(WMWorkload("workload2"))
        except Exception, ex:
            msg = "Failed to instantiate WMWorkloadHelper:\n"
            msg += str(ex)
            self.fail(msg)


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

        for i in workload.taskIterator():
            print i.name()

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



if __name__ == '__main__':
    unittest.main()
