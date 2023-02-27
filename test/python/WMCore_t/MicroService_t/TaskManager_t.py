"""
Unit tests for Unified/TaskManager.py module

Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
"""
from __future__ import division, print_function

import time
import unittest

from builtins import str as newstr
from future.utils import listvalues

from WMCore.MicroService.MSCore.TaskManager import \
    TaskManager, genkey, UidSet

def myFunc(interval, results):
    "Test function"
    time.sleep(interval)
    results.update({interval: 'ok_%s' % interval})


class TaskManagerTest(unittest.TestCase):
    "Unit test for TaskManager module"

    def setUp(self):
        pass

    def testUidSet(self):
        "Test UidSet class"
        mgr = UidSet()
        mgr.add(1)
        mgr.add(1)
        self.assertEqual(2, mgr.get(1))
        mgr.discard(1)
        self.assertEqual(1, mgr.get(1))
        mgr.discard(1)
        self.assertEqual(0, mgr.get(1))

    def testTaskManager(self):
        "Test TaskManager class"
        mgr = TaskManager(nworkers=3)
        self.assertEqual(3, mgr.nworkers())

        jobs = []
        results = {}
        kwds = {}
        args1 = (1, results)
        args2 = (2, results)
        args3 = (3, results)
        args4 = (4, results)
        pid1 = genkey(str(args1) + str(kwds))
        pid2 = genkey(str(args2) + str(kwds))
        pid3 = genkey(str(args3) + str(kwds))
        pid4 = genkey(newstr(args4) + newstr(kwds))

        jobs.append(mgr.spawn(myFunc, *args1))
        jobs.append(mgr.spawn(myFunc, *args2))
        jobs.append(mgr.spawn(myFunc, *args3))
        jobs.append(mgr.spawn(myFunc, *args4))

        self.assertEqual(True, mgr.is_alive(pid1))
        self.assertEqual(True, mgr.is_alive(pid2))
        self.assertEqual(True, mgr.is_alive(pid3))
        self.assertEqual(True, mgr.is_alive(pid4))

        mgr.joinall(jobs)

        self.assertEqual(sorted(results.keys()), [1, 2, 3, 4])
        self.assertEqual(listvalues(results), ['ok_1', 'ok_2', 'ok_3', 'ok_4'])

        self.assertEqual(False, mgr.is_alive(pid1))
        self.assertEqual(False, mgr.is_alive(pid2))
        self.assertEqual(False, mgr.is_alive(pid3))

        for worker in mgr.workers:
            self.assertEqual(True, worker.is_alive())

        print("### TaskManager quit")
        mgr.quit()
        print("### TaskManager workrers no longer running")

        for worker in mgr.workers:
            self.assertEqual(False, worker.is_alive())


if __name__ == '__main__':
    unittest.main()
