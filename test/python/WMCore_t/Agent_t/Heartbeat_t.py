"""
_WorkQueueTestCase_

Unit tests for the WMBS File class.
"""

from __future__ import print_function

import time
import unittest

from Utils.PythonVersion import PY3

from WMCore.Agent.HeartbeatAPI import HeartbeatAPI
from WMQuality.TestInit import TestInit


class HeartbeatTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        Heartbeat tables.  Also add some dummy locations.
        """

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()  # logLevel = logging.SQLDEBUG
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules=["WMCore.Agent.Database"],
                                useDefault=False)

        if PY3:
            self.assertItemsEqual = self.assertCountEqual

    def tearDown(self):
        """
        _tearDown_

        Drop all the Heartbeat tables.
        """

        self.testInit.clearDatabase()

    def testAddComponent(self):
        """
        _testAddComponent_

        Test creation of components and worker threads as well as the
        get heartbeat DAOs
        """
        comp1 = HeartbeatAPI("testComponent1", pollInterval=60, heartbeatTimeout=600)
        comp1.registerComponent()
        self.assertEqual(comp1.getHeartbeatInfo(), [])  # no worker thread yet

        comp1.registerWorker("testWorker1")
        self.assertEqual(len(comp1.getHeartbeatInfo()), 1)

        comp1.registerWorker("testWorker2")
        self.assertEqual(len(comp1.getHeartbeatInfo()), 2)

        comp2 = HeartbeatAPI("testComponent2", pollInterval=30, heartbeatTimeout=300)
        comp2.registerComponent()
        self.assertEqual(comp2.getHeartbeatInfo(), [])  # no worker thread yet
        self.assertEqual(len(comp2.getAllHeartbeatInfo()), 2)

        comp2.registerWorker("testWorker21")
        self.assertEqual(len(comp2.getHeartbeatInfo()), 1)
        self.assertEqual(len(comp2.getAllHeartbeatInfo()), 3)

        comp1.updateWorkerHeartbeat("testWorker1", "Running")
        comp1.updateWorkerHeartbeat("testWorker2", "Running")
        comp2.updateWorkerHeartbeat("testWorker21", "Running")
        self.assertEqual(len(comp1.getAllHeartbeatInfo()), 3)
        self.assertEqual(len(comp2.getAllHeartbeatInfo()), 3)

        comp1Res = comp1.getHeartbeatInfo()
        comp2Res = comp2.getHeartbeatInfo()
        self.assertEqual(len(comp1Res), 2)
        self.assertEqual(len(comp2Res), 1)

        self.assertItemsEqual([item["name"] for item in comp1Res], ["testComponent1", "testComponent1"])
        self.assertItemsEqual([item["worker_name"] for item in comp1Res], ["testWorker1", "testWorker2"])
        self.assertItemsEqual([item["state"] for item in comp1Res], ["Running", "Running"])
        self.assertItemsEqual([item["poll_interval"] for item in comp1Res], [60, 60])
        self.assertItemsEqual([item["update_threshold"] for item in comp1Res], [600, 600])

        self.assertItemsEqual([item["name"] for item in comp2Res], ["testComponent2"])
        self.assertItemsEqual([item["worker_name"] for item in comp2Res], ["testWorker21"])
        self.assertItemsEqual([item["state"] for item in comp2Res], ["Running"])
        self.assertItemsEqual([item["poll_interval"] for item in comp2Res], [30])
        self.assertItemsEqual([item["update_threshold"] for item in comp2Res], [300])

    def testUpdateWorkers(self):
        """
        _testUpdateWorkers_

        Create a couple of components and workers and test the update methods
        """
        comp1 = HeartbeatAPI("testComponent1", pollInterval=60, heartbeatTimeout=600)
        comp1.registerComponent()
        comp1.registerWorker("testWorker1")
        comp1.registerWorker("testWorker2")

        comp2 = HeartbeatAPI("testComponent2", pollInterval=30, heartbeatTimeout=300)
        comp2.registerComponent()
        comp2.registerWorker("testWorker21")

        comp1.updateWorkerCycle("testWorker1", 1.001, None)
        comp2.updateWorkerCycle("testWorker21", 1234.1, 100)
        hb1 = comp1.getHeartbeatInfo()
        hb2 = comp2.getHeartbeatInfo()

        for worker in hb1:
            if worker['worker_name'] == 'testWorker1':
                self.assertTrue(worker["cycle_time"] > 1.0)
            else:
                self.assertEqual(worker["cycle_time"], 0)
        self.assertItemsEqual([item["outcome"] for item in hb1], [None, None])
        self.assertItemsEqual([item["error_message"] for item in hb1], [None, None])

        self.assertEqual(round(hb2[0]["cycle_time"], 1), 1234.1)
        self.assertEqual(hb2[0]["outcome"], '100')
        self.assertEqual(hb2[0]["error_message"], None)

        # time to update workers with an error
        comp1.updateWorkerError("testWorker2", "BAD JOB!!!")
        hb1 = comp1.getHeartbeatInfo()
        for worker in hb1:
            if worker['worker_name'] == 'testWorker2':
                self.assertTrue(worker["last_error"] > int(time.time() - 10))
                self.assertEqual(worker["state"], "Error")
                self.assertEqual(worker["error_message"], "BAD JOB!!!")


if __name__ == "__main__":
    unittest.main()
