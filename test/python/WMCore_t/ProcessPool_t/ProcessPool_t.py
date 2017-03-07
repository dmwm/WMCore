"""
_ProcessPool_t

Unit tests for the ProcessPool class.
"""

from builtins import range
import unittest
import nose

from WMCore.ProcessPool.ProcessPool import ProcessPool
from WMQuality.TestInit import TestInit

class ProcessPoolTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection(destroyAllDatabase = True)
        self.testInit.setSchema(customModules = ["WMCore.Agent.Database"],
                                useDefault = False)
        return

    def tearDown(self):
        """
        _tearDown_

        """
        self.testInit.clearDatabase()
        return

    def testA_ProcessPool(self):
        """
        _testProcessPool_

        """
        raise nose.SkipTest
        config = self.testInit.getConfiguration()
        config.Agent.useHeartbeat = False
        self.testInit.generateWorkDir(config)

        processPool = ProcessPool("ProcessPool_t.ProcessPoolTestWorker",
                                  totalSlaves = 1,
                                  componentDir = config.General.workDir,
                                  config = config,
                                  namespace = "WMCore_t")

        processPool.enqueue(["One", "Two", "Three"])
        result =  processPool.dequeue(3)

        self.assertEqual(len(result), 3, "Error: Expected three items back.")
        self.assertTrue( "One" in result)
        self.assertTrue( "Two" in result)
        self.assertTrue( "Three" in result)

        return

    def testB_ProcessPoolStress(self):
        """
        _testProcessPoolStress_

        """
        raise nose.SkipTest
        config = self.testInit.getConfiguration()
        config.Agent.useHeartbeat = False
        self.testInit.generateWorkDir(config)

        processPool = ProcessPool("ProcessPool_t.ProcessPoolTestWorker",
                                  totalSlaves = 1,
                                  componentDir = config.General.workDir,
                                  namespace = "WMCore_t",
                                  config = config)

        result = None
        input  = None
        for i in range(1000):
            input = []
            while i > 0:
                input.append("COMMAND%s" % i)
                i -= 1

            processPool.enqueue(input)
            result =  processPool.dequeue(len(input))

            self.assertEqual(len(result), len(input),
                             "Error: Wrong number of results returned.")

        for k in result:
            self.assertTrue(k in input)

        return


    def testC_MultiPool(self):
        """
        _testMultiPool_

        Run a test with multiple workers
        """
        raise nose.SkipTest
        config = self.testInit.getConfiguration()
        config.Agent.useHeartbeat = False
        self.testInit.generateWorkDir(config)

        processPool = ProcessPool("ProcessPool_t.ProcessPoolTestWorker",
                                  totalSlaves = 3,
                                  componentDir = config.General.workDir,
                                  namespace = "WMCore_t",
                                  config = config)

        for i in range(100):
            input = []
            while i > 0:
                input.append("COMMAND%s" % i)
                i -= 1

            processPool.enqueue(input)
            result =  processPool.dequeue(len(input))

            self.assertEqual(len(result), len(input),
                             "Error: Wrong number of results returned.")





if __name__ == "__main__":
    unittest.main()
