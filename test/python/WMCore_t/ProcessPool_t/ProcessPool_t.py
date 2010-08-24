"""
_ProcessPool_t

Unit tests for the ProcessPool class.
"""

import unittest

from WMCore.ProcessPool.ProcessPool import ProcessPool
from WMQuality.TestInit import TestInit

class ProcessPoolTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        """
        self.testInit = TestInit(__file__)
        return

    def testA_ProcessPool(self):
        """
        _testProcessPool_

        """
        config = self.testInit.getConfiguration()
        self.testInit.generateWorkDir(config)
                
        processPool = ProcessPool("ProcessPool_t.ProcessPoolTestWorker",
                                  totalSlaves = 1,
                                  componentDir = config.General.workDir,
                                  config = config,
                                  namespace = "WMCore_t",
                                  slaveInit = {})
                                 
        processPool.enqueue(["One", "Two", "Three"])
        result =  processPool.dequeue(3)

        assert len(result) == 3, \
               "Error: Expected three items back."
        assert "One" in result, \
               "Error: Missing data from dequeue()"
        assert "Two" in result, \
               "Error: Missing data from dequeue()"
        assert "Three" in result, \
               "Error: Missing data from dequeue()"        
               
        return

    def testB_ProcessPoolStress(self):
        """
        _testProcessPoolStress_

        """
        config = self.testInit.getConfiguration()
        self.testInit.generateWorkDir(config)
                
        processPool = ProcessPool("ProcessPool_t.ProcessPoolTestWorker",
                                  totalSlaves = 1,
                                  componentDir = config.General.workDir,
                                  namespace = "WMCore_t",
                                  config = config,
                                  slaveInit = {})

        for i in range(1000):
            input = []
            while i > 0:
                input.append("COMMAND%s" % i)
                i -= 1

            processPool.enqueue(input)
            result =  processPool.dequeue(len(input))
        
            assert len(result) == len(input), \
                   "Error: Wrong number of results returned."

        return


    def testC_MultiPool(self):
        """
        _testMultiPool_

        Run a test with multiple workers
        """

        config = self.testInit.getConfiguration()
        self.testInit.generateWorkDir(config)
                
        processPool = ProcessPool("ProcessPool_t.ProcessPoolTestWorker",
                                  totalSlaves = 3,
                                  componentDir = config.General.workDir,
                                  namespace = "WMCore_t",
                                  config = config,
                                  slaveInit = {})

        for i in range(100):
            input = []
            while i > 0:
                input.append("COMMAND%s" % i)
                i -= 1

            processPool.enqueue(input)
            result =  processPool.dequeue(len(input))
        
            assert len(result) == len(input), \
                   "Error: Wrong number of results returned."
        

if __name__ == "__main__":
    unittest.main()
            
