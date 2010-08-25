#!/usr/bin/env python
"""

"""

__revision__ = "$Id: WorkQueue_t.py,v 1.1 2009/05/21 18:22:53 swakef Exp $"
__version__ = "$Revision: 1.1 $"

import unittest
from WMCore.WorkQueue.WorkQueue import WorkQueue, _WQElement


class WorkQueueElementTest(unittest.TestCase):
    """
    _WorkQueueElementTest_
    
    """

    def testOrdering(self):
        """
        Test priority sorting
        """
        a = _WQElement("/test/a", priority=1)
        b = _WQElement("/test/b", priority=100)
        self.assertTrue(b < a)


    def testMatch(self):
        """
        Test match function matches correctly
        """
        pass


    def runTest(self):
        self.testOrdering() 
        self.testMatch()


class WorkQueueTest(unittest.TestCase):
    """
    _WorkQueueTest_
    
    """

    def testEnQueue(self):
        """
        Create and enqueue a WMSpec, ensure splitting matches expectations
        """
        pass


    def testDeQueue(self):
        """
        Test we get expected work back from list
        """
        pass
        
        
    def testPriorityChange(self):
        """
        If an element has its priority changed check it position also changes
        """
        pass
        

    def runTest(self):
        self.testEnQueue() 
        self.testDeQueue()
        self.testPriorityChange()

if __name__ == "__main__":
    unittest.main()
