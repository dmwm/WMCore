#!/usr/bin/env python
"""
    WorkQueueElement unit tests
"""
from __future__ import (print_function, division)
import unittest
import os
from WMCore.WMBase import getTestBase
from WMCore.Wrappers import JsonWrapper
from WMCore.WorkQueue.DataStructs.WorkQueueElementsSummary import WorkQueueElementsSummary
from WMCore.WorkQueue.DataStructs.WorkQueueElement import WorkQueueElement


class WorkQueueElementsSummaryTest(unittest.TestCase):
    
    def setUp(self):
        
        
        filePath = os.path.join(getTestBase(),
                                    "WMCore_t/WorkQueue_t/DataStructs_t/wq_available_elements.json")
        with open(filePath, "r") as f:
            gqData = JsonWrapper.load(f)
        
        self.gqElements = []
        for ele in gqData:
            self.gqElements.append(WorkQueueElement(**ele))


    
    def testElementsWithHigherPriorityInSameSites(self):
        """Id calculated correctly"""
        
        gqSummary = WorkQueueElementsSummary(self.gqElements)
        testReq = "riahi_TEST_HELIX_0911-T1_UK_RALBackfill_151119_190209_6101"
        filteredElements = gqSummary.elementsWithHigherPriorityInSameSites(testReq)
        
        wqSummary = WorkQueueElementsSummary(filteredElements)
        wqElements = wqSummary.getWQElementResultsByReauest()
        
        self.assertEqual(gqSummary.getPossibleSitesByRequest(testReq), set(['T1_UK_RAL']))
        priority = gqSummary.getWQElementResultsByReauest(testReq)['Priority']
        self.assertEqual(priority, 50000)
        
        self.assertEqual(len(wqElements), 27)
        
        jobs = 0
        for req in wqElements:
            jobs += wqElements[req]['Jobs']
            self.assertTrue(wqElements[req]['Priority'] >= priority)
        self.assertEqual(jobs, 222669)

if __name__ == '__main__':
    unittest.main()