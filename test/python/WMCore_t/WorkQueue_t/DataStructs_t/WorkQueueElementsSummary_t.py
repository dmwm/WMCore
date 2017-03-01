#!/usr/bin/env python
"""
    WorkQueueElement unit tests
"""
from __future__ import (print_function, division)

import json
import os
import unittest

from WMCore.WMBase import getTestBase
from WMCore.WorkQueue.DataStructs.WorkQueueElement import WorkQueueElement
from WMCore.WorkQueue.DataStructs.WorkQueueElementsSummary import WorkQueueElementsSummary


class WorkQueueElementsSummaryTest(unittest.TestCase):

    def setUp(self):


        filePath = os.path.join(getTestBase(),
                                    "WMCore_t/WorkQueue_t/DataStructs_t/wq_available_elements.json")
        with open(filePath, "r") as f:
            gqData = json.load(f)

        self.gqElements = []
        for ele in gqData:
            self.gqElements.append(WorkQueueElement(**ele))



    def testElementsWithHigherPriorityInSameSites(self):
        """Id calculated correctly"""

        gqSummary = WorkQueueElementsSummary(self.gqElements)
        testReq = "riahi_TEST_HELIX_0911-T1_UK_RALBackfill_151119_190209_6101"
        filteredElements = gqSummary.elementsWithHigherPriorityInSameSites(testReq, returnFormat="list")

        wqSummary = WorkQueueElementsSummary(filteredElements)
        wqElements = wqSummary.getWQElementResultsByRequest()

        self.assertEqual(gqSummary.getPossibleSitesByRequest(testReq), set(['T1_UK_RAL']))
        priority = gqSummary.getWQElementResultsByRequest(testReq)['Priority']
        self.assertEqual(priority, 50000)

        self.assertEqual(len(wqElements), 2)

        jobs = 0
        for req in wqElements:
            jobs += wqElements[req]['Jobs']
            self.assertTrue(wqElements[req]['Priority'] >= priority)
        self.assertEqual(jobs, 1942)

if __name__ == '__main__':
    unittest.main()
