#!/usr/bin/env python
"""
    WorkQueue.Policy.Start.MonteCarlo tests
"""




import unittest
from WMCore.WorkQueue.Policy.Start.MonteCarlo import MonteCarlo
from WMCore_t.WorkQueue_t.WorkQueue_t import TestMonteCarloFactory, mcArgs
from WMCore_t.WorkQueue_t.WorkQueue_t import getFirstTask
from WMCore_t.WMSpec_t.samples.MultiMergeProductionWorkload import workload as MultiMergeProductionWorkload
from WMCore_t.WMSpec_t.samples.MultiTaskProductionWorkload import workload as MultiTaskProductionWorkload

class MonteCarloTestCase(unittest.TestCase):

    splitArgs = dict(SliceType = 'NumEvents', SliceSize = 100)

    def testBasicProductionWorkload(self):
        """Basic Production Workload"""
        BasicProductionWorkload = TestMonteCarloFactory()('MonteCarloWorkload', mcArgs)
        getFirstTask(BasicProductionWorkload).addProduction(totalevents = 1000)
        getFirstTask(BasicProductionWorkload).setSiteWhitelist(['SiteA', 'SiteB'])
        for task in BasicProductionWorkload.taskIterator():
            units = MonteCarlo(**self.splitArgs)(BasicProductionWorkload, task)

            self.assertEqual(10, len(units))
            for unit in units:
                self.assertEqual(1, unit['Jobs'])
                self.assertEqual(unit['WMSpec'], BasicProductionWorkload)
                self.assertEqual(unit['Task'], task)


    def testMultiMergeProductionWorkload(self):
        """Multi merge production workload"""
        getFirstTask(MultiMergeProductionWorkload).setSiteWhitelist(['SiteA', 'SiteB'])
        for task in MultiMergeProductionWorkload.taskIterator():
            units = MonteCarlo(**self.splitArgs)(MultiMergeProductionWorkload, task)

            self.assertEqual(10.0, len(units))
            for unit in units:
                self.assertEqual(1, unit['Jobs'])
                self.assertEqual(unit['WMSpec'], MultiMergeProductionWorkload)
                self.assertEqual(unit['Task'], task)


    def testMultiTaskProcessingWorkload(self):
        """Multi Task Processing Workflow"""
        count = 0
        tasks = []
        getFirstTask(MultiTaskProductionWorkload).setSiteWhitelist(['SiteA', 'SiteB'])
        for task in MultiTaskProductionWorkload.taskIterator():
            count += 1
            units = MonteCarlo(**self.splitArgs)(MultiTaskProductionWorkload, task)

            self.assertEqual(10 * count, len(units))
            for unit in units:
                self.assertEqual(1, unit['Jobs'])
                self.assertEqual(unit['WMSpec'], MultiTaskProductionWorkload)
                self.assertEqual(unit['Task'], task)
        self.assertEqual(count, 2)


if __name__ == '__main__':
    unittest.main()
