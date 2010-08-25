#!/usr/bin/env python
"""
    WorkQueue.Policy.Start.MonteCarlo tests
"""

__revision__ = "$Id: MonteCarlo_t.py,v 1.3 2010/01/05 18:19:39 swakef Exp $"
__version__ = "$Revision: 1.3 $"

import unittest
from WMCore.WorkQueue.Policy.Start.MonteCarlo import MonteCarlo
from WMCore_t.WMSpec_t.samples.BasicProductionWorkload import workload as BasicProductionWorkload
from WMCore_t.WMSpec_t.samples.MultiMergeProductionWorkload import workload as MultiMergeProductionWorkload
from WMCore_t.WMSpec_t.samples.MultiTaskProductionWorkload import workload as MultiTaskProductionWorkload

class MonteCarloTestCase(unittest.TestCase):

    splitArgs = dict(SliceType = 'NumEvents', SliceSize = 100)

    def testBasicProductionWorkload(self):
        """Basic Production Workload"""
        for task in BasicProductionWorkload.taskIterator():
            units = MonteCarlo(**self.splitArgs)(BasicProductionWorkload, task)

            self.assertEqual(10, len(units))
            for unit in units:
                self.assertEqual(1, unit['Jobs'])
                spec = unit['WMSpec']
                # ensure new spec object created for each work unit
                self.assertNotEqual(id(spec), id(BasicProductionWorkload))
                initialTask = spec.taskIterator().next()
                self.assertEqual(initialTask.totalEvents(), 100)


    def testMultiMergeProductionWorkload(self):
        """Multi merge production workload"""
        for task in MultiMergeProductionWorkload.taskIterator():
            units = MonteCarlo(**self.splitArgs)(MultiMergeProductionWorkload, task)

            self.assertEqual(10.0, len(units))
            for unit in units:
                self.assertEqual(1, unit['Jobs'])
                spec = unit['WMSpec']
                # ensure new spec object created for each work unit
                self.assertNotEqual(id(spec), id(BasicProductionWorkload))
                initialTask = spec.taskIterator().next()
                self.assertEqual(initialTask.totalEvents(), 100)


    def testMultiTaskProcessingWorkload(self):
        """Multi Task Processing Workflow"""
        count = 0
        spec_ids = []
        for task in MultiTaskProductionWorkload.taskIterator():
            count += 1
            units = MonteCarlo(**self.splitArgs)(MultiTaskProductionWorkload, task)

            self.assertEqual(10 * count, len(units))
            for unit in units:
                self.assertEqual(1, unit['Jobs'])
                spec = unit['WMSpec']
                # ensure new spec object created for each work unit
                self.assertNotEqual(id(spec), id(MultiTaskProductionWorkload))
                self.assert_(id(spec) not in spec_ids)
                spec_ids.append(id(spec))
                initialTask = spec.taskIterator().next()
                self.assertEqual(initialTask.totalEvents(), 100)
        self.assertEqual(count, 2)


if __name__ == '__main__':
    unittest.main()
