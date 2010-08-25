#!/usr/bin/env python
"""
    WorkQueue.Policy.Start.MonteCarlo tests
"""

__revision__ = "$Id: MonteCarlo_t.py,v 1.1 2009/12/10 16:30:44 swakef Exp $"
__version__ = "$Revision: 1.1 $"

import unittest
from WMCore.WorkQueue.Policy.Start.MonteCarlo import MonteCarlo
from WMCore_t.WMSpec_t.samples.BasicProductionWorkload import workload as BasicProductionWorkload
from WMCore_t.WMSpec_t.samples.MultiMergeProductionWorkload import workload as MultiMergeProductionWorkload


class MonteCarloTestCase(unittest.TestCase):

    splitArgs = dict(SliceType = 'NumEvents', SliceSize = 100)

    def testBasicProductionWorkload(self):
        """Basic Production Workload"""
        units = MonteCarlo(**self.splitArgs)(BasicProductionWorkload)

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
        units = MonteCarlo(**self.splitArgs)(MultiMergeProductionWorkload)

        self.assertEqual(10.0, len(units))
        for unit in units:
            self.assertEqual(1, unit['Jobs'])
            spec = unit['WMSpec']
            # ensure new spec object created for each work unit
            self.assertNotEqual(id(spec), id(BasicProductionWorkload))
            initialTask = spec.taskIterator().next()
            self.assertEqual(initialTask.totalEvents(), 100)

if __name__ == '__main__':
    unittest.main()
