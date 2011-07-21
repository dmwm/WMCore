#!/usr/bin/env python
"""
    WorkQueue.Policy.Start.MonteCarlo tests
"""

import unittest
from WMCore.WorkQueue.Policy.Start.MonteCarlo import MonteCarlo
from WMQuality.Emulators.WMSpecGenerator.Samples.TestMonteCarloWorkload \
    import monteCarloWorkload, getMCArgs

from WMCore_t.WMSpec_t.samples.MultiMergeProductionWorkload \
    import workload as MultiMergeProductionWorkload
from WMCore_t.WMSpec_t.samples.MultiTaskProductionWorkload \
    import workload as MultiTaskProductionWorkload
from WMCore.WorkQueue.WorkQueueExceptions import *
from WMCore_t.WorkQueue_t.WorkQueue_t import getFirstTask
from WMQuality.Emulators.DataBlockGenerator import Globals

mcArgs = getMCArgs()

class MonteCarloTestCase(unittest.TestCase):

    splitArgs = dict(SliceType = 'NumEvents', SliceSize = 100, MaxJobsPerElement = 1)

    def testBasicProductionWorkload(self):
        """Basic Production Workload"""
        # change split defaults for this test
        totalevents = 1000000
        splitArgs = dict(SliceType = 'NumEvents', SliceSize = 100, MaxJobsPerElement = 5)

        BasicProductionWorkload = monteCarloWorkload('MonteCarloWorkload', mcArgs)
        getFirstTask(BasicProductionWorkload).setSiteWhitelist(['T2_XX_SiteA', 'T2_XX_SiteB'])
        getFirstTask(BasicProductionWorkload).addProduction(totalevents = totalevents)
        getFirstTask(BasicProductionWorkload).setSiteWhitelist(['T2_XX_SiteA', 'T2_XX_SiteB'])
        for task in BasicProductionWorkload.taskIterator():
            units = MonteCarlo(**splitArgs)(BasicProductionWorkload, task)

            self.assertEqual(int(totalevents / (splitArgs['SliceSize'] * splitArgs['MaxJobsPerElement'])),
                             len(units))
            first_event = 1
            first_lumi = 1
            first_run = 1
            for unit in units:
                self.assertEqual(int(splitArgs['MaxJobsPerElement']), unit['Jobs'])
                self.assertEqual(unit['WMSpec'], BasicProductionWorkload)
                self.assertEqual(unit['Task'], task)
                self.assertEqual(unit['Mask']['FirstEvent'], first_event)
                self.assertEqual(unit['Mask']['FirstLumi'], first_lumi)
                last_event = first_event + (self.splitArgs['SliceSize'] * unit['Jobs']) - 1
                self.assertEqual(unit['Mask']['LastEvent'], last_event)
                self.assertEqual(unit['Mask']['LastLumi'], first_lumi + unit['Jobs'] - 1)
                self.assertEqual(unit['Mask']['FirstRun'], first_run)
                first_event = last_event + 1
                first_lumi += unit['Jobs'] # one lumi per job
            self.assertEqual(last_event, totalevents)


    def testMultiMergeProductionWorkload(self):
        """Multi merge production workload"""
        getFirstTask(MultiMergeProductionWorkload).setSiteWhitelist(['T2_XX_SiteA', 'T2_XX_SiteB'])
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
        getFirstTask(MultiTaskProductionWorkload).setSiteWhitelist(['T2_XX_SiteA', 'T2_XX_SiteB'])
        for task in MultiTaskProductionWorkload.taskIterator():
            count += 1
            units = MonteCarlo(**self.splitArgs)(MultiTaskProductionWorkload, task)

            self.assertEqual(10 * count, len(units))
            for unit in units:
                self.assertEqual(1, unit['Jobs'])
                self.assertEqual(unit['WMSpec'], MultiTaskProductionWorkload)
                self.assertEqual(unit['Task'], task)
        self.assertEqual(count, 2)

    def testInvalidSpecs(self):
        """Specs with no work"""
        # no whitelist
        mcspec = monteCarloWorkload('testProcessingInvalid', mcArgs)
        getFirstTask(mcspec).setSiteWhitelist(None)
        for task in mcspec.taskIterator():
            self.assertRaises(WorkQueueWMSpecError, MonteCarlo(), mcspec, task)
        getFirstTask(mcspec).setSiteWhitelist([])

        # 0 events
        getFirstTask(mcspec).addProduction(totalevents = 0)
        for task in mcspec.taskIterator():
            self.assertRaises(WorkQueueNoWorkError, MonteCarlo(), mcspec, task)

if __name__ == '__main__':
    unittest.main()
