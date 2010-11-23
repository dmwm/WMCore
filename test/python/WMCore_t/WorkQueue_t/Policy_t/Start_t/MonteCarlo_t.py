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

mcArgs = getMCArgs()

def getFirstTask(wmspec):
    """Return the 1st top level task"""
    # http://www.logilab.org/ticket/8774
    # pylint: disable-msg=E1101,E1103
    return wmspec.taskIterator().next()

class MonteCarloTestCase(unittest.TestCase):

    splitArgs = dict(SliceType = 'NumEvents', SliceSize = 100, MaxJobsPerElement = 1)

    def testBasicProductionWorkload(self):
        """Basic Production Workload"""
        # change split defaults for this test
        splitArgs = dict(SliceType = 'NumEvents', SliceSize = 100, MaxJobsPerElement = 5)

        BasicProductionWorkload = monteCarloWorkload('MonteCarloWorkload', mcArgs)
        getFirstTask(BasicProductionWorkload).setSiteWhitelist(['SiteA', 'SiteB'])
        getFirstTask(BasicProductionWorkload).addProduction(totalevents = 1000)
        getFirstTask(BasicProductionWorkload).setSiteWhitelist(['SiteA', 'SiteB'])
        for task in BasicProductionWorkload.taskIterator():
            units = MonteCarlo(**splitArgs)(BasicProductionWorkload, task)

            self.assertEqual(int(1000 / (splitArgs['SliceSize'] * splitArgs['MaxJobsPerElement'])),
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
            self.assertEqual(last_event, 1000)


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
