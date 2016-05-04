#!/usr/bin/env python
"""
    WorkQueue.Policy.Start.MonteCarlo tests
"""
from __future__ import division
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
from WMQuality.Emulators.EmulatedUnitTest import EmulatedUnitTest
import math

mcArgs = getMCArgs()

class MonteCarloTestCase(EmulatedUnitTest):
    """Test case MonteCarlo Workload"""

    splitArgs = dict(SliceType='NumEvents', SliceSize=100, MaxJobsPerElement=1)

    def testBasicProductionWorkload(self):
        """Basic Production Workload"""
        # change split defaults for this test
        totalevents = 1000000
        splitArgs = dict(SliceType='NumberOfEvents', SliceSize=100, MaxJobsPerElement=5)
        mcArgs["EventsPerJob"] = 100
        BasicProductionWorkload = monteCarloWorkload('MonteCarloWorkload', mcArgs)
        getFirstTask(BasicProductionWorkload).setSiteWhitelist(['T2_XX_SiteA', 'T2_XX_SiteB'])
        getFirstTask(BasicProductionWorkload).addProduction(totalEvents=totalevents)
        getFirstTask(BasicProductionWorkload).setSiteWhitelist(['T2_XX_SiteA', 'T2_XX_SiteB'])
        for task in BasicProductionWorkload.taskIterator():
            units, _ = MonteCarlo(**splitArgs)(BasicProductionWorkload, task)

            SliceSize = BasicProductionWorkload.startPolicyParameters()['SliceSize']
            self.assertEqual(math.ceil(float(totalevents) / (SliceSize * splitArgs['MaxJobsPerElement'])), len(units))
            first_event = 1
            first_lumi = 1
            first_run = 1
            for unit in units:
                self.assertTrue(unit['Jobs'] <= splitArgs['MaxJobsPerElement'])
                self.assertEqual(unit['WMSpec'], BasicProductionWorkload)
                self.assertEqual(unit['Task'], task)
                self.assertEqual(unit['Mask']['FirstEvent'], first_event)
                self.assertEqual(unit['Mask']['FirstLumi'], first_lumi)
                last_event = first_event + (SliceSize * unit['Jobs']) - 1
                if last_event > totalevents:
                    # this should be the last unit of work
                    last_event = totalevents
                last_lumi = first_lumi + unit['Jobs'] - 1
                self.assertEqual(unit['Mask']['LastEvent'], last_event)
                self.assertEqual(unit['Mask']['LastLumi'], last_lumi)
                self.assertEqual(unit['Mask']['FirstRun'], first_run)
                self.assertEqual(last_lumi - first_lumi + 1, unit['NumberOfLumis'])
                self.assertEqual(last_event - first_event + 1, unit['NumberOfEvents'])
                first_event = last_event + 1
                first_lumi += unit['Jobs'] # one lumi per job
            self.assertEqual(unit['Mask']['LastEvent'], totalevents)

    def testLHEProductionWorkload(self):
        """
        _testLHEProductionWorkload_

        Make sure that splitting by event plus events in per lumi works

        """
        totalevents = 542674
        splitArgs = dict(SliceType='NumEvents', SliceSize=47, MaxJobsPerElement=5, SubSliceType='NumEventsPerLumi', SubSliceSize=13)

        LHEProductionWorkload = monteCarloWorkload('MonteCarloWorkload', mcArgs)
        LHEProductionWorkload.setJobSplittingParameters(
            getFirstTask(LHEProductionWorkload).getPathName(),
            'EventBased',
            {'events_per_job': splitArgs['SliceSize'],
             'events_per_lumi': splitArgs['SubSliceSize']})
        getFirstTask(LHEProductionWorkload).setSiteWhitelist(['T2_XX_SiteA', 'T2_XX_SiteB'])
        getFirstTask(LHEProductionWorkload).addProduction(totalEvents=totalevents)
        getFirstTask(LHEProductionWorkload).setSiteWhitelist(['T2_XX_SiteA', 'T2_XX_SiteB'])
        for task in LHEProductionWorkload.taskIterator():
            units, _ = MonteCarlo(**splitArgs)(LHEProductionWorkload, task)

            SliceSize = LHEProductionWorkload.startPolicyParameters()['SliceSize']
            self.assertEqual(math.ceil(float(totalevents) / (SliceSize * splitArgs['MaxJobsPerElement'])),
                             len(units))
            first_event = 1
            first_lumi = 1
            first_run = 1
            lumis_per_job = int(math.ceil(float(SliceSize) / splitArgs['SubSliceSize']))
            for unit in units:
                self.assertTrue(unit['Jobs'] <= splitArgs['MaxJobsPerElement'])
                self.assertEqual(unit['WMSpec'], LHEProductionWorkload)
                self.assertEqual(unit['Task'], task)
                self.assertEqual(unit['Mask']['FirstEvent'], first_event)
                self.assertEqual(unit['Mask']['FirstLumi'], first_lumi)
                last_event = first_event + (SliceSize * unit['Jobs']) - 1
                last_lumi = first_lumi + (lumis_per_job * unit['Jobs']) - 1
                if last_event > totalevents:
                    # this should be the last unit of work
                    last_event = totalevents
                    last_lumi = first_lumi
                    last_lumi += math.ceil(((last_event - first_event + 1) % SliceSize) / splitArgs['SubSliceSize'])
                    last_lumi += (lumis_per_job * (unit['Jobs'] - 1)) - 1
                self.assertEqual(unit['Mask']['LastEvent'], last_event)
                self.assertEqual(unit['Mask']['LastLumi'], last_lumi)
                self.assertEqual(unit['Mask']['FirstRun'], first_run)
                first_event = last_event + 1
                first_lumi = last_lumi + 1
            self.assertEqual(unit['Mask']['LastEvent'], totalevents)

    def testExtremeSplits(self):
        """
        _testExtremeSplits_

        Make sure that the protection to avoid going over 2^32 works

        """
        totalevents = 2**34
        splitArgs = dict(SliceType='NumEvents', SliceSize=2**30, MaxJobsPerElement=7)

        LHEProductionWorkload = monteCarloWorkload('MonteCarloWorkload', mcArgs)
        LHEProductionWorkload.setJobSplittingParameters(
            getFirstTask(LHEProductionWorkload).getPathName(),
            'EventBased',
            {'events_per_job': splitArgs['SliceSize'],
             'events_per_lumi': splitArgs['SliceSize']})
        getFirstTask(LHEProductionWorkload).setSiteWhitelist(['T2_XX_SiteA', 'T2_XX_SiteB'])
        getFirstTask(LHEProductionWorkload).addProduction(totalEvents=totalevents)
        getFirstTask(LHEProductionWorkload).setSiteWhitelist(['T2_XX_SiteA', 'T2_XX_SiteB'])
        for task in LHEProductionWorkload.taskIterator():
            units, _ = MonteCarlo(**splitArgs)(LHEProductionWorkload, task)

            SliceSize = LHEProductionWorkload.startPolicyParameters()['SliceSize']
            self.assertEqual(math.ceil(float(totalevents) / (SliceSize * splitArgs['MaxJobsPerElement'])),
                             len(units))
            self.assertEqual(len(units), 3, "Should produce 3 units")

            unit1 = units[0]
            unit2 = units[1]
            unit3 = units[2]

            self.assertEqual(unit1['Jobs'],
                             7, 'First unit produced more jobs than expected')
            self.assertEqual(unit1['Mask']['FirstEvent'], 1, 'First unit has a wrong first event')
            self.assertEqual(unit1['Mask']['LastEvent'], 7*(2**30), 'First unit has a wrong last event')
            self.assertEqual(unit2['Jobs'], 7, 'Second unit produced more jobs than expected')
            self.assertEqual(unit2['Mask']['FirstEvent'], 2**30 + 1, 'Second unit has a wrong first event')
            self.assertEqual(unit2['Mask']['LastEvent'], 8*(2**30), 'Second unit has a wrong last event')
            self.assertEqual(unit3['Jobs'], 2, 'Third unit produced more jobs than expected')
            self.assertEqual(unit3['Mask']['FirstEvent'], 2*(2**30) + 1, 'Third unit has a wrong first event')
            self.assertEqual(unit3['Mask']['LastEvent'], 4*(2**30), 'First unit has a wrong last event')

    def testShiftedStartSplitting(self):
        """
        _testShiftedStartSplitting_

        Make sure that splitting by event plus events in per lumi works
        when the first event and lumi is not 1

        """
        totalevents = 542674
        splitArgs = dict(SliceType='NumEvents', SliceSize=47, MaxJobsPerElement=5, SubSliceType='NumEventsPerLumi', SubSliceSize=13)

        LHEProductionWorkload = monteCarloWorkload('MonteCarloWorkload', mcArgs)
        LHEProductionWorkload.setJobSplittingParameters(getFirstTask(LHEProductionWorkload).getPathName(), 'EventBased', {'events_per_job': splitArgs['SliceSize'], 'events_per_lumi': splitArgs['SubSliceSize']})
        getFirstTask(LHEProductionWorkload).setSiteWhitelist(['T2_XX_SiteA', 'T2_XX_SiteB'])
        getFirstTask(LHEProductionWorkload).addProduction(totalEvents=totalevents)
        getFirstTask(LHEProductionWorkload).setFirstEventAndLumi(50, 100)
        getFirstTask(LHEProductionWorkload).setSiteWhitelist(['T2_XX_SiteA', 'T2_XX_SiteB'])
        for task in LHEProductionWorkload.taskIterator():
            units, _ = MonteCarlo(**splitArgs)(LHEProductionWorkload, task)

            SliceSize = LHEProductionWorkload.startPolicyParameters()['SliceSize']
            self.assertEqual(math.ceil(float(totalevents) / (SliceSize * splitArgs['MaxJobsPerElement'])), len(units))
            first_event = 50
            first_lumi = 100
            first_run = 1
            lumis_per_job = int(math.ceil(float(SliceSize) / splitArgs['SubSliceSize']))
            for unit in units:
                self.assertTrue(unit['Jobs'] <= splitArgs['MaxJobsPerElement'])
                self.assertEqual(unit['WMSpec'], LHEProductionWorkload)
                self.assertEqual(unit['Task'], task)
                self.assertEqual(unit['Mask']['FirstEvent'], first_event)
                self.assertEqual(unit['Mask']['FirstLumi'], first_lumi)
                last_event = first_event + (SliceSize * unit['Jobs']) - 1
                last_lumi = first_lumi + (lumis_per_job * unit['Jobs']) - 1
                if last_event > totalevents:
                    # this should be the last unit of work
                    last_event = totalevents + 50 - 1
                    last_lumi = first_lumi
                    last_lumi += math.ceil(((last_event - first_event + 1) % SliceSize) / splitArgs['SubSliceSize'])
                    last_lumi += (lumis_per_job * (unit['Jobs'] - 1)) - 1
                self.assertEqual(unit['Mask']['LastEvent'], last_event)
                self.assertEqual(unit['Mask']['LastLumi'], last_lumi)
                self.assertEqual(unit['Mask']['FirstRun'], first_run)
                first_event = last_event + 1
                first_lumi = last_lumi + 1
            self.assertEqual(unit['Mask']['LastEvent'], totalevents + 50 - 1)


    def testMultiMergeProductionWorkload(self):
        """Multi merge production workload"""
        getFirstTask(MultiMergeProductionWorkload).setSiteWhitelist(['T2_XX_SiteA', 'T2_XX_SiteB'])
        getFirstTask(MultiMergeProductionWorkload).setFirstEventAndLumi(1, 1)
        for task in MultiMergeProductionWorkload.taskIterator():
            units, _ = MonteCarlo(**self.splitArgs)(MultiMergeProductionWorkload, task)

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
            task.setFirstEventAndLumi(1, 1)
            units, _ = MonteCarlo(**self.splitArgs)(MultiTaskProductionWorkload, task)

            self.assertEqual(10 * count, len(units))
            for unit in units:
                self.assertEqual(1, unit['Jobs'])
                self.assertEqual(unit['WMSpec'], MultiTaskProductionWorkload)
                self.assertEqual(unit['Task'], task)
        self.assertEqual(count, 2)

    def testInvalidSpecs(self):
        """Specs with no work"""
        mcspec = monteCarloWorkload('testProcessingInvalid', mcArgs)
        # 0 events
        getFirstTask(mcspec).addProduction(totalEvents=0)
        for task in mcspec.taskIterator():
            self.assertRaises(WorkQueueNoWorkError, MonteCarlo(), mcspec, task)

        # -ve split size
        mcspec2 = monteCarloWorkload('testProdInvalid', mcArgs)
        mcspec2.data.policies.start.SliceSize = -100
        for task in mcspec2.taskIterator():
            self.assertRaises(WorkQueueWMSpecError, MonteCarlo(), mcspec2, task)

    def testContinuousSplittingSupport(self):
        """Doesn't support continuous splitting"""
        policyInstance = MonteCarlo(**self.splitArgs)
        self.assertFalse(policyInstance.supportsWorkAddition(), "MonteCarlo instance should not support continuous splitting")
        self.assertRaises(NotImplementedError, policyInstance.newDataAvailable, *[None, None])
        self.assertRaises(NotImplementedError, policyInstance.modifyPolicyForWorkAddition, *[None])
        return

if __name__ == '__main__':
    unittest.main()
