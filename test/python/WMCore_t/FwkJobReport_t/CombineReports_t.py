#!/usr/bin/env python
# encoding: utf-8
"""
CombineReports_t.py

Created by Dave Evans on 2011-07-01.
Copyright (c) 2011 Fermilab. All rights reserved.
"""

import unittest
import time

from WMCore.WMSpec.Steps.Executors.MulticoreCMSSW import readMultiJobReports
from WMCore.FwkJobReport.Report import Report
from WMCore.FwkJobReport.MulticoreUtils import Aggregator, updateMulticoreReport, AggrFunctions


def testReports():
    """
    _testReports_

    Build a bunch of performance reports for tests
    """
    reports = []

    report1 = Report("cmsRun1")
    reports.append(report1)
    performance = report1.report.performance
    performance.section_('memory')
    performance.memory.PeakValueRss = '943.285'
    performance.memory.PeakValueVsize = '1158.31'
    performance.section_('storage')
    performance.storage.writeTotalMB = 653.823
    performance.storage.readPercentageOps = 1.0
    performance.storage.readAveragekB = 127711.451429
    performance.storage.readTotalMB = 3492.11
    performance.storage.readNumOps = 5149.0
    performance.storage.readCachePercentageOps = 0.0
    performance.storage.readMBSec = 0.0136159502792
    performance.storage.readMaxMSec = 39836.4
    performance.storage.readTotalSecs = 0
    performance.storage.writeTotalSecs = 9555720.0
    performance.section_('summaries')
    performance.section_('cpu')
    performance.cpu.TotalJobCPU = '5105.35'
    performance.cpu.TotalEventCPU = '5035.17'
    performance.cpu.AvgEventCPU = '4.1003'
    performance.cpu.AvgEventTime = '4.63966'
    performance.cpu.MinEventCPU = '0.672898'
    performance.cpu.MaxEventTime = '49.8404'
    performance.cpu.TotalJobTime = '5697.5'
    performance.cpu.MinEventTime = '0.699774'
    performance.cpu.MaxEventCPU = '20.7558'


    report2 = Report("cmsRun1")
    reports.append(report2)
    performance = report2.report.performance
    performance.section_('memory')
    performance.memory.PeakValueRss = '943.285'
    performance.memory.PeakValueVsize = '1158.31'
    performance.section_('storage')
    performance.storage.writeTotalMB = 648.459
    performance.storage.readPercentageOps = 1.0
    performance.storage.readAveragekB = 127711.451429
    performance.storage.readTotalMB = 3492.11
    performance.storage.readNumOps = 5207.0
    performance.storage.readCachePercentageOps = 0.0
    performance.storage.readMBSec = 0.0129124590952
    performance.storage.readMaxMSec = 44769.7
    performance.storage.readTotalSecs = 0
    performance.storage.writeTotalSecs = 10207800.0
    performance.section_('summaries')
    performance.section_('cpu')
    performance.cpu.TotalJobCPU = '5102.21'
    performance.cpu.TotalEventCPU = '5031.88'
    performance.cpu.AvgEventCPU = '4.11774'
    performance.cpu.AvgEventTime = '4.65631'
    performance.cpu.MinEventCPU = '0.495924'
    performance.cpu.MaxEventTime = '48.5577'
    performance.cpu.TotalJobTime = '5690.01'
    performance.cpu.MinEventTime = '0.496432'
    performance.cpu.MaxEventCPU = '19.485'

    report3 = Report("cmsRun1")
    reports.append(report3)
    performance = report3.report.performance
    performance.section_('memory')
    performance.memory.PeakValueRss = '943.285'
    performance.memory.PeakValueVsize = '1158.31'
    performance.section_('storage')
    performance.storage.writeTotalMB = 648.325
    performance.storage.readPercentageOps = 1.0
    performance.storage.readAveragekB = 127711.451429
    performance.storage.readTotalMB = 3492.11
    performance.storage.readNumOps = 5232.0
    performance.storage.readCachePercentageOps = 0.0
    performance.storage.readMBSec = 0.0129207721139
    performance.storage.readMaxMSec = 62902.1
    performance.storage.readTotalSecs = 0
    performance.storage.writeTotalSecs = 7514260.0
    performance.section_('summaries')
    performance.section_('cpu')
    performance.cpu.TotalJobCPU = '5092.12'
    performance.cpu.TotalEventCPU = '5023.96'
    performance.cpu.AvgEventCPU = '4.07789'
    performance.cpu.AvgEventTime = '4.62357'
    performance.cpu.MinEventCPU = '0.686896'
    performance.cpu.MaxEventTime = '66.9332'
    performance.cpu.TotalJobTime = '5696.24'
    performance.cpu.MinEventTime = '0.686953'
    performance.cpu.MaxEventCPU = '24.7832'

    report4 = Report("cmsRun1")
    reports.append(report4)
    performance = report4.report.performance
    performance.section_('memory')
    performance.memory.PeakValueRss = '943.285'
    performance.memory.PeakValueVsize = '1158.31'
    performance.section_('storage')
    performance.storage.writeTotalMB = 641.113
    performance.storage.readPercentageOps = 1.0
    performance.storage.readAveragekB = 127711.451429
    performance.storage.readTotalMB = 3492.11
    performance.storage.readNumOps = 5162.0
    performance.storage.readCachePercentageOps = 0.0
    performance.storage.readMBSec = 0.0114592719719
    performance.storage.readMaxMSec = 48329.6
    performance.storage.readTotalSecs = 0
    performance.storage.writeTotalSecs = 6999670.0
    performance.section_('summaries')
    performance.section_('cpu')
    performance.cpu.TotalJobCPU = '5086.71'
    performance.cpu.TotalEventCPU = '5018.28'
    performance.cpu.AvgEventCPU = '4.168'
    performance.cpu.AvgEventTime = '4.72844'
    performance.cpu.MinEventCPU = '0.6669'
    performance.cpu.MaxEventTime = '55.1588'
    performance.cpu.TotalJobTime = '5693.04'
    performance.cpu.MinEventTime = '0.667495'
    performance.cpu.MaxEventCPU = '20.8438'

    return reports


class CombineReports_t(unittest.TestCase):
    """
    TestCase to combine performance reports for a multicore job

    """
    def testA(self):
        """ read a set of reports"""
        reps = testReports()
        try:
            aggr = Aggregator()
        except Exception, ex:
            msg = "Error instantiating Aggregator object:\n%s" % str(ex)
            self.fail(msg)
        try:
            [ aggr.add(rep.report.performance) for rep in reps]
        except Exception, ex:
            msg = "Error adding report data to Aggregator: %s" % str(ex)
            self.fail(msg)
        try:
            report = aggr.aggregate()
        except Exception, ex:
            msg = "Error invoking aggregation of report data: %s" %str(ex)
            self.fail(msg)
        timestamp = time.time()
        mergetimes = [ 1,2,3,4,5,6,7,8 ]

        finalReport = Report("cmsRun1")
        finalReport.report.performance = report

        updateMulticoreReport(finalReport, 8, timestamp , timestamp + 100 , timestamp - 1000, *mergetimes)

        performance = finalReport.report.performance
        self.assertEqual(performance.multicore.maxMergeTime , 8)
        self.assertEqual(performance.multicore.minMergeTime , 1)
        self.assertEqual(performance.multicore.coresUsed , 4)

        self.assertEqual(performance.cpu.TotalJobTime,
            AggrFunctions['cpu.TotalJobTime'](aggr.values['cpu.TotalJobTime']))
        self.assertEqual(performance.storage.readTotalSecs,
            AggrFunctions['storage.readTotalSecs'](aggr.values['storage.readTotalSecs']))
        self.assertEqual(performance.memory.PeakValueVsize,
            AggrFunctions['memory.PeakValueVsize'](aggr.values['memory.PeakValueVsize']))

        self.assertEqual(performance.multicore.stepEfficiency, 1)

if __name__ == '__main__':
    unittest.main()
