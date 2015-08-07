#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
__DashboardReporter_t__
Unit tests for the dashboard reporter
Created on Fri Jun  8 11:21:07 2012

@author: dballest
"""

import unittest

import os

from WMCore.FwkJobReport.Report import Report
from WMCore.Services.Dashboard.DashboardReporter import DashboardReporter
from WMCore.DataStructs.Job import Job
from WMCore.WMBase import getTestBase

from WMCore_t.Services_t.Dashboard_t.reportSamples import ErrorSample
from WMCore_t.Services_t.Dashboard_t.reportSamples import MergeSample
from WMCore_t.Services_t.Dashboard_t.reportSamples import FallbackSample
from WMCore_t.Services_t.Dashboard_t.reportSamples import ProcessingSample

class DashboardReporterTest(unittest.TestCase):
    """
    _DashboardReporterTest_

    Unit tests for the dashboard reporter class.
    """
    def setUp(self):
        """
        _setUp_

        Setup a dashboard reporter
        """
        self.reporter = DashboardReporter(config=None)
        self.processingReport = ProcessingSample.report
        self.mergeReport = MergeSample.report
        self.errorReport = ErrorSample.report
        self.fallbackReport = FallbackSample.report

        self.twoFileFallbackXmlPath = os.path.join(getTestBase(), "WMCore_t/FwkJobReport_t/CMSSWTwoFileRemote.xml")
        self.pileupXmlPath = os.path.join(getTestBase(), "WMCore_t/FwkJobReport_t/CMSSWPileup.xml")

        return

    def tearDown(self):
        """
        _tearDown_

        Just get out
        """
        pass

    def trimNoneValues(self, package):
        """
        _trimNoneValues_

        Simple utility to trim the None values of a dictionary
        """
        trimmed = {}
        for key in package:
            if package[key] != None:
                trimmed[key] = package[key]
        return trimmed

    def createTestJob(self, fwjr):
        """
        _createTestJob_

        Creates a minimal job to report
        """
        job = Job('finishedJob')
        job['retry_count'] = 1
        job['workflow'] = 'testWorkflow'
        job['fwjr'] = fwjr

        return job

    def testHandleSteps(self):
        """
        _testHandleSteps_

        Check that we can extract the information from a completed job
        and report it

        """
        job = self.createTestJob(self.processingReport)
        self.reporter.handleSteps(job)
        job = self.createTestJob(self.mergeReport)
        self.reporter.handleSteps(job)
        job = self.createTestJob(self.errorReport)
        self.reporter.handleSteps(job)

    def testPerformanceReport(self):
        """
        _testPerformanceReport_

        Check that the performance information is extracted correctly for
        different reports
        """
        step = self.processingReport.retrieveStep('cmsRun1')
        perfInfo = self.reporter.getPerformanceInformation(step)
        self.assertEqual(len(self.trimNoneValues(perfInfo)), 21,
                         'Found less information than expected')
        self.assertEqual(perfInfo['PeakValueRss'], '891.617',
                         'Values do not match')
        self.assertEqual(perfInfo['readCachePercentageOps'], 0.995779157341,
                         'Values do not match')
        self.assertEqual(perfInfo['MaxEventTime'], '3.32538',
                         'Values do not match')

        step = self.processingReport.retrieveStep('logArch1')
        perfInfo = self.reporter.getPerformanceInformation(step)
        self.assertEqual(self.trimNoneValues(perfInfo), {},
                         'logArch1 performance info is not empty')

        step = self.processingReport.retrieveStep('stageOut1')
        perfInfo = self.reporter.getPerformanceInformation(step)
        self.assertEqual(self.trimNoneValues(perfInfo), {},
                         'stageOut1 performance info is not empty')

        step = self.errorReport.retrieveStep('cmsRun1')
        perfInfo = self.reporter.getPerformanceInformation(step)
        self.assertEqual(self.trimNoneValues(perfInfo), {},
                         'cmsRun1 performance info is not empty')

        step = self.errorReport.retrieveStep('logArch1')
        perfInfo = self.reporter.getPerformanceInformation(step)
        self.assertEqual(self.trimNoneValues(perfInfo), {},
                         'logArch1 performance info is not empty')

        step = self.errorReport.retrieveStep('stageOut1')
        perfInfo = self.reporter.getPerformanceInformation(step)
        self.assertEqual(self.trimNoneValues(perfInfo), {},
                         'stageOut1 performance info is not empty')


    def testEventInformationReport(self):
        """
        _testEventInformationReport_

        Check that the event information is extracted correctly for
        different reports
        """
        eventInfo = self.reporter.getEventInformation('cmsRun1',
                                                      self.processingReport)
        self.assertEqual(eventInfo['inputEvents'], 18192,
                         'Input events do not match')
        self.assertEqual(eventInfo['OutputEventInfo'].count('Run2012B-WElectron-PromptSkim-v1:USER:1603'), 1)
        self.assertEqual(eventInfo['OutputEventInfo'].count('Run2012B-LogErrorMonitor-PromptSkim-v1:USER:137'), 1)
        self.assertEqual(eventInfo['OutputEventInfo'].count('Run2012B-LogError-PromptSkim-v1:RAW-RECO:66'), 1)
        self.assertEqual(eventInfo['OutputEventInfo'].count('Run2012B-TOPElePlusJets-PromptSkim-v1:AOD:2320'), 1)
        self.assertEqual(eventInfo['OutputEventInfo'].count('Run2012B-HighMET-PromptSkim-v1:RAW-RECO:8'), 1)
        self.assertEqual(eventInfo['OutputEventInfo'].count('Run2012B-DiTau-PromptSkim-v1:RAW-RECO:192'), 1)

        eventInfo = self.reporter.getEventInformation('stageOut1',
                                                      self.processingReport)
        self.assertEqual(eventInfo, {},
                         'stageOut1 event info is not empty')

        eventInfo = self.reporter.getEventInformation('logArch1',
                                                      self.processingReport)
        self.assertEqual(eventInfo, {},
                         'logArch1 event info is not empty')

        eventInfo = self.reporter.getEventInformation('cmsRun1',
                                                      self.mergeReport)
        self.assertEqual(eventInfo['inputEvents'], 0,
                         'Input events do not match')
        self.assertEqual(eventInfo['OutputEventInfo'].count('Run2012B-LogError-PromptSkim-v1:RAW-RECO:0'), 1)

        eventInfo = self.reporter.getEventInformation('cmsRun1',
                                                      self.errorReport)
        self.assertEqual(eventInfo, {},
                         'Error report event info is not empty')

    def testFileInformation(self):
        """
        _testFileInformation_

        Check that the file information is extracted correctly for
        different reports
        """

        # First test the processingReport

        step = self.processingReport.retrieveStep('cmsRun1')
        fileInfo = self.reporter.getInputFilesInformation(step)

        fileReports = fileInfo['inputFiles'].split(';')
        self.assertEqual(2, len(fileReports))

        # Format is LFN, Status, Type (EDM), Local/Remote, Count
        report0 = (fileInfo['inputFiles'].split(';'))[0].split('::')
        report1 = (fileInfo['inputFiles'].split(';'))[1].split('::')

        self.assertEqual('1', report0[1])
        self.assertEqual('1', report1[1])
        self.assertEqual('Local', report0[3])
        self.assertEqual('Local', report1[3])
        self.assertEqual('1', report0[4])
        self.assertEqual('2', report1[4])

        step = self.processingReport.retrieveStep('logArch1')
        fileInfo = self.reporter.getInputFilesInformation(step)
        self.assertEqual(self.trimNoneValues(fileInfo), {},
                         'logArch1 file info is not empty')

        step = self.processingReport.retrieveStep('stageOut1')
        fileInfo = self.reporter.getInputFilesInformation(step)
        self.assertEqual(self.trimNoneValues(fileInfo), {},
                         'stageOut1 file info is not empty')

        # Now shorter test on mergeReport

        step = self.mergeReport.retrieveStep('cmsRun1')
        fileInfo = self.reporter.getInputFilesInformation(step)

        fileReports = fileInfo['inputFiles'].split(';')
        self.assertEqual(1, len(fileReports))

        report0 = (fileInfo['inputFiles'].split(';'))[0].split('::')

        self.assertEqual('1', report0[1])
        self.assertEqual('Local', report0[3])
        self.assertEqual('1', report0[4])

        # Now shorter test on errorReport
        step = self.errorReport.retrieveStep('cmsRun1')
        fileInfo = self.reporter.getInputFilesInformation(step)

        fileReports = fileInfo['inputFiles'].split(';')
        self.assertEqual(2, len(fileReports))

        # Format is LFN, Status, Type (EDM), Local/Remote, Count
        report0 = (fileInfo['inputFiles'].split(';'))[0].split('::')
        report1 = (fileInfo['inputFiles'].split(';'))[1].split('::')

        self.assertEqual('1', report0[1])
        self.assertEqual('0', report1[1])
        self.assertEqual('Local', report0[3])
        self.assertEqual('Local', report1[3])
        self.assertEqual('1', report0[4])
        self.assertEqual('2', report1[4])

        # And tests on the fallback report

        step = self.fallbackReport.retrieveStep('cmsRun1')
        fileInfo = self.reporter.getInputFilesInformation(step)

        fileReports = fileInfo['inputFiles'].split(';')
        self.assertEqual(1, len(fileReports))

        # Format is LFN, Status, Type (EDM), Local/Remote, Count
        report0 = (fileInfo['inputFiles'].split(';'))[0].split('::')

        self.assertEqual('1', report0[1])
        self.assertEqual('Remote', report0[3])
        self.assertEqual('1', report0[4])

        # And tests on a report of two fallback files

        twoReport = Report("cmsRun1")
        twoReport.parse(self.twoFileFallbackXmlPath)
        step = twoReport.retrieveStep('cmsRun1')
        fileInfo = self.reporter.getInputFilesInformation(step)

        fileReports = fileInfo['inputFiles'].split(';')
        self.assertEqual(2, len(fileReports))

        # Format is LFN, Status, Type (EDM), Local/Remote, Count
        report0 = (fileInfo['inputFiles'].split(';'))[0].split('::')
        report1 = (fileInfo['inputFiles'].split(';'))[1].split('::')

        self.assertEqual('1', report0[1])
        self.assertEqual('1', report1[1])
        self.assertEqual('Remote', report0[3])
        self.assertEqual('Remote', report1[3])
        self.assertEqual('1', report0[4])
        self.assertEqual('2', report1[4])

        pileupReport = Report("cmsRun1")
        pileupReport.parse(self.pileupXmlPath)
        step = pileupReport.retrieveStep('cmsRun1')

        localCount = 0
        remoteCount = 0
        for report in (self.reporter.getInputFilesInformation(step))['inputFiles'].split(';'):
            if (report.split('::'))[3] == 'Remote':
                remoteCount += 1
            elif (report.split('::'))[3] == 'Local':
                localCount += 1

        self.assertEqual(1, remoteCount)
        self.assertEqual(13, localCount)


if __name__ == '__main__':
    unittest.main()

