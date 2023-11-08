#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
_XMLParser_t_

Unit tests for XMLParser module
"""

# system modules
import os
import unittest

# WMCore modules
from WMCore.FwkJobReport.XMLParser import perfSummaryHandler, perfRepHandler, \
        reportBuilder, reportDispatcher, inputFileHandler, fileHandler, \
        runHandler, branchHandler, inputAssocHandler, \
        perfCPUHandler, perfMemHandler, perfStoreHandler, castMetricValue
from WMCore.FwkJobReport.Report import Report
from WMCore.Algorithms.ParseXMLFile import xmlFileToNode
from WMCore.WMBase import getTestBase


class XMLParserTest(unittest.TestCase):
    """
    _XMLParserTest_

    Unit tests for XMLParser module
    """

    def setUp(self):
        """
        _setUp_

        Figure out the location of the XML report produced by CMSSW.
        """
        testData = os.path.join(getTestBase(), "WMCore_t/FwkJobReport_t")
        self.xmlFile = os.path.join(testData, "CMSSWMergeReport2.xml")
        self.xmlXrd = os.path.join(testData, "CMSSWJobReportXrdSiteStatistics.xml")

    def testPerfSummaryHandler(self):
        """
        testPerfSummaryHandler
        Check performance summary handler function
        """
        # read XML, build node structure
        node = xmlFileToNode(self.xmlFile)

        # Set up coroutine pipeline
        fileDispatchers = {
            "Runs": runHandler(),
            "Branches": branchHandler(),
            "Inputs": inputAssocHandler(),
        }
        perfRepDispatchers = {
            "CPU": perfCPUHandler(),
            "Memory": perfMemHandler(),
            "Storage": perfStoreHandler(),
            "PerformanceSummary": perfSummaryHandler()
        }
        dispatchers = {
            "File": fileHandler(fileDispatchers),
            "InputFile": inputFileHandler(fileDispatchers),
            "PerformanceReport": perfRepHandler(perfRepDispatchers)
        }

        # Feed pipeline with node structure and report result instance
        report = Report('cmsRun1')
        report.parse(self.xmlFile)
        reportBuilder(node, report, reportDispatcher(dispatchers))
        cmsRun = getattr(report.data, 'cmsRun1', {})
        performance = getattr(cmsRun, 'performance', {})
        cmssw = getattr(performance, 'cmssw', {})
        obj = cmssw.dictionary_()
        keys = ['SystemMemory', 'ProcessingSummary', 'StorageStatistics', 'SystemCPU', 'Timing', 'ApplicationMemory']
        for key in obj.keys():
            self.assertTrue(key in keys)

    def testXrdSiteStatistics(self):
        """
        unit test for XrdSiteStatistics, it covers castXrdSiteStatistics function.
        Check XrdSiteStatistics metrics in performance summary part of FJR report
        """
        # Feed pipeline with node structure and report result instance
        report = Report('cmsRun1')
        report.parse(self.xmlXrd)
        cmsRun = getattr(report.data, 'cmsRun1', {})
        performance = getattr(cmsRun, 'performance', {})
        cmssw = getattr(performance, 'cmssw', {})
        xrd = getattr(cmssw, 'XrdSiteStatistics', {})
        rdict = xrd.dictionary_()
        keys = [
                'read-numOperations',
                'read-totalMegabytes',
                'readv-numChunks',
                'readv-numOperations',
                'readv-totalMegabytes',
                'readv-totalMsecs'
                ]
        for key in keys:
            self.assertTrue(key in rdict.keys())
            for idict in rdict.get(key, []):
                self.assertTrue('site' in idict.keys())
                self.assertTrue('value' in idict.keys())

    def testCastMetricValue(self):
        """
        unit test castMetricValue function
        """
        self.assertTrue(1, castMetricValue("1"))
        self.assertTrue(1.1, castMetricValue("1.1"))
        self.assertTrue(True, castMetricValue("true"))
        self.assertTrue("bla", castMetricValue("bla "))



if __name__ == "__main__":
    unittest.main()
