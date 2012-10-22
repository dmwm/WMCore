#!/usr/bin/env python
# encoding: utf-8
"""
MultiReport_t.py

Created by Dave Evans on 2010-06-10.
Copyright (c) 2010 Fermilab. All rights reserved.
"""
import os
import unittest
import inspect

from WMCore.FwkJobReport.XMLParser import multiXmlToJobReport
from WMCore.FwkJobReport.Report import Report

def findThisModule():
    pass

class MultiReport_t(unittest.TestCase):
    def setUp(self):

        self.directory = os.path.dirname(inspect.getsourcefile(findThisModule))
        self.multiReport = os.path.join(self.directory, "CMSSWMulticoreTopReport.xml")
        self.report = Report("MultiReport_t")


    def tearDown(self):
        del self.report

    def testA(self):
        """
        test reading the multi report file, which should in turn read the processing report
        """
        if not os.path.exists(self.multiReport):
            msg = "Multi Report file not found: %s" % self.multiReport
            self.fail(msg)
        try:
            multiXmlToJobReport(self.report, self.multiReport, directory = self.directory)
        except Exception, ex:
            msg = "Error calling multiXmlToJobReport: %s" % str(ex)
            self.fail(msg)

        self.failUnless(hasattr(self.report.report, "input"))
        self.failUnless(hasattr(self.report.report.input, "source"))
        self.failUnless(hasattr(self.report.report.input.source, "files"))
        self.failUnless(hasattr(self.report.report.input.source.files, "fileCount"))
        self.failUnless(hasattr(self.report.report, "output"))

        self.assertEqual(self.report.report.input.source.files.fileCount, 1)
        self.failUnless("outputRECORECO" in self.report.report.output.listSections_())
        self.failUnless("outputALCARECORECO" in self.report.report.output.listSections_())




if __name__ == '__main__':
    unittest.main()
