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
        self.multiReport = "%s/CMSSWMulticoreTopReport.xml" % self.directory
        self.report = Report("MultiReport_t")
        
        
    def tearDown(self):
        del self.report
        
    def testA(self):
        """
        test reading the multi report file, which should in turn read the processing report
        """
        multiXmlToJobReport(self.report, self.multiReport, directory = self.directory)


        print self.report.data

    
if __name__ == '__main__':
    unittest.main()