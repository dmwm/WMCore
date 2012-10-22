#!/usr/bin/env python
# encoding: utf-8
"""
MulticoreReader.py

Created by Dave Evans on 2012-01-05.
Copyright (c) 2012 evansde77. All rights reserved.
"""

import sys
import os
from WMCore.FwkJobReport.Report import Report
import WMCore.FwkJobReport.MulticoreUtils as ReportUtils
import WMCore.FwkJobReport.XMLParser as ReportReader

def readMultiJobReports(multiReportFile, stepName, directory):
    """
    _readMultiJobReports_

    Read a multi report and return a list of report instances indexed by it
    """
    result = []
    jobRepNode = ReportReader.xmlFileToNode(multiReportFile)
    for repNode in ReportReader.childrenMatching(jobRepNode, "FrameworkJobReport"):
        for childProcFiles in ReportReader.childrenMatching(repNode, "ChildProcessFiles"):
            for childRep in ReportReader.childrenMatching(childProcFiles, "ChildProcessFile"):
                fileName =  childRep.text
                if directory != None:
                    fileName = "%s/%s" % (directory, fileName)
                if os.path.exists(fileName):
                    reportInstance = Report(stepName)
                    ReportReader.xmlToJobReport(reportInstance, fileName)
                    result.append(reportInstance)

    return result
