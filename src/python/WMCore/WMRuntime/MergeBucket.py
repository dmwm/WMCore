#!/usr/bin/env python
# encoding: utf-8
"""
MergeBucket.py

Created by Dave Evans on 2010-12-09.
Copyright (c) 2010 Fermilab. All rights reserved.

Utility to construct merge jobs for the different output modules produced in a Multicore CMSSW job

"""

import sys
import os
import WMCore.FwkJobReport.XMLParser as ReportReader
from WMCore.FwkJobReport.Report import Report

class MergeBucket(list):
    """
    _MergeBucket_

    Container used to merge multiprocess job output fragments and preserve parentage
    """

    def __init__(self, lfn, moduleName, stepName, workDir):
        list.__init__(self)
        self.lfn = lfn
        self.moduleName = moduleName
        self.stepName = stepName
        self.workingDir = workDir



    merge_inputs = property( lambda x: [ "\"file:%s\"" % j['pfn'] for j in x] )
    merge_pset_file = property( lambda x: "multicore-merge-%s.py" % x.moduleName)
    merge_report_file = property( lambda x: "multicore-merge-%s.xml" % x.moduleName)
    merge_command = property(lambda x: "cmsRun -j %s %s" % (x.merge_report_file, x.merge_pset_file))

    def inputFiles(self):
        """
        reduce the input files LFN list
        """
        inputfiles = set()
        for f in self:
            [ inputfiles.add(x) for x in f['input']]
        return list(inputfiles)

    def mergeConfig(self):
        """
        _mergeConfig_

        Build the config information for the merge of the files in this bucket
        """
        config = \
        "from Configuration.DataProcessing.Merge import mergeProcess\nprocess = mergeProcess(\n    "
        config += ",".join(self.merge_inputs)
        config += ",\n"
        config += "    output_file = \"%s\",\n"   %  os.path.basename(self.lfn)
        config += "    output_lfn = \"%s\"\n) "  % self.lfn
        return config

    def writeConfig(self):
        """
        _writeConfig_

        Write the merge config into the file named by this obejct in the working dir
        """
        targetFile = "%s/%s" % (self.workingDir, self.merge_pset_file)
        handle = open(targetFile, 'w')
        handle.write(self.mergeConfig())
        handle.close()
        return

    def mergeReport(self):
        """
        _mergeReport_

        read the merge report
        """
        reportInstance = Report(self.stepName)
        ReportReader.xmlToJobReport(reportInstance, os.path.join(self.workingDir, self.merge_report_file))
        return reportInstance

    def editReport(self, finalReport):
        """
        _editReport_

        edit job report for all the files in the merge
        set the parents etc and hide the fact that the merge ever happened.

        """
        report = self.mergeReport()
        for f in report.getAllFiles():
            f['outputModule'] = self.moduleName
            f['module_label'] = self.moduleName
            f['inputpfns'] = []
            f['inputs'] = self.inputFiles()
            finalReport.addOutputFile(self.moduleName, f)
