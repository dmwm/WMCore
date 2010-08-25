#!/usr/bin/env python
"""
_Report_

Job Report object

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: Report.py,v 1.1 2009/11/11 00:35:50 evansde Exp $"
__author__ = "evansde"


from WMCore.Configuration import ConfigSection
from WMCore.FwkJobReport.XMLParser import xmlToJobReport


class Report:

    def __init__(self, reportname):
        self.data = ConfigSection("FrameworkJobReport")
        self.reportname = reportname
        self.data.section_(reportname)
        self.report = getattr(self.data, reportname)
        self.report.id = None
        self.report.task = None
        self.report.workload = None
        self.report.status = 0

        # structure
        self.report.section_("site")
        self.report.section_("output")
        self.report.section_("input")
        self.report.section_("performance")
        self.report.section_("analysis")
        self.report.section_("errors")
        self.report.section_("skipped")
        self.report.section_("parameters")
        self.report.skipped.section_("events")
        self.report.skipped.section_("files")
        self.report.analysis.fileCount = 0


    def parse(self, xmlfile):
        """
        _parse_

        Read in the FrameworkJobReport XML file produced
        by cmsRun and pull the information from it into this object

        """
        xmlToJobReport(self, xmlfile)


    def addOutputModule(self, moduleName):
        """
        _addOutputModule_

        Add an entry for an output module.

        """
        self.report.output.section_(moduleName)

        outMod = getattr(self.report.output, moduleName)
        outMod.section_("files")
        outMod.section_("dataset")
        outMod.files.fileCount = 0

        return outMod

    def addOutputFile(self, outputModule, **attrs):
        """
        _addFile_

        Add an output file to the outputModule provided

        """
        outMod = getattr(self.report.output, outputModule, None)
        if outMod == None:
            outMod = self.addOutputModule(outputModule)
        count = outMod.files.fileCount
        fileSection = "file%s" % count
        outMod.files.section_(fileSection)
        fileRef = getattr(outMod.files, fileSection)
        [ setattr(fileRef, k, v) for k, v in attrs.items()]
        outMod.files.fileCount += 1

        fileRef.section_("inputs")
        fileRef.section_("runs")
        fileRef.section_("branches")
        return fileRef


    def addInputSource(self, sourceName):
        """
        _addInputSource_

        Add an input source

        """
        self.report.input.section_(sourceName)
        srcMod = getattr(self.report.input, sourceName)
        srcMod.section_("files")
        srcMod.section_("dataset")
        srcMod.files.fileCount = 0

        return srcMod

    def addInputFile(self, sourceName, **attrs):
        """
        _addInputFile_

        Add an input file to the source named

        """
        srcMod = getattr(self.report.input, sourceName, None)
        if srcMod == None:
            srcMod = self.addInputSource(sourceName)
        count = srcMod.files.fileCount
        fileSection = "file%s" % count
        srcMod.files.section_(fileSection)
        fileRef = getattr(srcMod.files, fileSection)
        [ setattr(fileRef, k, v) for k, v in attrs.items()]
        srcMod.files.fileCount += 1

        fileRef.section_("runs")
        fileRef.section_("branches")

        return fileRef




    def addAnalysisFile(self, filename, **attrs):
        """
        _addAnalysisFile_

        Add and Analysis File

        """
        analysisFiles = self.report.analysis
        count = self.report.analysis.fileCount
        label = "file%s" % count

        analysisFiles.section_(label)
        newFile = getattr(analysisFiles, label)
        newFile.fileName = filename

        [ setattr(newFile, x, y) for x,y in attrs.items() ]

        self.report.analysis.fileCount += 1
        return



if __name__ == '__main__':

    file1 = "/Users/evansde/Work/devel/logs/FrameworkJobReport-Backup.xml"
    file2 = "/Users/evansde/Work/devel/logs/TestReport2.xml"

    report1 = Report("cmsRun1")
    report2 = Report("cmsRun2")

    report1.parse(file1)
    report2.parse(file2)

    print report1.data
    print report2.data
