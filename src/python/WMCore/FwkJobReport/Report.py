#!/usr/bin/env python
"""
_Report_

Job Report object

"""
__version__ = "$Revision: 1.3 $"
__revision__ = "$Id: Report.py,v 1.3 2009/11/11 20:21:31 evansde Exp $"
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
        self.report.skipped.files.fileCount = 0
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


    def addError(self, exitCode, errorType, errorDetails):
        """
        _addError_

        Add an error report with an exitCode, type/class of error and
        details of the error as a string

        """
        self.report.errors.section_(errorType)
        errSection = getattr(self.report.errors, errorType)
        errorCount = getattr(errSection, "errorCount", 0)

        errEntry = "error%s" % errorCount
        errSection.section_(errEntry)
        errDetails = getattr(errSection, errEntry)
        errDetails.exitStatus = exitCode
        errDetails.description = errorDetails

        self.report.status = int(exitCode)
        setattr(errSection, "errorCount", errorCount +1)
        return


    def addSkippedFile(self, lfn, pfn):
        """
        _addSkippedFile_

        report a skipped input file

        """

        count = self.report.skipped.files.fileCount
        entry = "file%s" % count
        self.report.skipped.files.section_(entry)
        skipSect = getattr(self.report.skipped.files, entry)
        skipSect.PhysicalFileName = pfn
        skipSect.LogicalFileName = lfn
        self.report.skipped.files.fileCount += 1
        return



    def addSkippedEvent(self, run, event):
        """
        _addSkippedEvent_


        """
        self.report.skipped.events.section_(str(run))
        runsect = getattr(self.report.skipped.events, str(run))
        if not hasattr(runsect, "eventList"):
            runsect.eventList = []
        runsect.eventList.append(event)
        return




