#!/usr/bin/env python
"""
_Report_

Job Report object

"""
__version__ = "$Revision: 1.6 $"
__revision__ = "$Id: Report.py,v 1.6 2010/01/08 15:43:31 evansde Exp $"
__author__ = "evansde"

import pickle
from WMCore.Configuration import ConfigSection
from WMCore.FwkJobReport.XMLParser import xmlToJobReport


def jsonise(cfgSect):
    result = {}
    result['section_name_'] = cfgSect._internal_name
    result['sections_'] = []
    d = cfgSect.dictionary_()
    for key, value in d.items():
        if key in cfgSect._internal_children:
            result[key] = jsonise(value)
            result['sections_'].append(key)
        else:
            result[key] = value
    return result

def dejsonise(jsondict):
    section = ConfigSection(jsondict['section_name_'])
    sectionList = jsondict['sections_']
    for key, value in jsondict.items():
        if key in ("sections_", "section_name_"): continue
        if key in sectionList:
            setattr(section, key, dejsonise(value))
        else:
            setattr(section, key, value)
    return section


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
        self.report.section_("logs")
        self.report.section_("cleanup")
        self.report.cleanup.section_("removed")
        self.report.cleanup.section_("unremoved")
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
        try:
            xmlToJobReport(self, xmlfile)
        except Exception, ex:
            msg = "Error reading XML job report file, possibly corrupt XML File:\n"
            msg += "Details: %s" % str(ex)
            self.addError(50115, "MissingJobReport", msg)

    def json(self):
        """
        _json_

        convert into JSON dictionary object

        """
        return jsonise(self.data)

    def dejson(self, jsondicts):
        """
        _dejson_

        Convert JSON provided into ConfigSection structure

        """
        self.data = dejsonise(self.data)

    def persist(self, filename):
        """
        _persist_


        """
        handle = open(filename, 'w')
        pickle.dump(self.data, handle)
        handle.close()
        return

    def unpersist(self, filename):
        """
        _unpersist_

        load object from file

        """

        handle = open(filename, 'r')
        self.data = pickle.load(handle)
        handle.close()
        return




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
        fileRef.inputs.fileCount = 0
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




