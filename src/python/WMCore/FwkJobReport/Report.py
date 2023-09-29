#!/usr/bin/env python

"""
_Report_

Framework job report object.
"""
from __future__ import print_function
from builtins import str as newstr, bytes, range, object
from future.utils import viewitems, listitems

import logging
import math
import re
import sys
import time
import traceback

from Utils.PythonVersion import PY3
from Utils.Utilities import decodeBytesToUnicode, encodeUnicodeToBytes
from WMCore.Configuration import ConfigSection
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Run import Run
from WMCore.FwkJobReport.FileInfo import FileInfo
from WMCore.WMException import WMException
from WMCore.WMExceptions import WM_JOB_ERROR_CODES

import pickle


class FwkJobReportException(WMException):
    """
    _FwkJobReportException_

    FwkJobReport error class
    """
    pass


def addBranchNamesToFile(fileSection, branchNames):
    """
    _addBranchNamesToFile_

    Given a list of branch names add them to the file section in the FWJR.
    """
    setattr(fileSection, "branches", branchNames)
    return


def addInputToFile(fileSection, inputLFN, inputPFN):
    """
    _addInputToFile_

    Given the LFN of an input file add it to the input section of the output
    file.
    """
    if not hasattr(fileSection, "input"):
        setattr(fileSection, "input", [])
    if not hasattr(fileSection, "inputpfns"):
        setattr(fileSection, "inputpfns", [])

    fileSection.input.append(inputLFN)
    fileSection.inputpfns.append(inputPFN)
    return


def addRunInfoToFile(fileSection, runInfo):
    """
    _addRunInfoToFile_

    Given a ConfigSection object corresponding to an input/output file in the
    FWJR and a WMCore.DataStructs.Run object add the run and lumi information
    to the ConfigSection.  It will have the following format:
      fileSection.runs.RUNNUMBER = [LUMI1, LUMI2, LUMI3...]

    Note that the run number will have to be cast to a string.
    """
    if not isinstance(runInfo, Run):
        for singleRun in runInfo:
            setattr(fileSection.runs, str(singleRun.run), singleRun.eventsPerLumi)
    else:
        setattr(fileSection.runs, str(runInfo.run), runInfo.eventsPerLumi)
    return


def addAttributesToFile(fileSection, **attributes):
    """
    _addAttributesToFiles_

    Add attributes to a file in the FWJR.
    """
    for attName, attValue in viewitems(attributes):
        setattr(fileSection, attName, attValue)
    return


class Report(object):
    """
    The base class for the new jobReport

    """

    def __init__(self, reportname=None):
        self.data = ConfigSection("FrameworkJobReport")
        self.data.steps = []
        self.data.workload = "Unknown"
        self.report = None
        self.reportname = ""

        if reportname:
            self.addStep(reportname=reportname)

        return

    def __str__(self):
        return str(self.data)

    def listSteps(self):
        """
        _listSteps_

        List the names of all the steps in the report.
        """
        return self.data.steps

    def setStepStatus(self, stepName, status):
        """
        _setStepStatus_

        Set the status for a step.
        """
        reportStep = self.retrieveStep(stepName)
        reportStep.status = status
        return

    def parse(self, xmlfile, stepName="cmsRun1"):
        """
        _parse_

        Read in the FrameworkJobReport XML file produced
        by cmsRun and pull the information from it into this object
        """
        from WMCore.FwkJobReport.XMLParser import xmlToJobReport
        try:
            xmlToJobReport(self, xmlfile)
        except Exception as ex:
            msg = "Error reading XML job report file, possibly corrupt XML File:\n"
            msg += "Details: %s" % str(ex)

            crashMessage = "\nStacktrace:\n"

            stackTrace = traceback.format_tb(sys.exc_info()[2], None)
            for stackFrame in stackTrace:
                crashMessage += stackFrame

            logging.debug(crashMessage)
            raise FwkJobReportException(msg)

    @staticmethod
    def jsonizeFiles(reportModule):
        """
        _jsonizeFiles_

        Put individual files in JSON format.
        """
        jsonFiles = []
        files = getattr(reportModule, "files", None)
        if not files:
            return jsonFiles

        fileCount = getattr(reportModule.files, "fileCount", 0)

        for i in range(fileCount):
            reportFile = getattr(reportModule.files, "file%s" % i)
            jsonFile = reportFile.dictionary_()

            if jsonFile.get('runs', None):
                cfgSectionRuns = jsonFile["runs"]
                jsonFile["runs"] = {}
                for runNumber in cfgSectionRuns.listSections_():
                    jsonFile["runs"][str(runNumber)] = getattr(cfgSectionRuns,
                                                               runNumber)
            jsonFiles.append(jsonFile)

        return jsonFiles

    @staticmethod
    def jsonizePerformance(perfSection):
        """
        _jsonizePerformance_

        Convert the performance section of the FWJR into JSON.
        """
        jsonPerformance = {}
        for reportSection in ["storage", "memory", "cpu", "multicore", "cmssw"]:
            jsonPerformance[reportSection] = {}
            if not hasattr(perfSection, reportSection):
                continue

            jsonPerformance[reportSection] = getattr(perfSection, reportSection).dictionary_()
            for key in jsonPerformance[reportSection]:
                val = jsonPerformance[reportSection][key]
                if reportSection == 'cmssw' and isinstance(val, ConfigSection):
                    jsonPerformance[reportSection][key] = val.dictionary_()
                elif isinstance(val, float):
                    if math.isinf(val) or math.isnan(val):
                        jsonPerformance[reportSection][key] = None

        return jsonPerformance

    def __to_json__(self, thunker):
        """
        __to_json__

        Create a JSON version of the Report.
        """
        jsonReport = {}
        jsonReport["WorkerNodeInfo"] = self.getWorkerNodeInfo()
        jsonReport["task"] = self.getTaskName()
        jsonReport["steps"] = {}
        jsonReport["skippedFiles"] = self.getAllSkippedFiles()
        jsonReport["fallbackFiles"] = self.getAllFallbackFiles()
        jsonReport["Campaign"] = self.getCampaign()
        jsonReport["PrepID"] = self.getPrepID()
        jsonReport["EOSLogURL"] = self.getLogURL()
        jsonReport["WMTiming"] = self.getWMTiming()

        for stepName in self.listSteps():
            reportStep = self.retrieveStep(stepName)
            jsonStep = {}
            jsonStep["status"] = reportStep.status

            stepTimes = self.getTimes(stepName)

            if stepTimes["startTime"] is not None:
                stepTimes["startTime"] = int(stepTimes["startTime"])
            if stepTimes["stopTime"] is not None:
                stepTimes["stopTime"] = int(stepTimes["stopTime"])

            jsonStep["start"] = stepTimes["startTime"]
            jsonStep["stop"] = stepTimes["stopTime"]

            jsonStep["performance"] = self.jsonizePerformance(reportStep.performance)

            jsonStep["output"] = {}
            for outputModule in reportStep.outputModules:
                reportOutputModule = getattr(reportStep.output, outputModule)
                jsonStep["output"][outputModule] = self.jsonizeFiles(reportOutputModule)

            analysisSection = getattr(reportStep, 'analysis', None)
            if analysisSection:
                jsonStep["output"]['analysis'] = self.jsonizeFiles(analysisSection)

            jsonStep["input"] = {}
            for inputSource in reportStep.input.listSections_():
                reportInputSource = getattr(reportStep.input, inputSource)
                jsonStep["input"][inputSource] = self.jsonizeFiles(reportInputSource)

            jsonStep["errors"] = []
            errorCount = getattr(reportStep.errors, "errorCount", 0)
            for i in range(errorCount):
                reportError = getattr(reportStep.errors, "error%i" % i)
                jsonStep["errors"].append({"type": reportError.type,
                                           "details": reportError.details,
                                           "exitCode": reportError.exitCode})

            jsonStep["cleanup"] = {}
            jsonStep["parameters"] = {}
            jsonStep["site"] = self.getSiteName()
            jsonStep["analysis"] = {}
            jsonStep["logs"] = {}
            jsonStep["WMCMSSWSubprocess"] = self.getWMCMSSWSubprocess(stepName)
            jsonReport["steps"][stepName] = jsonStep

        return jsonReport

    def getSiteName(self):
        """
        _getSiteName_

        Returns the site name attribute (no step specific)
        """
        return getattr(self.data, 'siteName', {})


    def _setSiteName(self, site):
        """
        _setSiteName_

        Set the site name attribute (no step specific)
        """
        setattr(self.data, 'siteName', site)

        return


    def getExitCodes(self):
        """
        _getExitCodes_

        Return a list of all non-zero exit codes in the report
        """
        returnCodes = set()
        for stepName in self.listSteps():
            returnCodes.update(self.getStepExitCodes(stepName=stepName))
        return returnCodes

    def getWMCMSSWSubprocess(self, stepName):
        """
        Returns a WMCMSSWSubprocess metrics for given step
        :param stepName: string representing step name, e.g. cmsRun1
        :return: dictionary of WMCMSSWSubprocess metrics
        """
        reportStep = self.retrieveStep(stepName)
        data = getattr(reportStep, 'WMCMSSWSubprocess', {})
        if isinstance(data, dict):
            return data
        return data.dictionary_()

    def getStepExitCodes(self, stepName):
        """
        _getStepExitCodes_

        Returns a list of all non-zero exit codes in the step
        """
        returnCodes = set()
        reportStep = self.retrieveStep(stepName)
        errorCount = getattr(reportStep.errors, "errorCount", 0)
        for i in range(errorCount):
            reportError = getattr(reportStep.errors, "error%i" % i)
            if getattr(reportError, 'exitCode', None):
                returnCodes.add(int(reportError.exitCode))
            else:
                # exitCode is likely set to None(?!?)
                returnCodes.add(99999)

        return returnCodes

    def getExitCode(self):
        """
        _getExitCode_

        Return the first exit code you find.
        """
        returnCode = 0
        for stepName in self.listSteps():
            errorCode = self.getStepExitCode(stepName=stepName)
            if errorCode == 99999:
                # Then we don't know what this error was
                # Mark it for return only if we don't fine an
                # actual error code in the job.
                returnCode = errorCode
            elif errorCode != 0:
                return errorCode

        return returnCode

    def getStepExitCode(self, stepName):
        """
        _getStepExitCode_

        Get the exit code for a particular step
        Return 0 if none
        """
        returnCode, _ = self.getStepExitCodeAndMessage(stepName)
        return returnCode

    def getStepExitCodeAndMessage(self, stepName):
        """
        _getStepExitCodeAndMessage_

        Get the exit code and message for a particular step
        Return (0, None)  if there were no errors.
        """
        returnCode = 0
        returnMessage = None
        reportStep = self.retrieveStep(stepName)
        errorCount = getattr(reportStep.errors, "errorCount", 0)
        for i in range(errorCount):
            reportError = getattr(reportStep.errors, "error%i" % i)
            if not getattr(reportError, 'exitCode', None):
                returnCode = 99999
                returnMessage = 'Unknown'
            else:
                return int(reportError.exitCode), reportError.details

        return returnCode, returnMessage

    def persist(self, filename):
        """
        _persist_

        Pickle this object and save it to disk.
        """
        if PY3:
            with open(filename, 'wb') as handle:
                pickle.dump(encodeUnicodeToBytes(self.data), handle)
        else:
            with open(filename, 'w') as handle:
                pickle.dump(self.data, handle)
        return

    def unpersist(self, filename, reportname=None):
        """
        _unpersist_

        Load a pickled FWJR from disk.
        """
        if PY3:
            with open(filename, 'rb') as handle:
                self.data = decodeBytesToUnicode(pickle.load(handle))
        else:
            with open(filename, 'r') as handle:
                self.data = pickle.load(handle)

        # old self.report (if it existed) became unattached
        if reportname:
            self.report = getattr(self.data, reportname)

        return

    def addOutputModule(self, moduleName):
        """
        _addOutputModule_

        Add an entry for an output module.
        """
        self.report.outputModules.append(moduleName)
        self.report.output.section_(moduleName)

        outMod = getattr(self.report.output, moduleName)
        outMod.section_("files")
        outMod.section_("dataset")
        outMod.files.fileCount = 0

        return outMod

    def killOutput(self):
        """
        _killOutput_

        Remove all the output from the report.  This is useful for chained
        processing where we don't want to keep the output from a particular
        step in a job.
        """
        for outputModuleName in self.report.outputModules:
            delattr(self.report.output, outputModuleName)

        self.report.outputModules = []
        return

    def addOutputFile(self, outputModule, aFile=None):
        """
        _addFile_

        Add an output file to the outputModule provided.
        """
        logging.info("addOutputFile method called with outputModule: %s, aFile: %s", outputModule, aFile)
        aFile = aFile or {}

        # Now load the output module and create the file object
        outMod = getattr(self.report.output, outputModule, None)
        if outMod is None:
            outMod = self.addOutputModule(outputModule)
        count = outMod.files.fileCount
        fileSection = "file%s" % count
        outMod.files.section_(fileSection)
        fileRef = getattr(outMod.files, fileSection)
        logging.info("addOutputFile method fileRef: %s, whole tree: %s", fileRef, fileRef.dictionary_whole_tree_())
        outMod.files.fileCount += 1

        # Now we need to eliminate the optional and non-primitives:
        # runs, parents, branches, locations and datasets
        keyList = list(aFile)

        fileRef.section_("runs")
        if "runs" in aFile:
            for run in aFile["runs"]:
                addRunInfoToFile(fileRef, run)
            keyList.remove('runs')

        if "parents" in aFile:
            setattr(fileRef, 'parents', list(aFile['parents']))
            keyList.remove('parents')

        if "locations" in aFile:
            fileRef.location = list(aFile["locations"])
            keyList.remove('locations')
        elif "PNN" in aFile:
            fileRef.location = [aFile["PNN"]]

        if "LFN" in aFile:
            fileRef.lfn = aFile["LFN"]
            keyList.remove("LFN")
        if "PFN" in aFile:
            fileRef.lfn = aFile["PFN"]
            keyList.remove("PFN")

        # All right, the rest should be JSONalizable python primitives
        for entry in keyList:
            setattr(fileRef, entry, aFile[entry])

        # And we're done
        return fileRef

    def addInputSource(self, sourceName):
        """
        _addInputSource_

        Add an input source to the report doing nothing if the input source
        already exists.
        """
        if hasattr(self.report.input, sourceName):
            return getattr(self.report.input, sourceName)

        self.report.input.section_(sourceName)
        srcMod = getattr(self.report.input, sourceName)
        srcMod.section_("files")
        srcMod.files.fileCount = 0

        return srcMod

    def addInputFile(self, sourceName, **attrs):
        """
        _addInputFile_

        Add an input file to the given source.
        """
        srcMod = getattr(self.report.input, sourceName, None)
        if srcMod is None:
            srcMod = self.addInputSource(sourceName)
        count = srcMod.files.fileCount
        fileSection = "file%s" % count
        srcMod.files.section_(fileSection)
        fileRef = getattr(srcMod.files, fileSection)
        srcMod.files.fileCount += 1

        keyList = list(attrs)

        fileRef.section_("runs")
        if "runs" in attrs:
            for run in attrs["runs"]:
                addRunInfoToFile(fileRef, run)
            keyList.remove('runs')

        if "parents" in attrs:
            keyList.remove('parents')
        if "locations" in attrs:
            keyList.remove('locations')

        # All right, the rest should be JSONalizable python primitives
        for entry in keyList:
            setattr(fileRef, entry, attrs[entry])

        return fileRef

    def addAnalysisFile(self, filename, **attrs):
        """
        _addAnalysisFile_

        Add an Analysis File.
        """
        analysisFiles = self.report.analysis.files
        count = analysisFiles.fileCount
        label = "file%s" % count

        analysisFiles.section_(label)
        newFile = getattr(analysisFiles, label)
        newFile.fileName = filename

        for x, y in viewitems(attrs):
            setattr(newFile, x, y)

        analysisFiles.fileCount += 1
        return

    def addRemovedCleanupFile(self, **attrs):
        """
        _addRemovedCleanupFile_

        Add a file to the cleanup.removed file
        """
        removedFiles = self.report.cleanup.removed
        count = self.report.cleanup.removed.fileCount
        label = 'file%s' % count

        removedFiles.section_(label)
        newFile = getattr(removedFiles, label)

        for x, y in viewitems(attrs):
            setattr(newFile, x, y)

        self.report.cleanup.removed.fileCount += 1
        return

    def addError(self, stepName, exitCode, errorType, errorDetails, siteName=None):
        """
        _addError_

        Add an error report with an exitCode, type/class of error and
        details of the error as a string.
        Also, report attempted site if error happened before landing on it.
        """
        if self.retrieveStep(stepName) is None:
            # Create a step and set it to failed
            # Assumption: Adding an error fails a step
            self.addStep(stepName, status=1)

        if exitCode is not None:
            exitCode = int(exitCode)

        setExitCodes = self.getStepExitCodes(stepName)
        if exitCode in setExitCodes:
            logging.warning("Exit code: %s has been already added to the job report", exitCode)
            return

        stepSection = self.retrieveStep(stepName)
        errorCount = getattr(stepSection.errors, "errorCount", 0)
        errEntry = "error%s" % errorCount
        stepSection.errors.section_(errEntry)
        errDetails = getattr(stepSection.errors, errEntry)
        errDetails.exitCode = exitCode
        errDetails.type = str(errorType)

        try:
            if isinstance(errorDetails, newstr):
                errDetails.details = errorDetails
            elif isinstance(errorDetails, bytes):
                errDetails.details = decodeBytesToUnicode(errorDetails, 'ignore')
            else:
                errDetails.details = newstr(errorDetails)
        except UnicodeEncodeError as ex:
            msg = "Failed to encode the job error details for job ID: %s." % self.getJobID()
            msg += "\nException message: %s\nOriginal error details: %s" % (str(ex), errorDetails)
            logging.error(msg)
            msg = "DEFAULT ERROR MESSAGE, because it failed to UTF-8 encode the original message."
            errDetails.details = msg
        except UnicodeDecodeError as ex:
            msg = "Failed to decode the job error details for job ID: %s." % self.getJobID()
            msg += "\nException message: %s\nOriginal error details: %s" % (str(ex), errorDetails)
            logging.error(msg)
            msg = "DEFAULT ERROR MESSAGE, because it failed to UTF-8 decode the original message."
            errDetails.details = msg

        setattr(stepSection.errors, "errorCount", errorCount + 1)
        self.setStepStatus(stepName=stepName, status=exitCode)

        if siteName:
            self._setSiteName(site=siteName)

        return

    def addSkippedFile(self, lfn, pfn):
        """
        _addSkippedFile_

        Report a skipped input file
        """
        count = self.report.skipped.files.fileCount
        entry = "file%s" % count
        self.report.skipped.files.section_(entry)
        skipSect = getattr(self.report.skipped.files, entry)
        skipSect.PhysicalFileName = pfn
        skipSect.LogicalFileName = lfn
        self.report.skipped.files.fileCount += 1
        return

    def addFallbackFile(self, lfn, pfn):
        """
        _addFallbackFile_

        Report a fallback attempt for input file
        """
        count = self.report.fallback.files.fileCount
        entry = "file%s" % count
        self.report.fallback.files.section_(entry)
        fallbackSect = getattr(self.report.fallback.files, entry)
        fallbackSect.PhysicalFileName = pfn
        fallbackSect.LogicalFileName = lfn
        self.report.fallback.files.fileCount += 1
        return

    def addSkippedEvent(self, run, event):
        """
        _addSkippedEvent_

        Add a skipped event.
        """
        self.report.skipped.events.section_(str(run))
        runsect = getattr(self.report.skipped.events, str(run))
        if not hasattr(runsect, "eventList"):
            runsect.eventList = []
        runsect.eventList.append(event)
        return

    def addStep(self, reportname, status=1):
        """
        _addStep_

        This creates a report section into self.report
        """
        if hasattr(self.data, reportname):
            msg = "Attempted to create pre-existing report section %s" % reportname
            logging.error(msg)
            return

        self.data.steps.append(reportname)

        self.reportname = reportname
        self.data.section_(reportname)
        self.report = getattr(self.data, reportname)
        self.report.id = None
        self.report.status = status
        self.report.outputModules = []

        # structure
        self.report.section_("site")
        self.report.section_("output")
        self.report.section_("input")
        self.report.section_("performance")
        self.report.section_("analysis")
        self.report.section_("errors")
        self.report.section_("skipped")
        self.report.section_("fallback")
        self.report.section_("parameters")
        self.report.section_("logs")
        self.report.section_("cleanup")
        self.report.analysis.section_("files")
        self.report.cleanup.section_("removed")
        self.report.cleanup.section_("unremoved")
        self.report.skipped.section_("events")
        self.report.skipped.section_("files")
        self.report.fallback.section_("files")
        self.report.skipped.files.fileCount = 0
        self.report.fallback.files.fileCount = 0
        self.report.analysis.files.fileCount = 0
        self.report.cleanup.removed.fileCount = 0

        return

    def setStep(self, stepName, stepSection):
        """
        _setStep_

        """
        if stepName not in self.listSteps():
            self.data.steps.append(stepName)
        else:
            logging.info("Step %s is now being overridden by a new step report", stepName)
        self.data.section_(stepName)
        setattr(self.data, stepName, stepSection)
        return

    def retrieveStep(self, step):
        """
        _retrieveStep_

        Grabs a report in the raw and returns it.
        """
        reportSection = getattr(self.data, step, None)
        return reportSection

    def load(self, filename):
        """
        _load_

        This just maps to unpersist
        """
        self.unpersist(filename)
        return

    def save(self, filename):
        """
        _save_

        This just maps to persist
        """
        self.persist(filename)
        return

    def getOutputModule(self, step, outputModule):
        """
        _getOutputModule_

        Get the output module from a particular step
        """
        stepReport = self.retrieveStep(step=step)

        if not stepReport:
            return None

        return getattr(stepReport.output, outputModule, None)

    def getOutputFile(self, fileName, outputModule, step):
        """
        _getOutputFile_

        Takes a fileRef object and returns a DataStructs/File object as output
        """

        outputMod = self.getOutputModule(step=step, outputModule=outputModule)

        if not outputMod:
            return None

        fileRef = getattr(outputMod.files, fileName, None)
        newFile = File(locations=set())

        # Locations
        newFile.setLocation(getattr(fileRef, "location", None))

        # Runs
        runList = fileRef.runs.listSections_()
        for run in runList:
            lumis = getattr(fileRef.runs, run)
            if isinstance(lumis, dict):
                newRun = Run(int(run), *listitems(lumis))
            else:
                newRun = Run(int(run), *lumis)
            newFile.addRun(newRun)

        newFile["lfn"] = getattr(fileRef, "lfn", None)
        newFile["pfn"] = getattr(fileRef, "pfn", None)
        newFile["events"] = int(getattr(fileRef, "events", 0))
        newFile["size"] = int(getattr(fileRef, "size", 0))
        newFile["branches"] = getattr(fileRef, "branches", [])
        newFile["input"] = getattr(fileRef, "input", [])
        newFile["inputpfns"] = getattr(fileRef, "inputpfns", [])
        newFile["branch_hash"] = getattr(fileRef, "branch_hash", None)
        newFile["catalog"] = getattr(fileRef, "catalog", "")
        newFile["guid"] = getattr(fileRef, "guid", "")
        newFile["module_label"] = getattr(fileRef, "module_label", "")
        newFile["checksums"] = getattr(fileRef, "checksums", {})
        newFile["merged"] = bool(getattr(fileRef, "merged", False))
        newFile["dataset"] = getattr(fileRef, "dataset", {})
        newFile["acquisitionEra"] = getattr(fileRef, 'acquisitionEra', None)
        newFile["processingVer"] = getattr(fileRef, 'processingVer', 0)
        newFile["validStatus"] = getattr(fileRef, 'validStatus', None)
        newFile["globalTag"] = getattr(fileRef, 'globalTag', None)
        newFile["prep_id"] = getattr(fileRef, 'prep_id', None)
        newFile['configURL'] = getattr(fileRef, 'configURL', None)
        newFile['inputPath'] = getattr(fileRef, 'inputPath', None)
        newFile["outputModule"] = outputModule
        newFile["fileRef"] = fileRef

        return newFile

    def getAllFilesFromStep(self, step):
        """
        _getAllFilesFromStep_

        For a given step, retrieve all the associated files
        """

        stepReport = self.retrieveStep(step=step)
        if not stepReport:
            logging.debug("Asked to retrieve files from non-existant step %s", step)
            return []

        # steps with no outputModules can be ok (even for CMSSW steps)
        listOfModules = getattr(stepReport, 'outputModules', None)
        if not listOfModules:
            return []

        listOfFiles = []
        for module in listOfModules:
            listOfFiles.extend(self.getFilesFromOutputModule(step=step, outputModule=module))

        return listOfFiles

    def getAllFiles(self):
        """
        _getAllFiles_

        Grabs all files in all output modules in all steps
        """
        listOfFiles = []

        for step in self.listSteps():
            listOfFiles.extend(self.getAllFilesFromStep(step=step))

        return listOfFiles

    def getAllInputFiles(self):
        """
        _getAllInputFiles_

        Gets all the input files
        """

        listOfFiles = []
        for step in self.listSteps():
            tmp = self.getInputFilesFromStep(stepName=step)
            if tmp:
                listOfFiles.extend(tmp)

        return listOfFiles

    def getInputFilesFromStep(self, stepName, inputSource=None):
        """
        _getInputFilesFromStep_

        Retrieve a list of input files from the given step.
        """
        step = self.retrieveStep(stepName)

        if inputSource is None:
            inputSources = step.input.listSections_()
        else:
            inputSources = [inputSource]

        inputFiles = []
        for inputSource in inputSources:
            source = getattr(step.input, inputSource)
            for fileNum in range(source.files.fileCount):
                fwjrFile = getattr(source.files, "file%d" % fileNum)

                lfn = getattr(fwjrFile, "lfn", None)
                pfn = getattr(fwjrFile, "pfn", None)
                size = getattr(fwjrFile, "size", 0)
                events = getattr(fwjrFile, "events", 0)
                branches = getattr(fwjrFile, "branches", [])
                catalog = getattr(fwjrFile, "catalog", None)
                guid = getattr(fwjrFile, "guid", None)
                inputSourceClass = getattr(fwjrFile, "input_source_class", None)
                moduleLabel = getattr(fwjrFile, "module_label", None)
                inputType = getattr(fwjrFile, "input_type", None)

                inputFile = File(lfn=lfn, size=size, events=events)
                inputFile["pfn"] = pfn
                inputFile["branches"] = branches
                inputFile["catalog"] = catalog
                inputFile["guid"] = guid
                inputFile["input_source_class"] = inputSourceClass
                inputFile["module_label"] = moduleLabel
                inputFile["input_type"] = inputType

                runSection = getattr(fwjrFile, "runs")
                runNumbers = runSection.listSections_()

                for runNumber in runNumbers:
                    lumiTuple = getattr(runSection, str(runNumber))
                    inputFile.addRun(Run(int(runNumber), *lumiTuple))

                inputFiles.append(inputFile)

        return inputFiles

    def getFilesFromOutputModule(self, step, outputModule):
        """
        _getFilesFromOutputModule_

        Grab all the files in a particular output module
        """

        outputMod = self.getOutputModule(step=step, outputModule=outputModule)

        if not outputMod:
            return []

        listOfFiles = []
        for n in range(outputMod.files.fileCount):
            aFile = self.getOutputFile(fileName='file%i' % (n), outputModule=outputModule, step=step)
            if aFile:
                listOfFiles.append(aFile)
            else:
                msg = "Could not find file%i in module" % (n)
                logging.error(msg)
                return []

        return listOfFiles

    def getAllSkippedFiles(self):
        """
        _getAllSkippedFiles_

        Get a list of LFNs for all the input files
        listed as skipped on the report.
        """
        listOfFiles = []
        for step in self.listSteps():
            tmp = self.getSkippedFilesFromStep(stepName=step)
            if tmp:
                listOfFiles.extend(tmp)

        return listOfFiles

    def getAllFallbackFiles(self):
        """
        _getAllFallbackFiles_

        Get a list of LFNs for all the input files
        listed as fallback attempt on the report
        """
        listOfFiles = []
        for step in self.listSteps():
            tmp = self.getFallbackFilesFromStep(stepName=step)
            if tmp:
                listOfFiles.extend(tmp)

        return listOfFiles

    def getSkippedFilesFromStep(self, stepName):
        """
        _getSkippedFilesFromStep_

        Get a list of LFNs skipped in the given step
        """
        skippedFiles = []

        step = self.retrieveStep(stepName)

        filesSection = step.skipped.files
        fileCount = getattr(filesSection, "fileCount", 0)

        for fileNum in range(fileCount):
            fileSection = getattr(filesSection, "file%d" % fileNum)
            lfn = getattr(fileSection, "LogicalFileName", None)
            if lfn is not None:
                skippedFiles.append(lfn)
            else:
                logging.error("Found no LFN in file %s", str(fileSection))

        return skippedFiles

    def getFallbackFilesFromStep(self, stepName):
        """
        _getFallbackFilesFromStep_

        Get a list of LFNs which triggered a fallback in the given step
        """
        fallbackFiles = []

        step = self.retrieveStep(stepName)
        try:
            filesSection = step.fallback.files
        except AttributeError:
            return fallbackFiles
        fileCount = getattr(filesSection, "fileCount", 0)

        for fileNum in range(fileCount):
            fileSection = getattr(filesSection, "file%d" % fileNum)
            lfn = getattr(fileSection, "LogicalFileName", None)
            if lfn is not None:
                fallbackFiles.append(lfn)
            else:
                logging.error("Found no LFN in file %s", str(fileSection))

        return fallbackFiles

    def getStepErrors(self, stepName):
        """
        _getStepErrors_

        Get all errors for a given step
        """
        if self.retrieveStep(stepName) is None:
            # Create a step and set it to failed
            # Assumption: Adding an error fails a step
            self.addStep(stepName, status=1)

        stepSection = self.retrieveStep(stepName)

        errorCount = getattr(stepSection.errors, "errorCount", 0)
        if errorCount == 0:
            return {}
        else:
            return stepSection.errors.dictionary_()

    def stepSuccessful(self, stepName):
        """
        _stepSuccessful_

        Determine wether or not a step was successful.
        """
        stepReport = self.retrieveStep(step=stepName)
        status = getattr(stepReport, 'status', 1)
        # We have too many possibilities
        if status not in [0, '0', 'success', 'Success']:
            return False

        return True

    def taskSuccessful(self, ignoreString='logArch'):
        """
        _taskSuccessful_

        Return True if all steps successful, False otherwise
        """
        value = True

        if len(self.listSteps()) == 0:
            # Mark jobs as failed if they have no steps
            msg = "Could not find any steps"
            logging.error(msg)
            return False

        for stepName in self.listSteps():
            # Ignore specified steps
            # i.e., logArch steps can fail without causing
            # the task to fail
            if ignoreString and re.search(ignoreString, stepName):
                continue
            if not self.stepSuccessful(stepName=stepName):
                value = False

        return value

    def getAnalysisFilesFromStep(self, step):
        """
        _getAnalysisFilesFromStep_

        Retrieve a list of all the analysis files produced in a step.
        """
        stepReport = self.retrieveStep(step=step)

        if not stepReport or not hasattr(stepReport.analysis, 'files'):
            return []

        analysisFiles = stepReport.analysis.files

        results = []
        for fileNum in range(analysisFiles.fileCount):
            results.append(getattr(analysisFiles, "file%s" % fileNum))

        # filter out duplicates
        duplicateCheck = []
        filteredResults = []
        for result in results:
            inputtag = getattr(result, 'inputtag', None)
            if (result.fileName, inputtag) not in duplicateCheck:
                duplicateCheck.append((result.fileName, inputtag))
                filteredResults.append(result)

        return filteredResults

    def getAllFileRefsFromStep(self, step):
        """
        _getAllFileRefsFromStep_

        Retrieve a list of all files produced in a step.  The files will be in
        the form of references to the ConfigSection objects in the acutal
        report.
        """
        stepReport = self.retrieveStep(step=step)
        if not stepReport:
            return []

        outputModules = getattr(stepReport, "outputModules", [])
        fileRefs = []
        for outputModule in outputModules:
            outputModuleRef = self.getOutputModule(step=step, outputModule=outputModule)

            for i in range(outputModuleRef.files.fileCount):
                fileRefs.append(getattr(outputModuleRef.files, "file%i" % i))

        analysisFiles = self.getAnalysisFilesFromStep(step)
        fileRefs.extend(analysisFiles)

        return fileRefs

    def addInfoToOutputFilesForStep(self, stepName, step):
        """
        _addInfoToOutputFilesForStep_

        Add the information missing from output files to the files
        This requires the WMStep to be passed in
        """

        stepReport = self.retrieveStep(step=stepName)
        fileInfo = FileInfo()

        if not stepReport:
            return

        listOfModules = getattr(stepReport, 'outputModules', None)

        for module in listOfModules:
            outputMod = getattr(stepReport.output, module, None)
            for n in range(outputMod.files.fileCount):
                aFile = getattr(outputMod.files, 'file%i' % n, None)
                if not aFile:
                    msg = "Could not find file%i in module" % n
                    logging.error(msg)
                    return
                fileInfo(fileReport=aFile, step=step, outputModule=module)

        return

    def deleteOutputModuleForStep(self, stepName, moduleName):
        """
        _deleteOutputModuleForStep_

        Delete any reference to the given output module in the step report
        that includes deleting any output file it produced
        """
        stepReport = self.retrieveStep(step=stepName)

        if not stepReport:
            return

        listOfModules = getattr(stepReport, 'outputModules', [])

        if moduleName not in listOfModules:
            return

        delattr(stepReport.output, moduleName)
        listOfModules.remove(moduleName)

        return

    def setStepStartTime(self, stepName):
        """
        _setStepStatus_

        Set the startTime for a step.
        """
        reportStep = self.retrieveStep(stepName)
        reportStep.startTime = time.time()
        return

    def setStepStopTime(self, stepName):
        """
        _setStepStatus_

        Set the stopTime for a step.
        """
        reportStep = self.retrieveStep(stepName)
        reportStep.stopTime = time.time()
        return

    def getTimes(self, stepName):
        """
        _getTimes_

        Return a dictionary with the start and stop times
        """
        reportStep = self.retrieveStep(stepName)

        startTime = getattr(reportStep, 'startTime', None)
        stopTime = getattr(reportStep, 'stopTime', None)

        return {'startTime': startTime, 'stopTime': stopTime}

    def getFirstStartLastStop(self):
        """
        _getFirstStartLastStop_

        Get the first startTime, last stopTime
        """

        steps = self.listSteps()

        if len(steps) < 1:
            return None

        firstStep = self.getTimes(stepName=steps[0])
        startTime = firstStep['startTime']
        stopTime = firstStep['stopTime']

        for stepName in steps:
            timeStamps = self.getTimes(stepName=stepName)
            if timeStamps['startTime'] is None or timeStamps['stopTime'] is None:
                # Unusable times
                continue
            if startTime is None or startTime > timeStamps['startTime']:
                startTime = timeStamps['startTime']
            if stopTime is None or stopTime < timeStamps['stopTime']:
                stopTime = timeStamps['stopTime']

        return {'startTime': startTime, 'stopTime': stopTime}

    def setTaskName(self, taskName):
        """
        _setTaskName_

        Set the task name for the report
        """
        self.data.task = taskName
        return

    def getTaskName(self):
        """
        _getTaskName_

        Return the task name
        """
        return getattr(self.data, 'task', None)

    def setJobID(self, jobID):
        """
        _setJobID_

        Set the WMBS jobID
        """

        self.data.jobID = jobID
        return

    def getJobID(self):
        """
        _getJobID_

        Get the WMBS job ID if attached
        """

        return getattr(self.data, 'jobID', None)

    def getAllFileRefs(self):
        """
        _getAllFileRefs_

        Get references for all file in the step
        """

        fileRefs = []
        for step in self.listSteps():
            tmpRefs = self.getAllFileRefsFromStep(step=step)
            if len(tmpRefs) > 0:
                fileRefs.extend(tmpRefs)

        return fileRefs

    def setAcquisitionProcessing(self, acquisitionEra, processingVer, processingStr=None):
        """
        _setAcquisitionProcessing_

        Set the acquisition and processing era for every output file
        ONLY run this after all files have been accumulated; it doesn't
        set things for future files.
        """
        fileRefs = self.getAllFileRefs()

        # Should now have all the fileRefs
        for f in fileRefs:
            f.acquisitionEra = acquisitionEra
            f.processingVer = processingVer
            f.processingStr = processingStr

        return

    def updateSubprocessInfo(self, sysTime, userTime, startTime, endTime):
        """
        Add process information to WMCore FJR
        :param sysTime: systtem time reported by subprocess job
        :param userTime: user time reported by subprocess job
        :param startTime: start time of subprocess (in seconds)
        :param endTime: endtime time of subprocess (in seconds)
        """
        self.report.section_('WMCMSSWSubprocess')
        self.report.WMCMSSWSubprocess.startTime = startTime
        self.report.WMCMSSWSubprocess.endTime = endTime
        self.report.WMCMSSWSubprocess.wallClockTime = endTime - startTime
        self.report.WMCMSSWSubprocess.userTime = userTime
        self.report.WMCMSSWSubprocess.sysTime = sysTime

    def setValidStatus(self, validStatus):
        """
        _setValidStatus_

        Set the validStatus for all steps and all files.
        ONLY run this after all files have been attached.
        """

        fileRefs = self.getAllFileRefs()

        # Should now have all the fileRefs
        for f in fileRefs:
            f.validStatus = validStatus

        return

    def setGlobalTag(self, globalTag):
        """
        _setGlobalTag_

        Set the global Tag from the spec on the WN
        ONLY run this after all the files have been attached
        """

        fileRefs = self.getAllFileRefs()

        # Should now have all the fileRefs
        for f in fileRefs:
            f.globalTag = globalTag

        return

    def setCampaign(self, campaign):
        """
        _setCampaign_
        Set the campaign for the report
        """
        self.data.campaign = campaign
        return

    def getCampaign(self):
        """
        _getCampaign_
        Return the campaign
        """
        return getattr(self.data, 'campaign', "")

    def setPrepID(self, prep_id):
        """
        _setGlobalTag_

        Set the global Tag from the spec on the WN
        ONLY run this after all the files have been attached
        """

        fileRefs = self.getAllFileRefs()

        # Should now have all the fileRefs
        for f in fileRefs:
            f.prep_id = prep_id

        self.data.prep_id = prep_id
        return

    def getPrepID(self):
        """
         _getPrepID_

         Return the PrepID
        """
        return getattr(self.data, 'prep_id', "")

    def getWMTiming(self):
        """
        Returns a WMTiming metrics
        :return: dictionary of WMTiming metrics
        """
        data = getattr(self.data, 'WMTiming', {})
        if isinstance(data, dict):
            return data
        return data.dictionary_()

    def setConfigURL(self, configURL):
        """
        _setConfigURL_

        Set the config URL in a portable storage form
        """

        fileRefs = self.getAllFileRefs()

        # Should now have all the fileRefs
        for f in fileRefs:
            f.configURL = configURL

        return

    def setInputDataset(self, inputPath):
        """
        _setInputDataset_

        Set the input dataset path for the task in each file
        """

        fileRefs = self.getAllFileRefs()

        # Should now have all the fileRefs
        for f in fileRefs:
            f.inputPath = inputPath
        return

    def setStepRSS(self, stepName, minimum, maximum, average):
        """
        _setStepRSS_

        Set the Performance RSS information
        """

        reportStep = self.retrieveStep(stepName)
        reportStep.performance.section_('RSSMemory')
        reportStep.performance.RSSMemory.min = minimum
        reportStep.performance.RSSMemory.max = maximum
        reportStep.performance.RSSMemory.average = average

        return

    def setStepPMEM(self, stepName, minimum, maximum, average):
        """
        _setStepPMEM_

        Set the Performance PMEM information
        """

        reportStep = self.retrieveStep(stepName)
        reportStep.performance.section_('PhysicalMemory')
        reportStep.performance.PhysicalMemory.min = minimum
        reportStep.performance.PhysicalMemory.max = maximum
        reportStep.performance.PhysicalMemory.average = average

        return

    def setStepPCPU(self, stepName, minimum, maximum, average):
        """
        _setStepPCPU_

        Set the Performance PCPU information
        """

        reportStep = self.retrieveStep(stepName)
        reportStep.performance.section_('PercentCPU')
        reportStep.performance.PercentCPU.min = minimum
        reportStep.performance.PercentCPU.max = maximum
        reportStep.performance.PercentCPU.average = average

        return

    def setStepVSize(self, stepName, minimum, maximum, average):
        """
        _setStepVSize_

        Set the Performance PCPU information
        """

        reportStep = self.retrieveStep(stepName)
        reportStep.performance.section_('VSizeMemory')
        reportStep.performance.VSizeMemory.min = minimum
        reportStep.performance.VSizeMemory.max = maximum
        reportStep.performance.VSizeMemory.average = average

        return

    def setStepCounter(self, stepName, counter):
        """
        _setStepCounter_

        Assign a number to the step
        """

        reportStep = self.retrieveStep(stepName)
        reportStep.counter = counter

        return

    def checkForAdlerChecksum(self, stepName):
        """
        _checkForAdlerChecksum_

        Some steps require that all output files have adler checksums
        This will go through all output files in a step and make sure they
          have an adler32 checksum.  If they don't it creates an error with
          code 60451 for the step, failing it.
        """
        error = None
        files = self.getAllFilesFromStep(step=stepName)
        for f in files:
            if 'adler32' not in f.get('checksums', {}):
                error = f.get('lfn', None)
            elif f['checksums']['adler32'] is None:
                error = f.get('lfn', None)

        if error:
            msg = '%s, file was %s' % (WM_JOB_ERROR_CODES[60451], error)
            self.addError(stepName, 60451, "NoAdler32Checksum", msg)
            self.setStepStatus(stepName=stepName, status=60451)

        return

    def checkForRunLumiInformation(self, stepName):
        """
        _checkForRunLumiInformation_

        Some steps require that all output files have run lumi information.
        This will go through all output files in a step and make sure
        they have run/lumi informaiton. If they don't it creates an error
        with code 70452 for the step, failing it.

        """
        error = None
        files = self.getAllFilesFromStep(step=stepName)
        for f in files:
            if not f.get('runs', None):
                error = f.get('lfn', None)
            else:
                for run in f['runs']:
                    lumis = run.lumis
                    if not lumis:
                        error = f.get('lfn', None)
                        break
        if error:
            msg = '%s, file was %s' % (WM_JOB_ERROR_CODES[70452], error)
            self.addError(stepName, 70452, "NoRunLumiInformation", msg)
            self.setStepStatus(stepName=stepName, status=70452)
        return

    def checkForOutputFiles(self, stepName):
        """
        _checkForOutputFiles_

        Verify that there is at least an output file, either from
        analysis or from an output module.
        """
        files = self.getAllFilesFromStep(step=stepName)
        analysisFiles = self.getAnalysisFilesFromStep(step=stepName)
        if len(files) == 0 and len(analysisFiles) == 0:
            msg = WM_JOB_ERROR_CODES[60450]
            msg += "\nList of skipped files is:\n"
            for skipF in self.getSkippedFilesFromStep(stepName=stepName):
                msg += "  %s\n" % skipF
            self.addError(stepName, 60450, "NoOutput", msg)
            self.setStepStatus(stepName=stepName, status=60450)
        return

    def stripInputFiles(self):
        """
        _stripInputFiles_

        If we need to compact the FWJR the easiest way is just to
        trim the number of input files.
        """

        for stepName in self.listSteps():
            step = self.retrieveStep(stepName)
            inputSources = step.input.listSections_()
            for inputSource in inputSources:
                source = getattr(step.input, inputSource)
                for fileNum in range(source.files.fileCount):
                    delattr(source.files, "file%d" % fileNum)
                source.files.fileCount = 0
        return

    def getWorkerNodeInfo(self):
        wnInfo = {"HostName": getattr(self.data, 'hostName', ''),
                  "MachineFeatures": getattr(self.data, 'machineFeatures', {}),
                  "JobFeatures": getattr(self.data, 'jobFeatures', {})}

        return wnInfo

    def setLogURL(self, url):
        """
        Set log url for the this job report.
        https://eoscmsweb.cern.ch/eos/cms/store/logs/prod/recent/
        """
        self.data.logURL = url

    def getLogURL(self):
        """
        _getLogURL_
        Return the log URL
        """
        return getattr(self.data, 'logURL', '')
