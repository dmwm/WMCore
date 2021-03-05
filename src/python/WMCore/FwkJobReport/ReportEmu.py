#!/usr/bin/env python
"""
_ReportEmu_

Class for creating bogus framework job reports.
"""
from __future__ import division

from builtins import object
import os.path

from WMCore.DataStructs.File import File
from WMCore.FwkJobReport import Report
from WMCore.Services.UUIDLib import makeUUID


class ReportEmu(object):
    """
    _ReportEmu_

    Job Report Emulator that creates a Report given a WMTask/WMStep and a Job instance.
    """

    def __init__(self, **options):
        """
        ___init___

        Options contain the settings for producing the report instance from the provided step
        """
        self.step = options.get("WMStep", None)
        self.job = options.get("Job", None)
        return

    def addInputFilesToReport(self, report):
        """
        _addInputFilesToReport_

        Pull all of the input files out of the job and add them to the report.
        """
        report.addInputSource("PoolSource")

        for inputFile in self.job["input_files"]:
            inputFileSection = report.addInputFile("PoolSource", lfn=inputFile["lfn"],
                                                   size=inputFile["size"],
                                                   events=inputFile["events"])
            Report.addRunInfoToFile(inputFileSection, inputFile["runs"])

        return

    def determineOutputSize(self):
        """
        _determineOutputSize_

        Determine the total size of and number of events in the input files and
        use the job mask to scale that to something that would reasonably
        approximate the size of and number of events in the output.
        """
        totalSize = 0
        totalEvents = 0

        for inputFile in self.job["input_files"]:
            totalSize += inputFile["size"]
            totalEvents += inputFile["events"]

        if self.job["mask"]["FirstEvent"] is not None and \
                        self.job["mask"]["LastEvent"] is not None:
            outputTotalEvents = self.job["mask"]["LastEvent"] - self.job["mask"]["FirstEvent"] + 1
        else:
            outputTotalEvents = totalEvents

        outputSize = int(totalSize * outputTotalEvents / totalEvents )
        return (outputSize, outputTotalEvents)

    def addOutputFilesToReport(self, report):
        """
        _addOutputFilesToReport_

        Add output files to every output module in the step.  Scale the size
        and number of events in the output files appropriately.
        """
        (outputSize, outputEvents) = self.determineOutputSize()

        if not os.path.exists('ReportEmuTestFile.txt'):
            with open('ReportEmuTestFile.txt', 'w') as f:
                f.write('A Shubbery')

        for outputModuleName in self.step.listOutputModules():
            outputModuleSection = self.step.getOutputModule(outputModuleName)
            outputModuleSection.fixedLFN = False
            outputModuleSection.disableGUID = False

            outputLFN = "%s/%s.root" % (outputModuleSection.lfnBase,
                                        str(makeUUID()))
            outputFile = File(lfn=outputLFN, size=outputSize, events=outputEvents,
                              merged=False)
            outputFile.setLocation(self.job["location"])
            outputFile['pfn'] = "ReportEmuTestFile.txt"
            outputFile['guid'] = "ThisIsGUID"
            outputFile["checksums"] = {"adler32": "1234", "cksum": "5678"}
            outputFile["dataset"] = {"primaryDataset": outputModuleSection.primaryDataset,
                                     "processedDataset": outputModuleSection.processedDataset,
                                     "dataTier": outputModuleSection.dataTier,
                                     "applicationName": "cmsRun",
                                     "applicationVersion": self.step.getCMSSWVersion()}
            outputFile["module_label"] = outputModuleName

            outputFileSection = report.addOutputFile(outputModuleName, outputFile)
            for inputFile in self.job["input_files"]:
                Report.addRunInfoToFile(outputFileSection, inputFile["runs"])

        return

    def __call__(self):
        report = Report.Report(self.step.name())

        report.id = self.job["id"]
        report.task = self.job["task"]
        report.workload = None

        self.addInputFilesToReport(report)
        self.addOutputFilesToReport(report)
        return report
