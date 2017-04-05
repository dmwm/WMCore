#!/usr/bin/env python

import random

from WMCore.FwkJobReport import Report
from WMCore.DataStructs.Run import Run
from WMCore.Services.UUIDLib import makeUUID

outputModules = ["outputModule1", "outputModule2", "outputModule3",
                 "outputModule4", "outputModule5", "outputModule6",
                 "outputModule7", "outputModule8", "outputModule9",
                 "outputModule10"]

runInfo = Run(1)
runInfo.extendLumis([11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
                     25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
                     39, 40])

totalReports = 25
inputFilesPerReport = 50

inputFileCounter = 0
for i in range(totalReports):
    loadTestReport = Report.Report("cmsRun1")
    loadTestReport.addInputSource("PoolSource")

    for j in range(inputFilesPerReport):
        inputFile = loadTestReport.addInputFile("PoolSource", lfn = "input%i" % inputFileCounter,
                                                events = 600000, size = 600000)
        inputFileCounter += 1

    Report.addRunInfoToFile(inputFile, runInfo)

    for outputModule in outputModules:
        loadTestReport.addOutputModule(outputModule)
        datasetInfo = {"applicationName": "cmsRun", "applicationVersion": "CMSSW_3_3_5_patch3",
                       "primaryDataset": outputModule, "dataTier": "RAW",
                       "processedDataset": "LoadTest10"}
        fileAttrs = {"lfn": makeUUID(), "location": "cmssrm.fnal.gov",
                     "checksums": {"adler32": "ff810ec3", "cksum": "2212831827"},
                     "events": random.randrange(500, 5000, 50),
                     "merged": True,
                     "size": random.randrange(1000, 2000, 100000000),
                     "module_label": outputModule, "dataset": datasetInfo}

        outputFile = loadTestReport.addOutputFile(outputModule, fileAttrs)
        Report.addRunInfoToFile(outputFile, runInfo)

    loadTestReport.persist("HeritageTest%02d.pkl" % i)
