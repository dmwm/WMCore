#!/usr/bin/env python

import random

from WMCore.FwkJobReport import Report
from WMCore.DataStructs.Run import Run
from WMCore.Services.UUIDLib import makeUUID

outputModules = ["write_A_Calo_RAW", "write_A_Cosmics_RAW",
                 "write_A_HcalHPDNoise_RAW", "write_A_MinimumBias_RAW",
                 "write_A_RandomTriggers_RAW", "write_A_Calibration_TestEnables_RAW",
                 "write_HLTDEBUG_Monitor_RAW"]

runInfo = Run(1)
runInfo.extendLumis([11, 12, 13, 14, 15])

for i in range(100):
    loadTestReport = Report.Report("cmsRun1")
    loadTestReport.addInputSource("PoolSource")
    inputFile = loadTestReport.addInputFile("PoolSource", lfn = makeUUID(),
                                            events = 600000, size = 600000)
    Report.addRunInfoToFile(inputFile, runInfo)

    for outputModule in outputModules:
        loadTestReport.addOutputModule(outputModule)
        datasetInfo = {"applicationName": "cmsRun", "applicationVersion": "CMSSW_3_3_5_patch3",
                       "primaryDataset": outputModule, "dataTier": "RAW",
                       "processedDataset": "LoadTest10"}
        fileAttrs = {"lfn": makeUUID(), "location": "cmssrm.fnal.gov",
                     "checksums": {"adler32": "ff810ec3", "cksum": "2212831827"},
                     "events": random.randrange(500, 5000, 50),
                     "merged": random.choice([True, False]),
                     "size": random.randrange(1000, 2000, 100000000),
                     "module_label": outputModule, "dataset": datasetInfo}

        outputFile = loadTestReport.addOutputFile(outputModule, fileAttrs)
        Report.addRunInfoToFile(outputFile, runInfo)

    loadTestReport.persist("LoadTest%02d.pkl" % i)
