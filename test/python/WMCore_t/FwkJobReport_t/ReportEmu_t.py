#!/usr/bin/env python
"""
_ReportEmu_t_

Tests for the JobReport emulator.
"""

__revision__ = "$Id: ReportEmu_t.py,v 1.2 2010/03/09 20:34:53 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

import unittest

from WMCore.DataStructs.File import File
from WMCore.DataStructs.Run import Run
from WMCore.DataStructs.Job import Job

from WMCore.FwkJobReport.ReportEmu import ReportEmu
from WMCore.WMSpec.StdSpecs.ReReco import rerecoWorkload

class ReportEmuTest(unittest.TestCase):
    """
    _ReportEmuTest_

    """
    def setUp(self):
        """
        _setUp_

        Setup some reasonable defaults for the ReReco workflow.
        """
        self.unmergedLFNBase = "/store/backfill/2/unmerged"
        self.mergedLFNBase = "/store/backfill/2"
        self.processingVersion = "v1"
        self.cmsswVersion = "CMSSW_3_4_2_patch1"
        self.acquisitionEra = "WMAgentCommissioining10"
        self.primaryDataset = "MinimumBias"

        arguments = {
            "OutputTiers" : ["RECO", "ALCA", "AOD"],
            "AcquisitionEra" : self.acquisitionEra,
            "GlobalTag" : "GR09_R_34X_V5::All",
            "LFNCategory" : self.mergedLFNBase,
            "TemporaryLFNCategory": self.unmergedLFNBase,
            "ProcessingVersion" : self.processingVersion,
            "Scenario" : "cosmics",
            "CMSSWVersion" : self.cmsswVersion,
            "InputDatasets" : "/MinimumBias/BeamCommissioning09-v1/RAW",
            "Emulate" : False,
            }
        
        self.workload = rerecoWorkload("Tier1ReReco", arguments)
        return

    def verifyOutputMetaData(self, outputFile, job):
        """
        _verifyOutputMetaData_

        Verify that metadata that in an emulated FWJR.  Most of the meta data
        should be the same as the input file.
        """
        goldenRuns = []

        for inputFile in job["input_files"]:
            for run in inputFile["runs"]:
                goldenRuns.append(Run(run.run, *run.lumis))

        assert len(outputFile["runs"]) == len(goldenRuns), \
                   "Error: Wrong number of runs in output file."

        for outputRun in outputFile["runs"]:
            for goldenRun in goldenRuns:
                if outputRun.run == goldenRun.run:
                    goldenRun.lumis.sort()
                    outputRun.lumis.sort()

                    if goldenRun.lumis == outputRun.lumis:
                        goldenRuns.remove(goldenRun)
                        break

        assert len(goldenRuns) == 0, \
               "Error: Run information wrong on output file."

        assert len(outputFile["locations"]) == 1,  \
               "Error: Wrong number of locations."

        assert list(outputFile["locations"])[0] == job["location"], \
               "Error: Output file at the wrong location."

        assert outputFile["merged"] == False, \
               "Error: Output should be unmerged."

        assert "adler32" in outputFile["checksums"].keys(), \
               "Error: Adler32 checksum missing."
        assert "cksum" in outputFile["checksums"].keys(), \
               "Error: CKSum checksum missing."

        return

    def testProcessing(self):
        """
        _testProcessing_

        Setup a processing workflow and job and verify that the FWJR produced
        by the emulator is reasonable.
        """
        rerecoTask = self.workload.getTask("ReReco")
        cmsRunStep = rerecoTask.getStep("cmsRun1")

        inputFile = File(lfn = "/path/to/test/lfn", size = 1048576, events = 1000, merged = True)
        inputFile.addRun(Run(1, *[1, 2, 3, 4, 5]))
        inputFile.addRun(Run(2, *[1, 2, 3, 4, 5, 6]))

        processingJob = Job(name = "ProcessingJob", files = [inputFile])
        processingJob["task"] = "/Tier1ReReco/ReReco"
        processingJob["mask"].setMaxAndSkipEvents(500, 0)
        processingJob["id"] = 1
        processingJob["location"] = "cmssrm.fnal.gov"

        emu = ReportEmu(WMStep = cmsRunStep.getTypeHelper(), Job = processingJob)
        report = emu()

        reportInputFiles = report.getInputFilesFromStep("cmsRun1")

        assert len(reportInputFiles) == 1, \
               "Error: Wrong number of input files for the job."
        assert reportInputFiles[0]["lfn"] == inputFile["lfn"], \
               "Error: Input LFNs do not match."
        assert reportInputFiles[0]["size"] == inputFile["size"], \
               "Error: Input file sizes do not match."
        assert reportInputFiles[0]["events"] == inputFile["events"], \
               "Error: Input file events do not match."

        goldenRuns = [Run(1, *[1, 2, 3, 4, 5]), Run(2, *[1, 2, 3, 4, 5, 6])]

        assert len(reportInputFiles[0]["runs"]) == len(goldenRuns), \
                   "Error: Wrong number of runs in input file."

        for inputRun in reportInputFiles[0]["runs"]:
            for goldenRun in goldenRuns:
                if inputRun.run == goldenRun.run:
                    goldenRun.lumis.sort()
                    inputRun.lumis.sort()

                    if goldenRun.lumis == inputRun.lumis:
                        goldenRuns.remove(goldenRun)
                        break

        assert len(goldenRuns) == 0, \
               "Error: Run information wrong on input file."

        recoOutputFiles = report.getFilesFromOutputModule("cmsRun1", "outputRECO")
        alcaOutputFiles = report.getFilesFromOutputModule("cmsRun1", "outputALCA")
        aodOutputFiles = report.getFilesFromOutputModule("cmsRun1", "outputAOD")
        
        assert len(recoOutputFiles) == 1, \
               "Error: There should only be one RECO output file."
        assert len(alcaOutputFiles) == 1, \
               "Error: There should only be one ALCA output file."
        assert len(aodOutputFiles) == 1, \
               "Error: There should only be one AOD output file."        

        assert recoOutputFiles[0]["ModuleLabel"] == "outputRECO", \
               "Error: RECO file has wrong output module."
        assert alcaOutputFiles[0]["ModuleLabel"] == "outputALCA", \
               "Error: ALCA file has wrong output module."
        assert aodOutputFiles[0]["ModuleLabel"] == "outputAOD", \
               "Error: AOD file has wrong output module."
        
        self.verifyOutputMetaData(recoOutputFiles[0], processingJob)
        self.verifyOutputMetaData(alcaOutputFiles[0], processingJob)
        self.verifyOutputMetaData(aodOutputFiles[0], processingJob)        

        # It seems there's no convention for processed dataset name so we won't
        # verify that here...
        lfnBase = "%s/%s/%s/" % (self.unmergedLFNBase, self.acquisitionEra, self.primaryDataset)
        assert recoOutputFiles[0]["lfn"].startswith(lfnBase), \
               "Error: LFN of RECO file is wrong."
        assert alcaOutputFiles[0]["lfn"].startswith(lfnBase), \
               "Error: LFN of ALCA file is wrong."
        assert aodOutputFiles[0]["lfn"].startswith(lfnBase), \
               "Error: LFN of AOD file is wrong."        

        dataTierMap = {"outputRECO": "RECO", "outputALCA": "ALCA", "outputAOD": "AOD"}
        for outputFile in [recoOutputFiles[0], alcaOutputFiles[0], aodOutputFiles[0]]:
            assert outputFile["dataset"]["applicationName"] == "cmsRun", \
                   "Error: Application name is incorrect."
            assert outputFile["dataset"]["primaryDataset"] == self.primaryDataset, \
                   "Error: Primary dataset is incorrect."
            assert outputFile["dataset"]["applicationVersion"] == self.cmsswVersion, \
                   "Error: Framework version is incorrect."
            assert outputFile["dataset"]["dataTier"] == dataTierMap[outputFile["ModuleLabel"]], \
                   "Error: Data tier is incorrect."

        return

    def testMerge(self):
        """
        _testMerge_

        Setup a merge workflow and job and verify that the FWJR produced by the
        emulator is reasonable.
        """
        #emu = ReportEmu(WMStep = self.cmssw, Job = self.job)
        #report = emu()        
        return
        
if __name__ == "__main__":
    unittest.main()
