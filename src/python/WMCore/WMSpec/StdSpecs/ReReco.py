#!/usr/bin/env python
"""
_ReReco_

Standard ReReco workflow.
"""

import os

from WMCore.WMSpec.StdSpecs.DataProcessing import DataProcessingWorkloadFactory

def getTestArguments():
    """
    _getTestArguments_

    This should be where the default REQUIRED arguments go
    This serves as documentation for what is currently required 
    by the standard ReReco workload in importable format.

    NOTE: These are test values.  If used in real workflows they
    will cause everything to crash/die/break, and we will be forced
    to hunt you down and kill you.
    """
    arguments = {
        "AcquisitionEra": "WMAgentCommissioning10",
        "Requestor": "sfoulkes@fnal.gov",
        "InputDataset": "/MinimumBias/Commissioning10-v4/RAW",
        "CMSSWVersion": "CMSSW_3_5_8",
        "ScramArch": "slc5_ia32_gcc434",
        "ProcessingVersion": "v2scf",
        "SkimInput": "output",
        "GlobalTag": "GR10_P_v4::All",
        
        "CouchURL": os.environ.get("COUCHURL", None),
        "CouchDBName": "scf_wmagent_configcache",
        
        "ProcScenario": "cosmics",
        #"ProcConfigCacheID": "03da10e20c7b98c79f9d6a5c8900f83b",

        #"SkimConfigs": [{"SkimName": "Prescaler", "SkimInput": "output",
        #                "SkimSplitAlgo": "FileBased",
        #                "SkimSplitArgs": {"files_per_job": 1, "include_parents": True},
        #                "ConfigCacheID": "3adb4bad8f05cabede27969face2e59d",
        #                "Scenario": None}]
        }

    return arguments

class ReRecoWorkloadFactory(DataProcessingWorkloadFactory):
    """
    _ReRecoWorkloadFactory_

    Stamp out ReReco workflows.
    """
    def __init__(self):
        DataProcessingWorkloadFactory.__init__(self)
        return

    def addSkims(self, workload):
        """
        _addSkims_

        Add skims to the standard dataprocessing workload that was given.

        Note that there will be LogCollect tasks created for each processing
        task and Cleanup tasks created for each merge task.
        """
        procMergeTasks = {}
        procTask = workload.getTopLevelTask()[0]
        for mergeTask in procTask.childTaskIterator():
            if mergeTask.taskType() == "Merge":
                procMergeTasks[mergeTask.data.input.outputModule] = mergeTask
        
        for skimConfig in self.skimConfigs:
            if not procMergeTasks.has_key(skimConfig["SkimInput"]):
                error = "Processing config does not have the following output module: %s.  " % skimConfig["SkimInput"]
                error += "Please change your skim input to be one of the following: %s" % procMergeTasks.keys()
                raise Exception, error
        
            mergeTask = procMergeTasks[skimConfig["SkimInput"]]
            skimTask = mergeTask.addTask(skimConfig["SkimName"])
            parentCmsswStep = mergeTask.getStep("cmsRun1")
            outputMods = self.setupProcessingTask(skimTask, "Skim", inputStep = parentCmsswStep, inputModule = "Merged",
                                                  couchURL = self.couchURL, couchDBName = self.couchDBName,
                                                  configDoc = skimConfig["ConfigCacheID"], splitAlgo = self.skimJobSplitAlgo,
                                                  splitArgs = self.skimJobSplitArgs)
            self.addLogCollectTask(skimTask, taskName = "%sLogCollect" % skimConfig["SkimName"])

            for outputModuleName in outputMods.keys():
                outputModuleInfo = outputMods[outputModuleName]
                self.addMergeTask(skimTask, self.skimJobSplitAlgo,
                                  outputModuleName,
                                  outputModuleInfo["dataTier"],
                                  outputModuleInfo["filterName"],
                                  outputModuleInfo["processedDataset"])

        return workload

    def __call__(self, workloadName, arguments):
        """
        _call_

        Create a ReReco workload with the given parameters.
        """
        workload = DataProcessingWorkloadFactory.__call__(self, workloadName, arguments)

        # The SkimConfig parameter must be a list of dictionaries where each
        # dictionary will have the following keys:
        #  SkimName
        #  SkimInput
        #  SkimSplitAlgo - Optional at workflow creation time
        #  SkimSplitParams - Optional at workflow creation time
        #  ConfigCacheID
        #  Scenario
        #
        # The ConfigCacheID and Scenaio are mutually exclusive, only one can be
        # set.  The SkimSplitAlgo and SkimSplitParams don't have to be set when
        # the workflow is created but must be set when the workflow is approved.
        # The SkimInput is the name of the output module in the  processing step
        # that will be used as the input for the skim.
        self.skimConfigs = arguments.get("SkimConfigs", [])

        # These are mostly place holders because the job splitting algo and
        # parameters will be updated after the workflow has been created.
        self.skimJobSplitAlgo = arguments.get("SkimJobSplitAlgo", "FileBased")
        self.skimJobSplitArgs = arguments.get("SkimJobSplitArgs",
                                              {"files_per_job": 1,
                                               "include_parents": True})

        return self.addSkims(workload)

def rerecoWorkload(workloadName, arguments):
    """
    _rerecoWorkload_

    Instantiate the ReRecoWorkflowFactory and have it generate a workload for
    the given parameters.
    """
    myReRecoFactory = ReRecoWorkloadFactory()
    return myReRecoFactory(workloadName, arguments)
