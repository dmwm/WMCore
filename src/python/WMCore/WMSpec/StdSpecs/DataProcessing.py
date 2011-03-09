#!/usr/bin/env python
"""
_DataProcessing_

Standard DataProcessing workflow: a processing task and a merge for all outputs.
"""

import os

from WMCore.WMSpec.StdSpecs.StdBase import StdBase

def getTestArguments():
    """
    _getTestArguments_

    This should be where the default REQUIRED arguments go
    This serves as documentation for what is currently required 
    by the standard DataProcessing workload in importable format.

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
        "Multicore" : 4,
        }

    return arguments

class DataProcessingWorkloadFactory(StdBase):
    """
    _DataProcessingWorkloadFactory_

    Stamp out DataProcessing workflows.
    """
    def __init__(self):
        StdBase.__init__(self)
        self.multicore = False
        self.multicoreNCores = 1
        return

    def buildWorkload(self):
        """
        _buildWorkload_

        Build the workload given all of the input parameters.  At the very least
        this will create a processing task and merge tasks for all the outputs
        of the processing task.

        Not that there will be LogCollect tasks created for each processing
        task and Cleanup tasks created for each merge task.
        """
        (self.inputPrimaryDataset, self.inputProcessedDataset,
         self.inputDataTier) = self.inputDataset[1:].split("/")

        workload = self.createWorkload()
        workload.setWorkQueueSplitPolicy("Block", self.procJobSplitAlgo, self.procJobSplitArgs)
        procTask = workload.newTask("DataProcessing")

        cmsswStepType = "CMSSW"
        if self.multicore:
            cmsswStepType = "MulticoreCMSSW"
        outputMods = self.setupProcessingTask(procTask, "Processing", self.inputDataset,
                                              scenarioName = self.procScenario, scenarioFunc = "promptReco",
                                              scenarioArgs = {"globalTag": self.globalTag, "writeTiers": ["RECO", "ALCARECO"]}, 
                                              couchURL = self.couchURL, couchDBName = self.couchDBName,
                                              configDoc = self.procConfigCacheID, splitAlgo = self.procJobSplitAlgo,
                                              splitArgs = self.procJobSplitArgs, stepType = cmsswStepType) 
        self.addLogCollectTask(procTask)
        if self.multicore:
            cmsswStep = procTask.getStep("cmsRun1")
            multicoreHelper = cmsswStep.getTypeHelper()
            multicoreHelper.setMulticoreCores(self.multicoreNCores)

        procMergeTasks = {}
        for outputModuleName in outputMods.keys():
            outputModuleInfo = outputMods[outputModuleName]
            mergeTask = self.addMergeTask(procTask, self.procJobSplitAlgo,
                                          outputModuleName,
                                          outputModuleInfo["dataTier"],
                                          outputModuleInfo["filterName"],
                                          outputModuleInfo["processedDataset"])
            procMergeTasks[outputModuleName] = mergeTask

        return workload

    def __call__(self, workloadName, arguments):
        """
        _call_

        Create a DataProcessing workload with the given parameters.
        """
        StdBase.__call__(self, workloadName, arguments)

        # Required parameters that must be specified by the Requestor.
        self.inputDataset = arguments["InputDataset"]
        self.frameworkVersion = arguments["CMSSWVersion"]
        self.globalTag = arguments["GlobalTag"]

        # The CouchURL and name of the ConfigCache database must be passed in
        # by the ReqMgr or whatever is creating this workflow.
        self.couchURL = arguments["CouchURL"]
        self.couchDBName = arguments["CouchDBName"]        

        # One of these parameters must be set.
        if arguments.has_key("ProdConfigCacheID"):
            self.procConfigCacheID = arguments["ProdConfigCacheID"]
        else:
            self.procConfigCacheID = arguments.get("ProcConfigCacheID", None)

        if arguments.has_key("Scenario"):
            self.procScenario = arguments.get("Scenario", None)
        else:
            self.procScenario = arguments.get("ProcScenario", None)

        if arguments.has_key("Multicore"):
            numCores = arguments.get("Multicore")
            if numCores == None:
                self.multicore = False
            elif numCores == "auto":
                self.multicore = True
                self.multicoreNCores = "auto"
            else:
                self.multicore = True
                self.multicoreNCores = numCores

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

        # Optional arguments that default to something reasonable.
        self.dbsUrl = arguments.get("DbsUrl", "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet")
        self.blockBlacklist = arguments.get("BlockBlacklist", [])
        self.blockWhitelist = arguments.get("BlockWhitelist", [])
        self.runBlacklist = arguments.get("RunBlacklist", [])
        self.runWhitelist = arguments.get("RunWhitelist", [])
        self.emulation = arguments.get("Emulation", False)

        # These are mostly place holders because the job splitting algo and
        # parameters will be updated after the workflow has been created.
        self.procJobSplitAlgo  = arguments.get("StdJobSplitAlgo", "LumiBased")
        self.procJobSplitArgs  = arguments.get("StdJobSplitArgs",
                                               {"lumis_per_job": 15})
        return self.buildWorkload()

def dataProcessingWorkload(workloadName, arguments):
    """
    _dataProcessingWorkload_

    Instantiate the DataProcessingWorkflowFactory and have it generate a workload for
    the given parameters.
    """
    myDataProcessingFactory = DataProcessingWorkloadFactory()
    return myDataProcessingFactory(workloadName, arguments)


