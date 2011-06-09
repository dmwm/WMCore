#!/usr/bin/env python
"""
_MonteCarloFromGEN_

Workflow for processing MonteCarlo GEN files.
"""

import os

from WMCore.WMSpec.StdSpecs.StdBase import StdBase

def getTestArguments():
    """
    _getTestArguments_

    This should be where the default REQUIRED arguments go
    This serves as documentation for what is currently required 
    by the standard MonteCarloFromGEN workload in importable format.

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
        
        "ProcConfigCacheID": "03da10e20c7b98c79f9d6a5c8900f83b",
        }

    return arguments

class MonteCarloFromGENWorkloadFactory(StdBase):
    """
    _MonteCarloFromGENWorkloadFactory_

    Stamp out MonteCarloFromGEN workflows.
    """
    def __init__(self):
        StdBase.__init__(self)
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
        procTask = workload.newTask("MonteCarloFromGEN")

        outputMods = self.setupProcessingTask(procTask, "Production", self.inputDataset,
                                              couchURL = self.couchURL, couchDBName = self.couchDBName,
                                              configDoc = self.procConfigCacheID, splitAlgo = self.procJobSplitAlgo,
                                              splitArgs = self.procJobSplitArgs, stepType = "CMSSW")
        self.addLogCollectTask(procTask)

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

        Create a MonteCarloFromGEN workload with the given parameters.
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

        # Optional arguments that default to something reasonable.
        self.dbsUrl = arguments.get("DbsUrl", "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet")
        self.blockBlacklist = arguments.get("BlockBlacklist", [])
        self.blockWhitelist = arguments.get("BlockWhitelist", [])
        self.runBlacklist = arguments.get("RunBlacklist", [])
        self.runWhitelist = arguments.get("RunWhitelist", [])
        self.emulation = arguments.get("Emulation", False)

        self.procConfigCacheID = arguments.get("ProcConfigCacheID")

        # These are mostly place holders because the job splitting algo and
        # parameters will be updated after the workflow has been created.
        self.procJobSplitAlgo  = arguments.get("StdJobSplitAlgo", "EventBased")
        self.procJobSplitArgs  = arguments.get("StdJobSplitArgs",
                                               {"events_per_job": 750,
                                                "include_parents": self.includeParents})
        return self.buildWorkload()

def monteCarloFromGENWorkload(workloadName, arguments):
    """
    _monteCarloFromGENWorkload_

    Instantiate the MonteCarloFromGENWorkflowFactory and have it generate a workload for
    the given parameters.
    """
    myMonteCarloFromGENFactory = MonteCarloFromGENWorkloadFactory()
    return myMonteCarloFromGENFactory(workloadName, arguments)
