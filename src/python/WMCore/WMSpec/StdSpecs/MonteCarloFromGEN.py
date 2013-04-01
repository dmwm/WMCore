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
        "ProcessingVersion": 2,
        "SkimInput": "output",
        "GlobalTag": "GR10_P_v4::All",

        "CouchURL": os.environ.get("COUCHURL", None),
        "CouchDBName": "scf_wmagent_configcache",
        "ConfigCacheID": "03da10e20c7b98c79f9d6a5c8900f83b",
        # or alternatively CouchURL part can be replaced by ConfigCacheUrl,
        # then ConfigCacheUrl + CouchDBName + ConfigCacheID
        "ConfigCacheUrl": None,

        "DashboardHost" : "127.0.0.1",
        "DashboardPort" : 8884,
        }

    return arguments



class MonteCarloFromGENWorkloadFactory(StdBase):
    """
    _MonteCarloFromGENWorkloadFactory_

    Stamp out MonteCarloFromGEN workflows.
    """
    def __init__(self):
        StdBase.__init__(self)

        # Define attributes used by this spec
        self.openRunningTimeout = None

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
        workload.setDashboardActivity("lheproduction")
        self.reportWorkflowToDashboard(workload.getDashboardActivity())
        workload.setWorkQueueSplitPolicy("Block", self.procJobSplitAlgo, self.procJobSplitArgs,
                                         OpenRunningTimeout = self.openRunningTimeout)
        procTask = workload.newTask("MonteCarloFromGEN")

        outputMods = self.setupProcessingTask(procTask, "Processing", self.inputDataset,
                                              couchURL = self.couchURL, couchDBName = self.couchDBName,
                                              configCacheUrl = self.configCacheUrl,
                                              configDoc = self.configCacheID, splitAlgo = self.procJobSplitAlgo,
                                              splitArgs = self.procJobSplitArgs, stepType = "CMSSW",
                                              primarySubType = "Production", timePerEvent = self.timePerEvent,
                                              memoryReq = self.memory, sizePerEvent = self.sizePerEvent)
        self.addLogCollectTask(procTask)

        procMergeTasks = {}
        for outputModuleName in outputMods.keys():
            outputModuleInfo = outputMods[outputModuleName]
            mergeTask = self.addMergeTask(procTask, self.procJobSplitAlgo,
                                          outputModuleName)
            procMergeTasks[outputModuleName] = mergeTask

        workload.setBlockCloseSettings(workload.getBlockCloseMaxWaitTime(),
                                       workload.getBlockCloseMaxFiles(),
                                       25000000,
                                       workload.getBlockCloseMaxSize())

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
        self.configCacheUrl = arguments.get("ConfigCacheUrl", None)

        # MonteCarloFromGen is split by block and can receive more blocks after first split for certain delay
        self.openRunningTimeout = int(arguments.get("OpenRunningTimeout", 0))

        # Optional arguments that default to something reasonable.
        self.dbsUrl = arguments.get("DbsUrl", "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet")
        self.blockBlacklist = arguments.get("BlockBlacklist", [])
        self.blockWhitelist = arguments.get("BlockWhitelist", [])
        self.runBlacklist = arguments.get("RunBlacklist", [])
        self.runWhitelist = arguments.get("RunWhitelist", [])
        self.emulation = arguments.get("Emulation", False)

        self.configCacheID = arguments.get("ConfigCacheID")

        # These are mostly place holders because the job splitting algo and
        # parameters will be updated after the workflow has been created.
        self.procJobSplitAlgo  = arguments.get("StdJobSplitAlgo", "LumiBased")
        self.procJobSplitArgs  = arguments.get("StdJobSplitArgs", {"lumis_per_job": 1})
        return self.buildWorkload()
    

    def validateSchema(self, schema):
        """
        _validateSchema_

        Check for required fields, and some skim facts
        """
        arguments = getTestArguments()
        requiredFields = ["CMSSWVersion", "ConfigCacheID",
                          "GlobalTag", "InputDataset", "CouchURL",
                          "CouchDBName", "ScramArch"]
        self.requireValidateFields(fields = requiredFields, schema = schema,
                                   validate = False)
        couchUrl = schema.get("ConfigCacheUrl", None) or schema["CouchURL"]
        outMod = self.validateConfigCacheExists(configID = schema["ConfigCacheID"],
                                                couchURL = couchUrl,
                                                couchDBName = schema["CouchDBName"],
                                                getOutputModules = True)

        if schema.get("StdJobSplitAlgo", "LumiBased") == "LumiBased":
            if not schema.get("StdJobSplitArgs", {"lumis_per_job": 1}).get("lumis_per_job", 0) > 0:
                self.raiseValidationException(msg = "Invalid number of lumis_per_job for MCFromGEN")



def monteCarloFromGENWorkload(workloadName, arguments):
    """
    _monteCarloFromGENWorkload_

    Instantiate the MonteCarloFromGENWorkflowFactory and have it generate a workload for
    the given parameters.
    """
    myMonteCarloFromGENFactory = MonteCarloFromGENWorkloadFactory()
    return myMonteCarloFromGENFactory(workloadName, arguments)
