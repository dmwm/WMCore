#!/usr/bin/env python
#pylint: disable-msg=W0201, W0142, W0102
# W0201: Steve defines all global vars in __call__
#   I don't know why, but I'm not getting blamed for it
# W0142: Dave loves the ** magic
# W0102: Dangerous default values?  I live on danger!
#   Allows us to use a dict as a default
"""
_ReReco_

Standard ReReco workflow.
"""

import os

from WMCore.WMSpec.StdSpecs.StdBase import StdBase

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
        
        "CouchUrl": os.environ.get("COUCHURL", None),
        "CouchDBName": "scf_wmagent_configcache",
        
        "ProcScenario": "cosmics",
        #"ProcConfigCacheID": "03da10e20c7b98c79f9d6a5c8900f83b",

        #"SkimConfigs": [{"SkimName": "Prescaler", "SkimInput": "output",
        #                "SkimSplitAlgo": "TwoFileBased",
        #                "SkimSplitArgs": {"files_per_job": 1},
        #                "ConfigCacheID": "3adb4bad8f05cabede27969face2e59d",
        #                "Scenario": None}]
        }

    return arguments

class ReRecoWorkloadFactory(StdBase):
    """
    _ReRecoWorkloadFactory_

    Stamp out ReReco workflows.
    """
    def __init__(self):
        StdBase.__init__(self)
        return

    def buildWorkload(self):
        """
        _buildWorkload_

        Build the workload given all of the input parameters.  At the very least
        this will create a processing task and merge tasks for all the outputs
        of the processing task.  For each skim that is passed in, skim tasks
        will be created and merge tasks will be created for each skim output.

        Not that there will be LogCollect tasks created for each processing
        task and Cleanup tasks created for each merge task.
        """
        (self.inputPrimaryDataset, self.inputProcessedDataset,
         self.inputDataTier) = self.inputDataset[1:].split("/")

        workload = self.createWorkload()
        self.setWorkQueueSplitPolicy(workload, "FileBased", {"files_per_job": 1})
        procTask = workload.newTask("ReReco")

        outputMods = self.setupProcessingTask(procTask, "Processing", self.inputDataset,
                                              scenarioName = self.procScenario, scenarioFunc = "promptReco",
                                              scenarioArgs = {"globalTag": self.globalTag, "writeTiers": ["RECO", "ALCARECO"]}, 
                                              couchURL = self.couchURL, couchDBName = self.couchDBName,
                                              configDoc = self.procConfigCacheID, splitAlgo = self.procJobSplitAlgo,
                                              splitArgs = self.procJobSplitArgs) 
        self.addLogCollectTask(procTask)

        procMergeTasks = {}
        for outputModuleName in outputMods.keys():
            outputModuleInfo = outputMods[outputModuleName]
            mergeTask = self.addMergeTask(procTask, self.procJobSplitAlgo,
                                          outputModuleName,
                                          outputModuleInfo["dataTier"],
                                          outputModuleInfo["processedDataset"])
            procMergeTasks[outputModuleName] = mergeTask

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
                                  outputModuleInfo["processedDataset"])

        return workload

    def __call__(self, workloadName, arguments):
        """
        _call_

        Create a ReReco workload with the given parameters.
        """
        StdBase.__call__(self, workloadName, arguments)

        # Required parameters that must be specified by the Requestor.
        self.inputDataset = arguments["InputDataset"]
        self.frameworkVersion = arguments["CMSSWVersion"]
        self.globalTag = arguments["GlobalTag"]

        # The CouchURL and name of the ConfigCache database must be passed in
        # by the ReqMgr or whatever is creating this workflow.
        self.couchURL = arguments["CouchUrl"]
        self.couchDBName = arguments["CouchDBName"]        

        # One of these parameters must be set.
        self.procConfigCacheID = arguments.get("ProcConfigCacheID", None)

        if arguments.has_key("Scenario"):
            self.procScenario = arguments.get("Scenario", None)
        else:
            self.procScenario = arguments.get("ProcScenario", None)

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
        self.procJobSplitAlgo  = arguments.get("StdJobSplitAlgo", "FileBased")
        self.procJobSplitArgs  = arguments.get("StdJobSplitArgs",
                                               {"files_per_job": 1})
        self.skimJobSplitAlgo = arguments.get("SkimJobSplitAlgo", "TwoFileBased")
        self.skimJobSplitArgs = arguments.get("SkimJobSplitArgs",
                                              {"files_per_job": 1})

        return self.buildWorkload()

def rerecoWorkload(workloadName, arguments):
    """
    _rerecoWorkload_

    Instantiate the ReRecoWorkflowFactory and have it generate a workload for
    the given parameters.
    """
    myReRecoFactory = ReRecoWorkloadFactory()
    return myReRecoFactory(workloadName, arguments)
