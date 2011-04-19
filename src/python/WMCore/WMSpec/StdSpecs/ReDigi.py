#!/usr/bin/env python
"""
_ReDigi_

Standard two/three step redigi workflow.
"""

import os

from WMCore.WMSpec.StdSpecs.StdBase import StdBase
import WMCore.WMSpec.Steps.StepFactory as StepFactory

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
        "CMSSWVersion": "CMSSW_3_9_7",
        "ScramArch": "slc5_ia32_gcc434",
        "ProcessingVersion": "v2scf",
        "GlobalTag": "GR10_P_v4::All",

        "RAWOutputModuleName": "RAWDEBUGoutput",
        "RECOOutputModuleName": "RECODEBUGoutput",
        "StepTwoConfigCacheID": "3a4548750b61f485d42b4aa850ba385e",
        "StepOneConfigCacheID": "3a4548750b61f485d42b4aa850b9ede5",
        "StepThreeConfigCacheID": "3a4548750b61f485d42b4aa850ba4ab7",
        "KeepRAW": True,
        
        "CouchURL": os.environ.get("COUCHURL", None),
        "CouchDBName": "wmagent_configcachescf",
        }

    return arguments

class ReDigiWorkloadFactory(StdBase):
    """
    _ReDigiWorkloadFactory_

    Stamp out ReDigi workflows.
    """
    def __init__(self):
        StdBase.__init__(self)
        return

    def setupDependentProcessing(self, stepOneTask, outputMods):
        """
        _setupDependentProcessing_

        Setup seperate tasks for step one and step two processing.  Since we're
        keeping the RAW we need to also setup merge tasks for it.
        """
        stepOneMergeTasks = {}
        for outputModuleName in outputMods.keys():
            outputModuleInfo = outputMods[outputModuleName]
            mergeTask = self.addMergeTask(stepOneTask, self.procJobSplitAlgo,
                                          outputModuleName,
                                          outputModuleInfo["dataTier"],
                                          outputModuleInfo["filterName"],
                                          outputModuleInfo["processedDataset"])
            stepOneMergeTasks[outputModuleName] = mergeTask

        stepOneMergeTask = stepOneMergeTasks[self.rawOutputModuleName]
        stepTwoTask = stepOneMergeTask.addTask("ReDigiReReco")

        parentCmsswStep = stepOneMergeTask.getStep("cmsRun1")
        outputMods = self.setupProcessingTask(stepTwoTask, "Processing", inputStep = parentCmsswStep,
                                              inputModule = "Merged", couchURL = self.couchURL,
                                              couchDBName = self.couchDBName,
                                              configDoc = self.stepTwoConfigCacheID,
                                              splitAlgo = self.procJobSplitAlgo,
                                              splitArgs = self.procJobSplitArgs, stepType = "CMSSW")
        self.addLogCollectTask(stepTwoTask, taskName = "StepTwoLogCollect")
        
        return (stepTwoTask, "cmsRun1", outputMods)

    def setupChainedProcessing(self, stepOneTask):
        """
        _setupChainedProcessing_

        Modify the step one task to include a second chained CMSSW step to
        do RECO on the RAW.
        """
        parentCmsswStep = stepOneTask.getStep("cmsRun1")
        parentCmsswStepHelper = parentCmsswStep.getTypeHelper()
        parentCmsswStepHelper.keepOutput(False)
        stepTwoCmssw = parentCmsswStep.addTopStep("cmsRun2")
        stepTwoCmssw.setStepType("CMSSW")

        template = StepFactory.getStepTemplate("CMSSW")
        template(stepTwoCmssw.data)

        stepTwoCmsswHelper = stepTwoCmssw.getTypeHelper()
        stepTwoCmsswHelper.setGlobalTag(self.globalTag)
        stepTwoCmsswHelper.setupChainedProcessing("cmsRun1", self.rawOutputModuleName)
        stepTwoCmsswHelper.cmsswSetup(self.frameworkVersion, softwareEnvironment = "",
                                      scramArch = self.scramArch)
        stepTwoCmsswHelper.setConfigCache(self.couchURL, self.stepTwoConfigCacheID,
                                          self.couchDBName)
        configOutput = self.determineOutputModules(None, None, self.stepTwoConfigCacheID,
                                                   self.couchURL, self.couchDBName)
        outputMods = {}
        for outputModuleName in configOutput.keys():
            outputModule = self.addOutputModule(stepOneTask, outputModuleName,
                                                configOutput[outputModuleName]["dataTier"],
                                                configOutput[outputModuleName]["filterName"],
                                                stepName = "cmsRun2")
            outputMods[outputModuleName] = outputModule

        return (stepOneTask, "cmsRun2", outputMods)
    
    def buildWorkload(self):
        """
        _buildWorkload_

        Build the workload given all of the input parameters.

        Not that there will be LogCollect tasks created for each processing
        task and Cleanup tasks created for each merge task.
        """
        (self.inputPrimaryDataset, self.inputProcessedDataset,
         self.inputDataTier) = self.inputDataset[1:].split("/")
        
        workload = self.createWorkload()
        workload.setWorkQueueSplitPolicy("Block", self.procJobSplitAlgo, self.procJobSplitArgs)
        stepOneTask = workload.newTask("ReDigi")

        outputMods = self.setupProcessingTask(stepOneTask, "Processing", self.inputDataset,
                                              couchURL = self.couchURL, couchDBName = self.couchDBName,
                                              configDoc = self.stepOneConfigCacheID,
                                              splitAlgo = self.procJobSplitAlgo,
                                              splitArgs = self.procJobSplitArgs, stepType = "CMSSW")
        self.addLogCollectTask(stepOneTask)
        if self.pileupConfig:
            self.setupPileup(stepOneTask, self.pileupConfig)

        if self.stepTwoConfigCacheID == None or self.rawOutputModuleName == None or \
               self.stepTwoConfigCacheID == "" or self.rawOutputModuleName == "":
            parentTask = stepOneTask
            parentStepName = "cmsRun1"
            outputModes = outputMods
        elif self.keepRAW == True or self.keepRAW == "True":
            (parentTask, parentStepName, outputMods) = self.setupDependentProcessing(stepOneTask, outputMods)
        else:
            (parentTask, parentStepName, outputMods) = self.setupChainedProcessing(stepOneTask)

        stepTwoMergeTasks = {}
        for outputModuleName in outputMods.keys():
            outputModuleInfo = outputMods[outputModuleName]
            mergeTask = self.addMergeTask(parentTask, self.procJobSplitAlgo,
                                          outputModuleName,
                                          outputModuleInfo["dataTier"],
                                          outputModuleInfo["filterName"],
                                          outputModuleInfo["processedDataset"],
                                          parentStepName = parentStepName)
            stepTwoMergeTasks[outputModuleName] = mergeTask

        stepTwoMergeTask = stepTwoMergeTasks[self.recoOutputModuleName]
        stepThreeTask = stepTwoMergeTask.addTask("AODProd")
        parentCmsswStep = stepTwoMergeTask.getStep("cmsRun1")

        outputMods = self.setupProcessingTask(stepThreeTask, "Processing", inputStep = parentCmsswStep,
                                              inputModule = "Merged", couchURL = self.couchURL,
                                              couchDBName = self.couchDBName,
                                              configDoc = self.stepThreeConfigCacheID,
                                              splitAlgo = self.procJobSplitAlgo,
                                              splitArgs = self.procJobSplitArgs, stepType = "CMSSW")
        self.addLogCollectTask(stepThreeTask, "StepThreeLogCollect")

        for outputModuleName in outputMods.keys():
            outputModuleInfo = outputMods[outputModuleName]
            self.addMergeTask(stepThreeTask, self.procJobSplitAlgo,
                              outputModuleName,
                              outputModuleInfo["dataTier"],
                              outputModuleInfo["filterName"],
                              outputModuleInfo["processedDataset"])
        return workload

    def __call__(self, workloadName, arguments):
        """
        _call_

        Create a ReDigi workload with the given parameters.
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

        # Pull down the configs and the names of the output modules so that
        # we can chain things together properly.
        self.rawOutputModuleName = arguments.get("RAWOutputModuleName", None)
        self.recoOutputModuleName = arguments.get("RECOOutputModuleName")
        self.stepOneConfigCacheID = arguments.get("StepOneConfigCacheID")
        self.stepTwoConfigCacheID = arguments.get("StepTwoConfigCacheID", None)
        self.stepThreeConfigCacheID = arguments.get("StepThreeConfigCacheID")
        self.keepRAW = arguments.get("KeepRAW", True)

        # Pileup configuration for the first generation task
        self.pileupConfig = arguments.get("PileupConfig", None)

        # Optional arguments that default to something reasonable.
        self.dbsUrl = arguments.get("DbsUrl", "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet")
        self.blockBlacklist = arguments.get("BlockBlacklist", [])
        self.blockWhitelist = arguments.get("BlockWhitelist", [])
        self.runBlacklist = arguments.get("RunBlacklist", [])
        self.runWhitelist = arguments.get("RunWhitelist", [])
        self.emulation = arguments.get("Emulation", False)

        # These are mostly place holders because the job splitting algo and
        # parameters will be updated after the workflow has been created.
        self.procJobSplitAlgo = arguments.get("StdJobSplitAlgo", "LumiBased")
        self.procJobSplitArgs = arguments.get("StdJobSplitArgs",
                                              {"lumis_per_job": 15})
        return self.buildWorkload()

def reDigiWorkload(workloadName, arguments):
    """
    _reDigiWorkload_

    Instantiate the ReDigiWorkflowFactory and have it generate a workload for
    the given parameters.
    """
    myReDigiFactory = ReDigiWorkloadFactory()
    return myReDigiFactory(workloadName, arguments)
