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
        "ProcessingVersion": 2,
        "GlobalTag": "GR10_P_v4::All",

        "StepOneOutputModuleName": "RAWDEBUGoutput",
        "StepTwoOutputModuleName": "RECODEBUGoutput",
        "StepTwoConfigCacheID": "3a4548750b61f485d42b4aa850ba385e",
        "StepOneConfigCacheID": "3a4548750b61f485d42b4aa850b9ede5",
        "StepThreeConfigCacheID": "3a4548750b61f485d42b4aa850ba4ab7",
        "KeepStepOneOutput": True,
        "KeepStepTwoOutput": True,

        "CouchURL": os.environ.get("COUCHURL", None),
        "CouchDBName": "wmagent_configcachescf",
        # or alternatively CouchURL part can be replaced by ConfigCacheUrl,
        # then ConfigCacheUrl + CouchDBName + ConfigCacheID
        "ConfigCacheUrl": None,        
        "PileupConfig": {"mc": ["/mixing/pileup/dataset"]},
        
        "DashboardHost": "127.0.0.1",
        "DashboardPort": 8884,
        }

    return arguments

class ReDigiWorkloadFactory(StdBase):
    """
    _ReDigiWorkloadFactory_

    Stamp out ReDigi workflows.
    """
    def __init__(self):
        StdBase.__init__(self)

        # Define attributes used by this spec
        self.openRunningTimeout = None
        self.stepTwoMemory = None
        self.stepTwoSizePerEvent = None
        self.stepTwoTimePerEvent = None
        self.stepThreeMemory = None
        self.stepThreeSizePerEvent = None
        self.stepThreeTimePerEvent = None

        return

    def addMergeTasks(self, parentTask, parentStepName, outputMods):
        """
        _addMergeTasks_

        Add merge and cleanup tasks for the output of a processing step.
        """
        mergeTasks = {}
        for outputModuleName in outputMods.keys():
            outputModuleInfo = outputMods[outputModuleName]
            mergeTask = self.addMergeTask(parentTask, self.procJobSplitAlgo,
                                          outputModuleName, parentStepName)
            mergeTasks[outputModuleName] = mergeTask

        return mergeTasks

    def addDependentProcTask(self, taskName, parentMergeTask, configCacheID,
                             timePerEvent, sizePerEvent, memoryReq):
        """
        _addDependentProcTask_

        Add a dependent processing tasks to a merge tasks.
        """
        parentCmsswStep = parentMergeTask.getStep("cmsRun1")
        newTask = parentMergeTask.addTask(taskName)
        outputMods = self.setupProcessingTask(newTask, "Processing", inputStep = parentCmsswStep,
                                              inputModule = "Merged", couchURL = self.couchURL,
                                              couchDBName = self.couchDBName,
                                              configDoc = configCacheID,
                                              configCacheUrl = self.configCacheUrl,
                                              splitAlgo = self.procJobSplitAlgo,
                                              splitArgs = self.procJobSplitArgs, stepType = "CMSSW",
                                              timePerEvent = timePerEvent, sizePerEvent = sizePerEvent,
                                              memoryReq = memoryReq)
        self.addLogCollectTask(newTask, taskName = taskName + "LogCollect")
        mergeTasks = self.addMergeTasks(newTask, "cmsRun1", outputMods)
        return mergeTasks

    def setupThreeStepChainedProcessing(self, stepOneTask):
        """
        _setupThreeStepChainedProcessing_

        Modify the step one task to include two more CMSSW steps and chain the
        output between all three steps.
        
        """
        configCacheUrl = self.configCacheUrl or self.couchURL
        parentCmsswStep = stepOneTask.getStep("cmsRun1")
        parentCmsswStepHelper = parentCmsswStep.getTypeHelper()
        parentCmsswStepHelper.keepOutput(False)
        stepTwoCmssw = parentCmsswStep.addTopStep("cmsRun2")
        stepTwoCmssw.setStepType("CMSSW")

        template = StepFactory.getStepTemplate("CMSSW")
        template(stepTwoCmssw.data)

        stepTwoCmsswHelper = stepTwoCmssw.getTypeHelper()
        stepTwoCmsswHelper.setGlobalTag(self.globalTag)
        stepTwoCmsswHelper.setupChainedProcessing("cmsRun1", self.stepOneOutputModuleName)
        stepTwoCmsswHelper.cmsswSetup(self.frameworkVersion, softwareEnvironment = "",
                                      scramArch = self.scramArch)
        
        stepTwoCmsswHelper.setConfigCache(configCacheUrl, self.stepTwoConfigCacheID,
                                          self.couchDBName)
        stepTwoCmsswHelper.keepOutput(False)

        stepThreeCmssw = stepTwoCmssw.addTopStep("cmsRun3")
        stepThreeCmssw.setStepType("CMSSW")
        template(stepThreeCmssw.data)
        stepThreeCmsswHelper = stepThreeCmssw.getTypeHelper()
        stepThreeCmsswHelper.setGlobalTag(self.globalTag)
        stepThreeCmsswHelper.setupChainedProcessing("cmsRun2", self.stepTwoOutputModuleName)
        stepThreeCmsswHelper.cmsswSetup(self.frameworkVersion, softwareEnvironment = "",
                                      scramArch = self.scramArch)
        stepThreeCmsswHelper.setConfigCache(configCacheUrl, self.stepThreeConfigCacheID,
                                          self.couchDBName)

        configOutput = self.determineOutputModules(None, None, self.stepTwoConfigCacheID,
                                                   configCacheUrl, self.couchDBName)
        for outputModuleName in configOutput.keys():
            outputModule = self.addOutputModule(stepOneTask,
                                                outputModuleName,
                                                self.inputPrimaryDataset,
                                                configOutput[outputModuleName]["dataTier"],
                                                configOutput[outputModuleName]["filterName"],
                                                stepName = "cmsRun2")

        configOutput = self.determineOutputModules(None, None, self.stepThreeConfigCacheID,
                                                   configCacheUrl, self.couchDBName)
        outputMods = {}
        for outputModuleName in configOutput.keys():
            outputModule = self.addOutputModule(stepOneTask,
                                                outputModuleName,
                                                self.inputPrimaryDataset,
                                                configOutput[outputModuleName]["dataTier"],
                                                configOutput[outputModuleName]["filterName"],
                                                stepName = "cmsRun3")
            outputMods[outputModuleName] = outputModule

        self.addMergeTasks(stepOneTask, "cmsRun3", outputMods)
        return

    def setupDependentProcessing(self, stepOneTask, outputMods):
        """
        _setupDependentProcessing_

        Setup seperate tasks for all processing.
        """
        stepOneMergeTasks = self.addMergeTasks(stepOneTask, "cmsRun1", outputMods)

        if self.stepTwoConfigCacheID == None or self.stepTwoConfigCacheID == "":
            return

        stepOneMergeTask = stepOneMergeTasks[self.stepOneOutputModuleName]
        stepTwoMergeTasks = self.addDependentProcTask("StepTwoProc",
                                                      stepOneMergeTask,
                                                      self.stepTwoConfigCacheID,
                                                      timePerEvent = self.stepTwoTimePerEvent,
                                                      sizePerEvent = self.stepTwoSizePerEvent,
                                                      memoryReq = self.stepTwoMemory)

        if self.stepThreeConfigCacheID == None or self.stepThreeConfigCacheID == "":
            return

        stepTwoMergeTask = stepTwoMergeTasks[self.stepTwoOutputModuleName]
        self.addDependentProcTask("StepThreeProc", stepTwoMergeTask,
                                  self.stepThreeConfigCacheID,
                                  timePerEvent = self.stepThreeTimePerEvent,
                                  sizePerEvent = self.stepThreeSizePerEvent,
                                  memoryReq = self.stepThreeMemory)
        return

    def setupChainedProcessing(self, stepOneTask):
        """
        _setupChainedProcessing_

        Modify the step one task to include a second chained CMSSW step to
        do RECO on the RAW.
        
        """
        configCacheUrl = self.configCacheUrl or self.couchURL
        parentCmsswStep = stepOneTask.getStep("cmsRun1")
        parentCmsswStepHelper = parentCmsswStep.getTypeHelper()
        parentCmsswStepHelper.keepOutput(False)
        stepTwoCmssw = parentCmsswStep.addTopStep("cmsRun2")
        stepTwoCmssw.setStepType("CMSSW")

        template = StepFactory.getStepTemplate("CMSSW")
        template(stepTwoCmssw.data)

        stepTwoCmsswHelper = stepTwoCmssw.getTypeHelper()
        stepTwoCmsswHelper.setGlobalTag(self.globalTag)
        stepTwoCmsswHelper.setupChainedProcessing("cmsRun1", self.stepOneOutputModuleName)
        stepTwoCmsswHelper.cmsswSetup(self.frameworkVersion, softwareEnvironment = "",
                                      scramArch = self.scramArch)
        stepTwoCmsswHelper.setConfigCache(configCacheUrl, self.stepTwoConfigCacheID,
                                          self.couchDBName)
        configOutput = self.determineOutputModules(None, None, self.stepTwoConfigCacheID,
                                                   configCacheUrl, self.couchDBName)
        outputMods = {}
        for outputModuleName in configOutput.keys():
            outputModule = self.addOutputModule(stepOneTask,
                                                outputModuleName,
                                                self.inputPrimaryDataset,
                                                configOutput[outputModuleName]["dataTier"],
                                                configOutput[outputModuleName]["filterName"],
                                                stepName = "cmsRun2")
            outputMods[outputModuleName] = outputModule

        mergeTasks = self.addMergeTasks(stepOneTask, "cmsRun2", outputMods)

        if self.stepThreeConfigCacheID == None or self.stepThreeConfigCacheID == "":
            return

        mergeTask = mergeTasks[self.stepTwoOutputModuleName]
        self.addDependentProcTask("StepThreeProc", mergeTask,
                                  self.stepThreeConfigCacheID,
                                  timePerEvent = self.stepThreeTimePerEvent,
                                  sizePerEvent = self.stepThreeSizePerEvent,
                                  memoryReq = self.stepThreeMemory)
        return

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
        workload.setDashboardActivity("redigi")
        self.reportWorkflowToDashboard(workload.getDashboardActivity())
        workload.setWorkQueueSplitPolicy("Block", self.procJobSplitAlgo, self.procJobSplitArgs,
                                         OpenRunningTimeout = self.openRunningTimeout)
        stepOneTask = workload.newTask("StepOneProc")

        outputMods = self.setupProcessingTask(stepOneTask, "Processing", self.inputDataset,
                                              couchURL = self.couchURL, couchDBName = self.couchDBName,
                                              configCacheUrl = self.configCacheUrl,
                                              configDoc = self.stepOneConfigCacheID,
                                              splitAlgo = self.procJobSplitAlgo,
                                              splitArgs = self.procJobSplitArgs, stepType = "CMSSW",
                                              timePerEvent = self.timePerEvent, memoryReq = self.memory,
                                              sizePerEvent = self.sizePerEvent)
        self.addLogCollectTask(stepOneTask)

        if (self.keepStepOneOutput == True or self.keepStepOneOutput == "True") \
               and (self.keepStepTwoOutput == True or self.keepStepTwoOutput == "True"):
            self.setupDependentProcessing(stepOneTask, outputMods)
        elif (self.keepStepOneOutput == False or self.keepStepOneOutput == "False") \
                 and (self.keepStepTwoOutput == True or self.keepStepTwoOutput == "True"):
            self.setupChainedProcessing(stepOneTask)
        elif (self.keepStepOneOutput == False or self.keepStepOneOutput == "False") \
                 and (self.keepStepTwoOutput == False or self.keepStepTwoOutput == "False"):
            self.setupThreeStepChainedProcessing(stepOneTask)
        else:
            # Steps one and two are dependent, step three is chained.
            pass

        if self.pileupConfig:
            self.setupPileup(stepOneTask, self.pileupConfig)

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
        self.configCacheUrl = arguments.get("ConfigCacheUrl", None)

        # ReDigi is split by block and can receive more blocks after first split for certain delay
        self.openRunningTimeout = int(arguments.get("OpenRunningTimeout", 0))

        # Pull down the configs and the names of the output modules so that
        # we can chain things together properly.
        self.stepOneOutputModuleName = arguments.get("StepOneOutputModuleName", None)
        self.stepTwoOutputModuleName = arguments.get("StepTwoOutputModuleName")
        self.stepOneConfigCacheID = arguments.get("StepOneConfigCacheID")
        self.stepTwoConfigCacheID = arguments.get("StepTwoConfigCacheID", None)
        self.stepThreeConfigCacheID = arguments.get("StepThreeConfigCacheID")
        self.keepStepOneOutput = arguments.get("KeepStepOneOutput", True)
        self.keepStepTwoOutput = arguments.get("KeepStepTwoOutput", True)

        # Check extra performance information
        self.stepTwoTimePerEvent = float(arguments.get("StepTwoTimePerEvent", self.timePerEvent))
        self.stepTwoSizePerEvent = float(arguments.get("StepTwoSizePerEvent", self.sizePerEvent))
        self.stepTwoMemory = float(arguments.get("StepTwoMemory", self.memory))
        self.stepThreeTimePerEvent = float(arguments.get("StepThreeTimePerEvent", self.timePerEvent))
        self.stepThreeSizePerEvent = float(arguments.get("StepThreeSizePerEvent", self.sizePerEvent))
        self.stepThreeMemory = float(arguments.get("StepThreeMemory", self.memory))


        # Pileup configuration for the first generation task
        self.pileupConfig = arguments.get("PileupConfig", None)
        # If the pileup is data pileup, we may want deterministic pileup
        self.deterministicPileup = arguments.get("DeterministicPileup", False)

        # Optional arguments that default to something reasonable.
        self.blockBlacklist = arguments.get("BlockBlacklist", [])
        self.blockWhitelist = arguments.get("BlockWhitelist", [])
        self.runBlacklist = arguments.get("RunBlacklist", [])
        self.runWhitelist = arguments.get("RunWhitelist", [])
        self.emulation = arguments.get("Emulation", False)

        # These are mostly place holders because the job splitting algo and
        # parameters will be updated after the workflow has been created.
        self.procJobSplitAlgo = arguments.get("StdJobSplitAlgo", "LumiBased")
        self.procJobSplitArgs = arguments.get("StdJobSplitArgs",
                                              {"lumis_per_job": 8,
                                               "include_parents": self.includeParents})
        self.procJobSplitArgs.setdefault("deterministicPileup", self.deterministicPileup)
        return self.buildWorkload()

    def validateSchema(self, schema):
        """
        _validateSchema_

        Check for required fields, and some skim facts
        """
        requiredFields = ["CMSSWVersion", "ScramArch",
                          "GlobalTag", "InputDataset",
                          "StepOneConfigCacheID", "CouchURL",
                          "CouchDBName"]
        self.requireValidateFields(fields = requiredFields, schema = schema,
                                   validate = False)
        couchUrl = schema.get("ConfigCacheUrl", None) or schema["CouchURL"]
        outMod = self.validateConfigCacheExists(configID = schema["StepOneConfigCacheID"],
                                                couchURL = couchUrl,
                                                couchDBName = schema["CouchDBName"],
                                                getOutputModules = True)
        return

def reDigiWorkload(workloadName, arguments):
    """
    _reDigiWorkload_

    Instantiate the ReDigiWorkflowFactory and have it generate a workload for
    the given parameters.
    """
    myReDigiFactory = ReDigiWorkloadFactory()
    return myReDigiFactory(workloadName, arguments)
