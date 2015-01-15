#!/usr/bin/env python
"""
_ReDigi_

Standard two/three step redigi workflow.
"""
import WMCore.WMSpec.Steps.StepFactory as StepFactory

from WMCore.Lexicon import dataset
from WMCore.WMSpec.StdSpecs.DataProcessing import DataProcessing
from WMCore.WMSpec.WMWorkloadTools import strToBool, parsePileupConfig

class ReDigiWorkloadFactory(DataProcessing):
    """
    _ReDigiWorkloadFactory_

    Stamp out ReDigi workflows.
    """

    def addMergeTasks(self, parentTask, parentStepName, outputMods):
        """
        _addMergeTasks_

        Add merge and cleanup tasks for the output of a processing step.
        """
        mergeTasks = {}
        for outputModuleName in outputMods.keys():
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

        if self.stepTwoConfigCacheID is None:
            return

        stepOneMergeTask = stepOneMergeTasks[self.stepOneOutputModuleName]
        stepTwoMergeTasks = self.addDependentProcTask("StepTwoProc",
                                                      stepOneMergeTask,
                                                      self.stepTwoConfigCacheID,
                                                      timePerEvent = self.stepTwoTimePerEvent,
                                                      sizePerEvent = self.stepTwoSizePerEvent,
                                                      memoryReq = self.stepTwoMemory)

        if self.stepThreeConfigCacheID is None:
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

        if self.stepThreeConfigCacheID is None:
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
        workload.setDashboardActivity("reprocessing")
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

        if self.keepStepOneOutput and self.keepStepTwoOutput:
            self.setupDependentProcessing(stepOneTask, outputMods)
        elif not self.keepStepOneOutput and self.keepStepTwoOutput:
            self.setupChainedProcessing(stepOneTask)
        elif not self.keepStepOneOutput and not self.keepStepTwoOutput:
            self.setupThreeStepChainedProcessing(stepOneTask)
        else:
            # Steps one and two are dependent, step three is chained.
            # Not supported
            pass

        if self.pileupConfig:
            self.setupPileup(stepOneTask, self.pileupConfig)

        # setting the parameters which need to be set for all the tasks
        # sets acquisitionEra, processingVersion, processingString
        workload.setTaskPropertiesFromWorkload()

        # set the LFN bases (normally done by request manager)
        # also pass runNumber (workload evaluates it)
        workload.setLFNBase(self.mergedLFNBase, self.unmergedLFNBase,
                            runNumber = self.runNumber)

        return workload

    def __call__(self, workloadName, arguments):
        """
        _call_

        Create a ReDigi workload with the given parameters.
        """
        DataProcessing.__call__(self, workloadName, arguments)

        # Transform the pileup as required by the CMSSW step
        self.pileupConfig = parsePileupConfig(self.mcPileup, self.dataPileup)

        # Adjust the pileup splitting
        self.procJobSplitArgs.setdefault("deterministicPileup", self.deterministicPileup)

        # Adjust the sizePerEvent, timePerEvent and memory for step two and three
        if self.stepTwoTimePerEvent is None:
            self.stepTwoTimePerEvent = self.timePerEvent
        if self.stepTwoSizePerEvent is None:
            self.stepTwoSizePerEvent = self.sizePerEvent
        if self.stepTwoMemory is None:
            self.stepTwoMemory = self.memory
        if self.stepThreeTimePerEvent is None:
            self.stepThreeTimePerEvent = self.timePerEvent
        if self.stepThreeSizePerEvent is None:
            self.stepThreeSizePerEvent = self.sizePerEvent
        if self.stepThreeMemory is None:
            self.stepThreeMemory = self.memory


        return self.buildWorkload()

    @staticmethod
    def getWorkloadArguments():
        baseArgs = DataProcessing.getWorkloadArguments()
        specArgs = {"RequestType" : {"default" : "ReDigi", "optional" : True,
                                      "attr" : "requestType"},
                    "StepOneOutputModuleName" : {"default" : None, "type" : str,
                                                 "optional" : True, "validate" : None,
                                                 "attr" : "stepOneOutputModuleName", "null" : False},
                    "StepTwoOutputModuleName" : {"default" : None, "type" : str,
                                                 "optional" : True, "validate" : None,
                                                 "attr" : "stepTwoOutputModuleName", "null" : False},
                    "StepOneConfigCacheID" : {"default" : None, "type" : str,
                                              "optional" : False, "validate" : None,
                                              "attr" : "stepOneConfigCacheID", "null" : False},
                    "StepTwoConfigCacheID" : {"default" : None, "type" : str,
                                              "optional" : True, "validate" : None,
                                              "attr" : "stepTwoConfigCacheID", "null" : False},
                    "StepThreeConfigCacheID" : {"default" : None, "type" : str,
                                                "optional" : True, "validate" : None,
                                                "attr" : "stepThreeConfigCacheID", "null" : False},
                    "KeepStepOneOutput" : {"default" : True, "type" : strToBool,
                                           "optional" : True, "validate" : None,
                                           "attr" : "keepStepOneOutput", "null" : False},
                    "KeepStepTwoOutput" : {"default" : True, "type" : strToBool,
                                           "optional" : True, "validate" : None,
                                           "attr" : "keepStepTwoOutput", "null" : False},
                    "StepTwoTimePerEvent" : {"default" : None, "type" : float,
                                             "optional" : True, "validate" : lambda x : x > 0,
                                             "attr" : "stepTwoTimePerEvent", "null" : False},
                    "StepThreeTimePerEvent" : {"default" : None, "type" : float,
                                               "optional" : True, "validate" : lambda x : x > 0,
                                               "attr" : "stepThreeTimePerEvent", "null" : False},
                    "StepTwoSizePerEvent" : {"default" : None, "type" : float,
                                             "optional" : True, "validate" : lambda x : x > 0,
                                             "attr" : "stepTwoSizePerEvent", "null" : False},
                    "StepThreeSizePerEvent" : {"default" : None, "type" : float,
                                               "optional" : True, "validate" : lambda x : x > 0,
                                               "attr" : "stepThreeSizePerEvent", "null" : False},
                    "StepTwoMemory" : {"default" : None, "type" : float,
                                       "optional" : True, "validate" : lambda x : x > 0,
                                       "attr" : "stepTwoMemory", "null" : False},
                    "StepThreeMemory" : {"default" : None, "type" : float,
                                         "optional" : True, "validate" : lambda x : x > 0,
                                         "attr" : "stepThreeMemory", "null" : False},
                    "MCPileup" : {"default" : None, "type" : str,
                                  "optional" : True, "validate" : dataset,
                                  "attr" : "mcPileup", "null" : False},
                    "DataPileup" : {"default" : None, "type" : str,
                                    "optional" : True, "validate" : dataset,
                                    "attr" : "dataPileup", "null" : False},
                    "DeterministicPileup" : {"default" : False, "type" : strToBool,
                                             "optional" : True, "validate" : None,
                                             "attr" : "deterministicPileup", "null" : False}}
        baseArgs.update(specArgs)
        DataProcessing.setDefaultArgumentsProperty(baseArgs)
        return baseArgs

    def validateSchema(self, schema):
        couchUrl = schema.get("ConfigCacheUrl", None) or schema["CouchURL"]
        self.validateConfigCacheExists(configID = schema["StepOneConfigCacheID"],
                                       couchURL = couchUrl,
                                       couchDBName = schema["CouchDBName"])
        if schema.get("StepTwoConfigCacheID") is not None:
            self.validateConfigCacheExists(configID = schema["StepTwoConfigCacheID"],
                                           couchURL = couchUrl,
                                           couchDBName = schema["CouchDBName"])
            if schema.get("StepOneOutputModuleName") is None:
                self.raiseValidationException("StepTwoConfigCacheID is specified but StepOneOutputModuleName isn't")
        if schema.get("StepThreeConfigCacheID") is not None:
            self.validateConfigCacheExists(configID = schema["StepThreeConfigCacheID"],
                                           couchURL = couchUrl,
                                           couchDBName = schema["CouchDBName"])
            if schema.get("StepTwoOutputModuleName") is None:
                self.raiseValidationException("StepThreeConfigCacheID is specified but StepTwoOutputModuleName isn't")
        return

