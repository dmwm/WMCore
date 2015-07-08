#!/usr/bin/env python
"""
_StepChain_

Creates a StepChain like workflow where there is only one production/processing
task and several cmsRun steps inside.
"""

import WMCore.WMSpec.Steps.StepFactory as StepFactory
from WMCore.Lexicon import identifier, couchurl, block, primdataset, dataset
from WMCore.WMSpec.StdSpecs.StdBase import StdBase
from WMCore.WMSpec.WMWorkloadTools import makeList, strToBool,\
     validateArgumentsCreate, validateArgumentsNoOptionalCheck, parsePileupConfig

#
# simple utils for data mining the request dictionary
#
isGenerator = lambda args: not args["Step1"].get("InputDataset", None)
parentTaskModule = lambda args: args.get("InputFromOutputModule", None)


class StepChainWorkloadFactory(StdBase):
    """
    __StepChainWorkloadFactory__

    Factory for StepChain workflows.
    """

    def __init__(self):
        """
        __init__

        Setup parameters that will be later overwritten in the call,
        otherwise pylint will complain about them.
        """
        StdBase.__init__(self)
        self.couchURL = None
        self.couchDBName = None
        self.configCacheUrl = None
        self.globalTag = None
        self.frameworkVersion = None
        self.scramArch = None
        self.couchDBName = None
        self.stepChain = None
        self.sizePerEvent = None
        self.timePerEvent = None

    def __call__(self, workloadName, arguments):
        """
        __call__

        Create a StepChain workload with the given parameters.
        Configures the workload based on the first task information,
        then properly sets up the remaining tasks.
        """
        StdBase.__call__(self, workloadName, arguments)
        self.workload = self.createWorkload()

        # Update the task configuration
        taskConf = {}
        for k, v in arguments["Step1"].iteritems():
            taskConf[k] = v
        self.modifyTaskConfiguration(taskConf, True, 'InputDataset' not in taskConf)
        if taskConf['Multicore'] and taskConf['Multicore'] != 'None':
            self.multicoreNCores = int(taskConf['Multicore'])
        self.inputPrimaryDataset = taskConf['PrimaryDataset']
        self.blockBlacklist = taskConf["BlockBlacklist"]
        self.blockWhitelist = taskConf["BlockWhitelist"]
        self.runBlacklist = taskConf["RunBlacklist"]
        self.runWhitelist = taskConf["RunWhitelist"]
        self.splittingAlgo = taskConf['SplittingAlgo']

        # Create the first task
        firstTask = self.workload.newTask(taskConf['StepName'])

        # Set workload level arguments and set task arguments
        if isGenerator(arguments):
            self.workload.setDashboardActivity("production")
            self.workload.setWorkQueueSplitPolicy("MonteCarlo", taskConf['SplittingAlgo'],
                                                  taskConf['SplittingArguments'])
            self.workload.setEndPolicy("SingleShot")
            self.setupGeneratorTask(firstTask, taskConf)
        else:
            self.workload.setDashboardActivity("processing")
            self.workload.setWorkQueueSplitPolicy("Block", taskConf['SplittingAlgo'],
                                                  taskConf['SplittingArguments'])
            self.setupTask(firstTask, taskConf)
        self.reportWorkflowToDashboard(self.workload.getDashboardActivity())

        # Now modify this task to add the next steps
        self.setupNextSteps(firstTask, arguments)

        return self.workload

    def setupGeneratorTask(self, task, taskConf):
        """
        _setupGeneratorTask_

        Set up an initial generator task.
        """
        configCacheID = taskConf['ConfigCacheID']
        splitAlgorithm = taskConf["SplittingAlgo"]
        splitArguments = taskConf["SplittingArguments"]
        self.setupProcessingTask(task, "Production",
                                 couchURL=self.couchURL, couchDBName=self.couchDBName,
                                 configDoc=configCacheID, splitAlgo=splitAlgorithm,
                                 configCacheUrl=self.configCacheUrl,
                                 splitArgs=splitArguments, seeding=taskConf['Seeding'],
                                 totalEvents=taskConf['RequestNumEvents'],
                                 timePerEvent=taskConf.get('TimePerEvent', None),
                                 memoryReq=taskConf.get('Memory', None),
                                 sizePerEvent=taskConf.get('SizePerEvent', None),
                                 taskConf=taskConf)

        self.addLogCollectTask(task, 'LogCollectFor%s' % task.name())

        return

    def setupTask(self, task, taskConf):
        """
        _setupTask_

        Build the task using the setupProcessingTask from StdBase
        and set the parents appropriately to handle a processing task
        """
        configCacheID = taskConf["ConfigCacheID"]
        splitAlgorithm = taskConf["SplittingAlgo"]
        splitArguments = taskConf["SplittingArguments"]
        self.inputDataset = taskConf["InputDataset"]
        # Use PD from the inputDataset if not provided in the task itself
        if not self.inputPrimaryDataset:
            self.inputPrimaryDataset = self.inputDataset[1:].split("/")[0]
        inpStep, inpMod = None, None

        self.setupProcessingTask(task, "Processing",
                                 inputDataset=self.inputDataset,
                                 inputStep=inpStep, inputModule=inpMod,
                                 couchURL=self.couchURL, couchDBName=self.couchDBName,
                                 configDoc=configCacheID, splitAlgo=splitAlgorithm,
                                 configCacheUrl=self.configCacheUrl,
                                 splitArgs=splitArguments,
                                 timePerEvent=taskConf.get('TimePerEvent', None),
                                 memoryReq=taskConf.get('Memory', None),
                                 sizePerEvent=taskConf.get('SizePerEvent', None),
                                 taskConf=taskConf)

        lumiMask = taskConf.get("LumiList", self.workload.lumiList)
        if lumiMask:
            task.setLumiMask(lumiMask)

        if taskConf["PileupConfig"]:
            self.setupPileup(task, taskConf['PileupConfig'])

        self.addLogCollectTask(task, 'LogCollectFor%s' % task.name())

        return

    def setupNextSteps(self, task, origArgs):
        """
        _setupNextSteps_

        Modify the step one task to include N more CMSSW steps and
        chain the output between all three steps.
        """
        configCacheUrl = self.configCacheUrl or self.couchURL

        for i in range(2, self.stepChain + 1):
            inputStepName = "cmsRun%d" % (i-1)
            parentCmsswStep = task.getStep(inputStepName)
            parentCmsswStepHelper = parentCmsswStep.getTypeHelper()
            parentCmsswStepHelper.keepOutput(False)

            currentStepName = "cmsRun%d" % i
            taskConf = {}
            for k, v in origArgs["Step%d" % i].iteritems():
                taskConf[k] = v
            # Set default values to task parameters
            self.modifyTaskConfiguration(taskConf, False, 'InputDataset' not in taskConf)
            globalTag = taskConf.get("GlobalTag", self.globalTag)

            childCmssw = parentCmsswStep.addTopStep(currentStepName)
            childCmssw.setStepType("CMSSW")
            template = StepFactory.getStepTemplate("CMSSW")
            template(childCmssw.data)

            childCmsswHelper = childCmssw.getTypeHelper()
            childCmsswHelper.setGlobalTag(globalTag)
            childCmsswHelper.setupChainedProcessing(inputStepName, taskConf['InputFromOutputModule'])
            # Assuming we cannot change the CMSSW version inside the same job
            childCmsswHelper.cmsswSetup(self.frameworkVersion, softwareEnvironment="",
                                        scramArch=self.scramArch)
            childCmsswHelper.setConfigCache(configCacheUrl, taskConf['ConfigCacheID'],
                                            self.couchDBName)
            childCmsswHelper.keepOutput(False)

            # Pileup check
            taskConf["PileupConfig"] = parsePileupConfig(taskConf["MCPileup"], taskConf["DataPileup"])
            if taskConf["PileupConfig"]:
                self.setupPileup(task, taskConf['PileupConfig'])

            # Handling the output modules
            outputMods = {}
            configOutput = self.determineOutputModules(configDoc=taskConf['ConfigCacheID'],
                                                       couchURL=configCacheUrl,
                                                       couchDBName=self.couchDBName)
            for outputModuleName in configOutput.keys():
                outputModule = self.addOutputModule(task, outputModuleName,
                                                    self.inputPrimaryDataset,
                                                    configOutput[outputModuleName]["dataTier"],
                                                    configOutput[outputModuleName]["filterName"],
                                                    stepName=currentStepName)
                outputMods[outputModuleName] = outputModule

        # Closing out the task configuration
        # Only the last step output is important :-)
        childCmsswHelper.keepOutput(True)
        self.addMergeTasks(task, currentStepName, outputMods)

        # Override task parameters by the workload ones in case of their absence
        self.updateCommonParams(task, taskConf)
        return

    def addMergeTasks(self, parentTask, parentStepName, outputMods):
        """
        _addMergeTasks_

        Add merge, logCollect and cleanup tasks for the output modules.
        """
        mergeTasks = {}
        for outputModuleName in outputMods.keys():
            mergeTask = self.addMergeTask(parentTask, self.splittingAlgo,
                                          outputModuleName, parentStepName)
            mergeTasks[outputModuleName] = mergeTask

        return mergeTasks

    def updateCommonParams(self, task, taskConf):
        """
        _updateCommonParams_

        Update some parameters at a task level, using either the
        values set for the last Task or the workload level one.
        """
        task.setPrepID(taskConf.get("PrepID", self.workload.getPrepID()))
        task.setAcquisitionEra(taskConf.get("AcquisitionEra", self.workload.acquisitionEra))
        task.setProcessingString(taskConf.get("ProcessingString", self.workload.processingString))
        task.setProcessingVersion(taskConf.get("ProcessingVersion", self.workload.processingVersion))

    def modifyTaskConfiguration(self, taskConf, firstTask=False, generator=False):
        """
        _modifyTaskConfiguration_

        Modify the TaskConfiguration according to the specifications
        in getWorkloadArguments and getTaskArguments. It does type
        casting and assigns default values.
        """
        taskArguments = self.getTaskArguments(firstTask, generator)
        for argument in taskArguments:
            if argument not in taskConf:
                taskConf[argument] = taskArguments[argument]["default"]
            else:
                taskConf[argument] = taskArguments[argument]["type"](taskConf[argument])
        baseArguments = self.getWorkloadArguments()
        for argument in baseArguments:
            if argument in taskConf:
                taskConf[argument] = baseArguments[argument]["type"](taskConf[argument])

        taskConf["PileupConfig"] = parsePileupConfig(taskConf["MCPileup"], taskConf["DataPileup"])

        if firstTask:
            self.modifyJobSplitting(taskConf, generator)
        return

    def modifyJobSplitting(self, taskConf, generator):
        """
        _modifyJobSplitting_

        Adapt job splitting according to the first step configuration
        or lack of some of them.
        """
        sizePerEvt = taskConf.get("SizePerEvent", self.sizePerEvent)
        timePerEvt = taskConf.get("TimePerEvent", self.timePerEvent)
        if generator:
            requestNumEvts = int(taskConf.get("RequestNumEvents", 0))
            filterEff = taskConf.get("FilterEfficiency")
            # Adjust totalEvents according to the filter efficiency
            taskConf["SplittingAlgo"] = "EventBased"
            taskConf["RequestNumEvents"] = int(requestNumEvts / filterEff)
            taskConf["SizePerEvent"] = sizePerEvt * filterEff

        if taskConf["EventsPerJob"] is None:
            # TODO: the right thing is to consider TpE of all steps
            taskConf["EventsPerJob"] = int((8.0 * 3600.0) / timePerEvt)
        if taskConf["EventsPerLumi"] is None:
            taskConf["EventsPerLumi"] = taskConf["EventsPerJob"]

        taskConf["SplittingArguments"] = {}
        if taskConf["SplittingAlgo"] in ["EventBased", "EventAwareLumiBased"]:
            taskConf["SplittingArguments"]["events_per_job"] = taskConf["EventsPerJob"]
            if taskConf["SplittingAlgo"] == "EventAwareLumiBased":
                taskConf["SplittingArguments"]["max_events_per_lumi"] = 20000
            else:
                taskConf["SplittingArguments"]["events_per_lumi"] = taskConf["EventsPerLumi"]
            taskConf["SplittingArguments"]["lheInputFiles"] = taskConf["LheInputFiles"]
        elif taskConf["SplittingAlgo"] == "LumiBased":
            taskConf["SplittingArguments"]["lumis_per_job"] = taskConf["LumisPerJob"]
        elif taskConf["SplittingAlgo"] == "FileBased":
            taskConf["SplittingArguments"]["files_per_job"] = taskConf["FilesPerJob"]

        return

    @staticmethod
    def getWorkloadArguments():
        baseArgs = StdBase.getWorkloadArguments()
        specArgs = {"RequestType" : {"default" : "StepChain", "optional" : False},
                    "GlobalTag" : {"type" : str, "optional" : False},
                    "CouchURL" : {"type" : str, "optional" : False, "validate" : couchurl},
                    "CouchDBName" : {"type" : str, "optional" : False,
                                     "validate" : identifier},
                    "ConfigCacheUrl" : {"type" : str, "optional" : True, "null" : True},
                    "StepChain" : {"default" : 1, "type" : int,
                                   "optional" : False, "validate" : lambda x: x > 0,
                                   "attr" : "stepChain", "null" : False},
                    "FirstEvent" : {"default" : 1, "type" : int,
                                    "optional" : True, "validate" : lambda x: x > 0,
                                    "attr" : "firstEvent", "null" : False},
                    "FirstLumi" : {"default" : 1, "type" : int,
                                   "optional" : True, "validate" : lambda x: x > 0,
                                   "attr" : "firstLumi", "null" : False},
                   }
        baseArgs.update(specArgs)
        StdBase.setDefaultArgumentsProperty(baseArgs)
        return baseArgs

    @staticmethod
    def getTaskArguments(firstTask=False, generator=False):
        """
        _getTaskArguments_

        Each task dictionary specifies its own set of arguments
        that need to be validated as well, most of them are already
        defined in StdBase.getWorkloadArguments and those do not appear here
        since they are all optional. Here only new arguments are listed.
        """
        specArgs = {"StepName" : {"default" : None, "type" : str,
                                  "optional" : False, "validate" : None,
                                  "null" : False},
                    "ConfigCacheID" : {"default" : None, "type" : str,
                                       "optional" : False, "validate" : None,
                                       "null" : False},
                    "KeepOutput" : {"default" : True, "type" : strToBool,
                                    "optional" : True, "validate" : None,
                                    "null" : False},
                    "TransientOutputModules" : {"default" : [], "type" : makeList,
                                                "optional" : True, "validate" : None,
                                                "null" : False},
                    "PrimaryDataset" : {"default" : None, "type" : str,
                                        "optional" : not generator, "validate" : primdataset,
                                        "null" : False},
                    "Seeding" : {"default" : "AutomaticSeeding", "type" : str, "optional" : True,
                                 "validate" : lambda x: x in ["ReproducibleSeeding", "AutomaticSeeding"],
                                 "null" : False},
                    "RequestNumEvents" : {"default" : 1000, "type" : int,
                                          "optional" : not generator, "validate" : lambda x: x > 0,
                                          "null" : False},
                    "MCPileup" : {"default" : None, "type" : str,
                                  "optional" : True, "validate" : dataset,
                                  "null" : False},
                    "DataPileup" : {"default" : None, "type" : str,
                                    "optional" : True, "validate" : dataset,
                                    "null" : False},
                    "InputDataset" : {"default" : None, "type" : str,
                                      "optional" : generator or not firstTask, "validate" : dataset,
                                      "null" : False},
                    "InputStep" : {"default" : None, "type" : str,
                                   "optional" : firstTask, "validate" : None,
                                   "null" : False},
                    "InputFromOutputModule" : {"default" : None, "type" : str,
                                               "optional" : firstTask, "validate" : None,
                                               "null" : False},
                    "BlockBlacklist" : {"default" : [], "type" : makeList,
                                        "optional" : True, "validate" : lambda x: all([block(y) for y in x]),
                                        "null" : False},
                    "BlockWhitelist" : {"default" : [], "type" : makeList,
                                        "optional" : True, "validate" : lambda x: all([block(y) for y in x]),
                                        "null" : False},
                    "RunBlacklist" : {"default" : [], "type" : makeList,
                                      "optional" : True, "validate" : lambda x: all([int(y) > 0 for y in x]),
                                      "null" : False},
                    "RunWhitelist" : {"default" : [], "type" : makeList,
                                      "optional" : True, "validate" : lambda x: all([int(y) > 0 for y in x]),
                                      "null" : False},
                    "SplittingAlgo" : {"default" : "EventAwareLumiBased", "type" : str,
                                       "optional" : True, "validate" : lambda x: x in ["EventBased", "LumiBased",
                                                                                       "EventAwareLumiBased", "FileBased"],
                                       "null" : False},
                    "EventsPerJob" : {"default" : None, "type" : int,
                                      "optional" : True, "validate" : lambda x: x > 0,
                                      "null" : False},
                    "LumisPerJob" : {"default" : 8, "type" : int,
                                     "optional" : True, "validate" : lambda x: x > 0,
                                     "null" : False},
                    "FilesPerJob" : {"default" : 1, "type" : int,
                                     "optional" : True, "validate" : lambda x: x > 0,
                                     "null" : False},
                    "EventsPerLumi" : {"default" : None, "type" : int,
                                       "optional" : True, "validate" : lambda x: x > 0,
                                       "attr" : "eventsPerLumi", "null" : True},
                    "FilterEfficiency" : {"default" : 1.0, "type" : float,
                                          "optional" : True, "validate" : lambda x: x > 0.0,
                                          "attr" : "filterEfficiency", "null" : False},
                    "LheInputFiles" : {"default" : False, "type" : strToBool,
                                       "optional" : True, "validate" : None,
                                       "attr" : "lheInputFiles", "null" : False},
                    "PrepID": {"default" : None, "type": str,
                               "optional" : True, "validate" : None,
                               "attr" : "prepID", "null" : True},
                    "Multicore" : {"default" : None, "type" : int,
                                   "optional" : True, "validate" : lambda x: x > 0,
                                   "null" : False},
                   }
        StdBase.setDefaultArgumentsProperty(specArgs)
        return specArgs

    def validateSchema(self, schema):
        """
        _validateSchema_

        Go over each task and make sure it matches validation
        parameters derived from Dave's requirements.
        """
        numSteps = schema['StepChain']
        couchUrl = schema.get("ConfigCacheUrl", None) or schema["CouchURL"]
        for i in range(1, numSteps + 1):
            stepName = "Step%s" % i
            if stepName not in schema:
                msg = "No Step%s entry present in request" % i
                self.raiseValidationException(msg=msg)

            task = schema[stepName]
            # We can't handle non-dictionary tasks
            if type(task) != dict:
                msg = "Non-dictionary input for task in StepChain.\n"
                msg += "Could be an indicator of JSON error.\n"
                self.raiseValidationException(msg=msg)

            # Generic task parameter validation
            self.validateTask(task, self.getTaskArguments(i == 1, i == 1 and 'InputDataset' not in task))

            # Validate the existence of the configCache
            if task["ConfigCacheID"]:
                self.validateConfigCacheExists(configID=task['ConfigCacheID'],
                                               couchURL=couchUrl,
                                               couchDBName=schema["CouchDBName"],
                                               getOutputModules=True)

    def validateTask(self, taskConf, taskArgumentDefinition):
        """
        _validateTask_

        Validate the task information against the given
        argument description
        """
        msg = validateArgumentsCreate(taskConf, taskArgumentDefinition)
        if msg is not None:
            self.raiseValidationException(msg)

        # Also retrieve the "main" arguments which may be overriden in the task
        # Change them all to optional for validation
        baseArgs = self.getWorkloadArguments()
        validateArgumentsNoOptionalCheck(taskConf, baseArgs)

        for arg in baseArgs:
            baseArgs[arg]["optional"] = True
        msg = validateArgumentsCreate(taskConf, baseArgs)
        if msg is not None:
            self.raiseValidationException(msg)
        return

