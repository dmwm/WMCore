#!/usr/bin/env python
"""
_StepChain_

Creates a StepChain like workflow where there is only one production/processing
task and an arbitrary number of steps (cmsRun) inside this task, which are
chained based on their StepName, InputStep and InputFromOutputModule.

Each Step is formed as a dictionary providing only the basic information, see:
test/data/ReqMgr/requests/DMWM/StepChain_MC.json

It also assumes all the intermediate steps output are transient and do not need
to be staged out and registered in DBS/PhEDEx. Only the last step output will be
made available.
"""
from builtins import range
from builtins import str
from Utils.Utilities import strToBool
import WMCore.WMSpec.Steps.StepFactory as StepFactory
from WMCore.Lexicon import primdataset
from WMCore.WMSpec.StdSpecs.StdBase import StdBase
from WMCore.WMSpec.WMWorkloadTools import validateArgumentsCreate, parsePileupConfig

# simple utils for data mining the request dictionary
isGenerator = lambda args: not args["Step1"].get("InputDataset", None)


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
        self.configCacheUrl = None
        self.globalTag = None
        self.frameworkVersion = None
        self.scramArch = None
        self.couchDBName = None
        self.stepChain = None
        self.sizePerEvent = None
        self.timePerEvent = None
        self.primaryDataset = None
        self.prepID = None
        # stepMapping is going to be used during assignment for properly mapping
        # the arguments to each step/cmsRun
        self.stepMapping = {}

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

        self.inputPrimaryDataset = self.getStepValue('PrimaryDataset', taskConf, self.primaryDataset)
        self.blockBlacklist = taskConf["BlockBlacklist"]
        self.blockWhitelist = taskConf["BlockWhitelist"]
        self.runBlacklist = taskConf["RunBlacklist"]
        self.runWhitelist = taskConf["RunWhitelist"]
        self.splittingAlgo = taskConf['SplittingAlgo']

        # Create the first task
        firstTask = self.workload.newTask(taskConf['StepName'])

        # Create a proper task and set workload level arguments
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

        # Now modify this task to add the next steps
        if self.stepChain > 1:
            self.setupNextSteps(firstTask, arguments)

        self.workload.setStepMapping(self.stepMapping)
        self.reportWorkflowToDashboard(self.workload.getDashboardActivity())

        return self.workload

    def setupGeneratorTask(self, task, taskConf):
        """
        _setupGeneratorTask_

        Set up an initial generator task.
        """
        configCacheID = taskConf['ConfigCacheID']
        splitAlgorithm = taskConf["SplittingAlgo"]
        splitArguments = taskConf["SplittingArguments"]
        outMods = self.setupProcessingTask(task, "Production", couchDBName=self.couchDBName,
                                           configDoc=configCacheID, configCacheUrl=self.configCacheUrl,
                                           splitAlgo=splitAlgorithm, splitArgs=splitArguments,
                                           seeding=taskConf['Seeding'],
                                           totalEvents=taskConf['RequestNumEvents'],
                                           cmsswVersion=taskConf.get("CMSSWVersion", None),
                                           scramArch=taskConf.get("ScramArch", None),
                                           globalTag=taskConf.get("GlobalTag", None),
                                           taskConf=taskConf)

        # outputModules were added already, we just want to create merge tasks here
        if strToBool(taskConf.get('KeepOutput', True)):
            self.setupMergeTask(task, taskConf, "cmsRun1", outMods)

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

        outMods = self.setupProcessingTask(task, "Processing",
                                           inputDataset=self.inputDataset, couchDBName=self.couchDBName,
                                           configDoc=configCacheID, configCacheUrl=self.configCacheUrl,
                                           splitAlgo=splitAlgorithm, splitArgs=splitArguments,
                                           cmsswVersion=taskConf.get("CMSSWVersion", None),
                                           scramArch=taskConf.get("ScramArch", None),
                                           globalTag=taskConf.get("GlobalTag", None),
                                           taskConf=taskConf)

        lumiMask = taskConf.get("LumiList", self.workload.getLumiList())
        if lumiMask:
            task.setLumiMask(lumiMask)

        if taskConf["PileupConfig"]:
            self.setupPileup(task, taskConf['PileupConfig'])

        # outputModules were added already, we just want to create merge tasks here
        if strToBool(taskConf.get('KeepOutput', True)):
            self.setupMergeTask(task, taskConf, "cmsRun1", outMods)

        return

    def setupNextSteps(self, task, origArgs):
        """
        _setupNextSteps_

        Modify the step one task to include N more CMSSW steps and
        chain the output between all three steps.
        """
        self.stepMapping.setdefault(origArgs['Step1']['StepName'], ('Step1', 'cmsRun1'))

        for i in range(2, self.stepChain + 1):
            currentStepNumber = "Step%d" % i
            currentCmsRun = "cmsRun%d" % i
            self.stepMapping.setdefault(origArgs[currentStepNumber]['StepName'], (currentStepNumber, currentCmsRun))
            taskConf = {}
            for k, v in origArgs[currentStepNumber].iteritems():
                taskConf[k] = v

            parentStepNumber = self.stepMapping.get(taskConf['InputStep'])[0]
            parentCmsRun = self.stepMapping.get(taskConf['InputStep'])[1]
            parentCmsswStep = task.getStep(parentCmsRun)
            parentCmsswStepHelper = parentCmsswStep.getTypeHelper()

            # Set default values for the task parameters
            self.modifyTaskConfiguration(taskConf, False, 'InputDataset' not in taskConf)
            globalTag = self.getStepValue('GlobalTag', taskConf, self.globalTag)
            frameworkVersion = self.getStepValue('CMSSWVersion', taskConf, self.frameworkVersion)
            scramArch = self.getStepValue('ScramArch', taskConf, self.scramArch)

            childCmssw = parentCmsswStep.addTopStep(currentCmsRun)
            childCmssw.setStepType("CMSSW")
            template = StepFactory.getStepTemplate("CMSSW")
            template(childCmssw.data)

            childCmsswStepHelper = childCmssw.getTypeHelper()
            childCmsswStepHelper.setGlobalTag(globalTag)
            childCmsswStepHelper.setupChainedProcessing(parentCmsRun, taskConf['InputFromOutputModule'])
            childCmsswStepHelper.cmsswSetup(frameworkVersion, softwareEnvironment="", scramArch=scramArch)
            childCmsswStepHelper.setConfigCache(self.configCacheUrl, taskConf['ConfigCacheID'], self.couchDBName)

            # multicore settings
            multicore = self.multicore
            eventStreams = self.eventStreams
            if taskConf['Multicore'] > 0:
                multicore = taskConf['Multicore']
            if taskConf.get('EventStreams') >= 0:
                eventStreams = taskConf['EventStreams']

            childCmsswStepHelper.setNumberOfCores(multicore, eventStreams)

            # Pileup check
            taskConf["PileupConfig"] = parsePileupConfig(taskConf["MCPileup"], taskConf["DataPileup"])
            if taskConf["PileupConfig"]:
                self.setupPileup(task, taskConf['PileupConfig'])

            # Handling the output modules
            parentKeepOutput = strToBool(origArgs[parentStepNumber].get('KeepOutput', True))
            parentCmsswStepHelper.keepOutput(parentKeepOutput)
            childKeepOutput = strToBool(taskConf.get('KeepOutput', True))
            childCmsswStepHelper.keepOutput(childKeepOutput)
            self.setupOutputModules(task, taskConf, currentCmsRun, childKeepOutput)

        # Closing out the task configuration. The last step output must be saved/merged
        childCmsswStepHelper.keepOutput(True)

        return

    def getStepValue(self, keyName, stepDict, topLevelValue):
        """
        Utilitarian method to reliably get the value of a step key
        or fallback to the top level one.
        """
        if keyName in stepDict and stepDict.get(keyName) is not None:
            return stepDict.get(keyName)
        else:
            return topLevelValue

    def setupOutputModules(self, task, taskConf, stepCmsRun, keepOutput):
        """
        _setupOutputModules_

        Retrieves the outputModules from the step configuration and sets up
        a merge task for them. Only when KeepOutput is set to True.
        """
        taskConf = taskConf or {}

        outputMods = {}

        configOutput = self.determineOutputModules(configDoc=taskConf["ConfigCacheID"],
                                                   configCacheUrl=self.configCacheUrl,
                                                   couchDBName=self.couchDBName)
        for outputModuleName in configOutput.keys():
            outputModule = self.addOutputModule(task, outputModuleName,
                                                self.inputPrimaryDataset,
                                                configOutput[outputModuleName]["dataTier"],
                                                configOutput[outputModuleName]["filterName"],
                                                stepName=stepCmsRun, taskConf=taskConf)
            outputMods[outputModuleName] = outputModule

        if keepOutput:
            self.setupMergeTask(task, taskConf, stepCmsRun, outputMods)

        return

    def setupMergeTask(self, task, taskConf, stepCmsRun, outputMods):
        """
        _setupMergeTask_

        Adds a merge task to the parent with the proper task configuration.
        """
        frameworkVersion = taskConf.get("CMSSWVersion", self.frameworkVersion)
        scramArch = taskConf.get("ScramArch", self.scramArch)
        # PrepID has to be inherited from the workload level, not from task
        if not taskConf.get('PrepID'):
            taskConf['PrepID'] = self.prepID

        for outputModuleName in outputMods.keys():
            dummyTask = self.addMergeTask(task, self.splittingAlgo, outputModuleName, stepCmsRun,
                                          cmsswVersion=frameworkVersion, scramArch=scramArch,
                                          forceTaskName=taskConf.get('StepName'), taskConf=taskConf)

        return

    def modifyTaskConfiguration(self, taskConf, firstTask=False, generator=False):
        """
        _modifyTaskConfiguration_

        Modify the TaskConfiguration according to the specifications
        in getWorkloadCreateArgs and getChainCreateArgs. It does type
        casting and assigns default values.
        """
        baseArguments = self.getWorkloadCreateArgs()
        for argument in baseArguments:
            if argument in taskConf:
                taskConf[argument] = baseArguments[argument]["type"](taskConf[argument])

        taskArguments = self.getChainCreateArgs(firstTask, generator)
        for argument in taskArguments:
            if argument not in taskConf:
                taskConf[argument] = taskArguments[argument]["default"]
            else:
                taskConf[argument] = taskArguments[argument]["type"](taskConf[argument])

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
        if generator:
            requestNumEvts = int(taskConf.get("RequestNumEvents", 0))
            filterEff = taskConf.get("FilterEfficiency")
            # Adjust totalEvents according to the filter efficiency
            taskConf["SplittingAlgo"] = "EventBased"
            taskConf["RequestNumEvents"] = int(requestNumEvts / filterEff)
            taskConf["SizePerEvent"] = self.sizePerEvent * filterEff

        if taskConf["EventsPerJob"] is None:
            taskConf["EventsPerJob"] = int((8.0 * 3600.0) / self.timePerEvent)
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

        taskConf["SplittingArguments"].setdefault("deterministicPileup", self.deterministicPileup)

        return

    @staticmethod
    def getWorkloadCreateArgs():
        baseArgs = StdBase.getWorkloadCreateArgs()
        specArgs = {"RequestType": {"default": "StepChain", "optional": False},
                    "Step1": {"default": {}, "optional": False, "type": dict},
                    # ConfigCacheID is not used in the main dict for StepChain
                    "ConfigCacheID": {"optional": True, "null": True},
                    "DeterministicPileup": {"default": False, "type": strToBool, "optional": True, "null": False},
                    "PrimaryDataset": {"null": True, "validate": primdataset},
                    "StepChain": {"default": 1, "type": int, "null": False,
                                  "optional": False, "validate": lambda x: x > 0},
                    "FirstEvent": {"default": 1, "type": int, "validate": lambda x: x > 0,
                                   "null": False},
                    "FirstLumi": {"default": 1, "type": int, "validate": lambda x: x > 0,
                                  "null": False}
                   }
        baseArgs.update(specArgs)
        StdBase.setDefaultArgumentsProperty(baseArgs)
        return baseArgs

    @staticmethod
    def getChainCreateArgs(firstTask=False, generator=False):
        """
        _getChainCreateArgs_

        This represents the authoritative list of request arguments that are
        allowed in each chain (Step/Task) of chained request, during request creation.
        Additional especific arguments must be defined inside each spec class.

        For more information on how these arguments are built, please have a look
        at the docstring for getWorkloadCreateArgs.
        """
        baseArgs = StdBase.getChainCreateArgs(firstTask, generator)
        arguments = {
            'InputStep': {'default': None, 'null': False, 'optional': firstTask, 'type': str},
            'StepName': {'null': False, 'optional': False},
            'PrimaryDataset': {'default': None, 'optional': True,
                               'validate': primdataset, 'null': False}
            }

        baseArgs.update(arguments)
        StdBase.setDefaultArgumentsProperty(baseArgs)

        return baseArgs

    def validateSchema(self, schema):
        """
        _validateSchema_

        Settings that are not supported and will cause workflow injection to fail, are:
         * output from the last step *must* be saved
         * each step configuration must be a dictionary
         * StepChain argument must reflect the number of Steps in the request
         * usual Step arguments validation, as defined in the spec
         * usual ConfigCacheID validation
         * trident configuration, where 2 steps have the same output module AND datatier
        """
        outputModTier = []
        lastStep = "Step%s" % schema['StepChain']
        if not strToBool(schema[lastStep].get('KeepOutput', True)):
            msg = "Dropping the output of the last step is prohibited.\n"
            msg += "Set the 'KeepOutput' value to True and try again."
            self.raiseValidationException(msg=msg)

        for i in range(1, schema['StepChain'] + 1):
            stepNumber = "Step%s" % i
            if stepNumber not in schema:
                msg = "No Step%s entry present in the request" % i
                self.raiseValidationException(msg=msg)

            step = schema[stepNumber]
            # We can't handle non-dictionary steps
            if not isinstance(step, dict):
                msg = "Non-dictionary input for step in StepChain.\n"
                msg += "Could be an indicator of JSON error.\n"
                self.raiseValidationException(msg=msg)

            # Generic step parameter validation
            self.validateStep(step, self.getChainCreateArgs(i == 1, i == 1 and 'InputDataset' not in step))

            # Validate the existence of the configCache
            if step["ConfigCacheID"]:
                self.validateConfigCacheExists(configID=step['ConfigCacheID'],
                                               configCacheUrl=schema['ConfigCacheUrl'],
                                               couchDBName=schema["CouchDBName"],
                                               getOutputModules=False)
            # we cannot save output of two steps using the same output module and datatier(s)
            if strToBool(step.get("KeepOutput", True)):
                configOutput = self.determineOutputModules(configDoc=step["ConfigCacheID"],
                                                           configCacheUrl=schema['ConfigCacheUrl'],
                                                           couchDBName=schema["CouchDBName"])
                for modName, values in configOutput.items():
                    thisOutput = (modName, values['dataTier'])
                    if thisOutput in outputModTier:
                        msg = "StepChain cannot save output of different steps using "
                        msg += "the same output module AND datatier(s)."
                        msg += "\n%s re-using outputModule: %s and datatier: %s" % (stepNumber,
                                                                                    modName,
                                                                                    values['dataTier'])
                        self.raiseValidationException(msg=msg)
                    outputModTier.append(thisOutput)
        return

    def validateStep(self, taskConf, taskArgumentDefinition):
        """
        _validateStep_

        Validate the step information against the given
        argument description
        """
        try:
            validateArgumentsCreate(taskConf, taskArgumentDefinition, checkInputDset=False)
        except Exception as ex:
            self.raiseValidationException(str(ex))

        return
