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
from __future__ import division
from future.utils import viewitems
from builtins import range

from Utils.Utilities import strToBool
import WMCore.WMSpec.Steps.StepFactory as StepFactory
from WMCore.Lexicon import primdataset, taskStepName
from WMCore.WMSpec.StdSpecs.StdBase import StdBase
from WMCore.WMSpec.WMWorkloadTools import (validateArgumentsCreate, parsePileupConfig,
                                           checkMemCore, checkEventStreams, checkTimePerEvent)
from WMCore.WMSpec.WMSpecErrors import WMSpecFactoryException


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
        self.eventsPerJob = None
        self.eventsPerLumi = None
        # stepMapping is going to be used during assignment for properly mapping
        # the arguments to each step/cmsRun
        self.stepMapping = {}
        self.stepParentageMapping = {}

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
        for k, v in viewitems(arguments["Step1"]):
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

        # it has to be called before the other steps are created
        self.createStepMappings(arguments)

        # Create a proper task and set workload level arguments
        startPolicy = self.decideWorkQueueStartPolicy(arguments)
        self.workload.setWorkQueueSplitPolicy(startPolicy, taskConf['SplittingAlgo'],
                                              taskConf['SplittingArguments'],
                                              OpenRunningTimeout=self.openRunningTimeout)
        if startPolicy == "MonteCarlo":
            self.workload.setDashboardActivity("production")
            self.workload.setEndPolicy("SingleShot")
            self.setupGeneratorTask(firstTask, taskConf)
        else:
            self.workload.setDashboardActivity("processing")
            self.setupTask(firstTask, taskConf)

        # Now modify this task to add the next steps
        if self.stepChain > 1:
            self.setupNextSteps(firstTask, arguments)

        self.createStepParentageMappings(firstTask, arguments)

        self.workload.setStepMapping(self.stepMapping)
        self.workload.setStepParentageMapping(self.stepParentageMapping)
        # and push the parentage map to the reqmgr2 workload cache doc
        arguments['ChainParentageMap'] = self.workload.getChainParentageSimpleMapping()

        # Feed values back to save in couch
        if self.eventsPerJob:
            arguments['Step1']['EventsPerJob'] = self.eventsPerJob
        if self.eventsPerLumi:
            arguments['Step1']['EventsPerLumi'] = self.eventsPerLumi
        return self.workload

    def createStepMappings(self, origArgs):
        """
        _createStepMappings_

        Create a simple map of StepName to Step and cmsRun number.
        cmsRun numbers are sequential, just like the step number.
        :param origArgs: arguments provided by the user + default spec args
        :return: update a dictionary in place which is latter used to set a
         `stepMapping` property in the workload object
        """
        for i in range(1, self.stepChain + 1):
            stepNumber = "Step%d" % i
            stepName = origArgs[stepNumber]['StepName']
            cmsRunNumber = "cmsRun%d" % i
            self.stepMapping.setdefault(stepName, (stepNumber, cmsRunNumber))

    def createStepParentageMappings(self, firstTaskO, origArgs):
        """
        _createStepParentageMappings_

        Create a dict struct with a mapping of step name to parent step. It
        also includes a map of output datasets and parent dataset.
        :param firstTaskO: a WMTask object with the top level StepChain task
        :param origArgs: arguments provided by the user + default spec args
        :return: update a dictionary in place which will be later set as a
        WMWorkload property
        """
        for i in range(1, self.stepChain + 1):
            stepNumber = "Step%d" % i
            stepName = origArgs[stepNumber]['StepName']
            cmsRunNumber = "cmsRun%d" % i

            self.stepParentageMapping.setdefault(stepName, {})
            self.stepParentageMapping[stepName] = {'StepNumber': stepNumber,
                                                   'StepCmsRun': cmsRunNumber,
                                                   'ParentStepName': None,
                                                   'ParentStepNumber': None,
                                                   'ParentStepCmsRun': None,
                                                   'ParentDataset': None,
                                                   'OutputDatasetMap': {}}

            if stepNumber == 'Step1':
                self.stepParentageMapping[stepName]['ParentDataset'] = origArgs[stepNumber].get('InputDataset')

            # set the OutputDatasetMap or empty if KeepOutput is False
            if origArgs[stepNumber].get("KeepOutput", True):
                stepHelper = firstTaskO.getStepHelper(cmsRunNumber)
                for outputModuleName in stepHelper.listOutputModules():
                    outputModule = stepHelper.getOutputModule(outputModuleName)
                    outputDataset = "/%s/%s/%s" % (outputModule.primaryDataset,
                                                   outputModule.processedDataset,
                                                   outputModule.dataTier)
                    self.stepParentageMapping[stepName]['OutputDatasetMap'][outputModuleName] = outputDataset

            if "InputStep" in origArgs[stepNumber]:
                parentStepName = origArgs[stepNumber]["InputStep"]
                self.stepParentageMapping[stepName]['ParentStepName'] = parentStepName
                parentStepNumber = self.stepParentageMapping[parentStepName]['StepNumber']
                self.stepParentageMapping[stepName]['ParentStepNumber'] = parentStepNumber
                parentStepCmsRun = self.stepParentageMapping[parentStepName]['StepCmsRun']
                self.stepParentageMapping[stepName]['ParentStepCmsRun'] = parentStepCmsRun

                parentOutputModName = origArgs[stepNumber]["InputFromOutputModule"]
                parentDset = self.findParentStepWithOutputDataset(origArgs, parentStepNumber, parentStepName, parentOutputModName)
                self.stepParentageMapping[stepName]['ParentDataset'] = parentDset

    def findParentStepWithOutputDataset(self, origArgs, stepNumber, stepName, outModName):
        """
        _findParentStepWithOutputDataset_
        Given the parent step name and output module name, finds the parent dataset 
        :param origArgs: request arguments
        :param stepNumber: step number of the parent step
        :param stepName: step name of the parent step
        :param outModName: output module name of the parent step
        :return: the parent dataset name (str), otherwise None
        """
        if origArgs[stepNumber].get("KeepOutput", True):
            return self.stepParentageMapping[stepName]['OutputDatasetMap'][outModName]
        else:
            # then fetch grand-parent data
            parentStepNumber = self.stepParentageMapping[stepName]['ParentStepNumber']
            parentStepName = self.stepParentageMapping[stepName]['ParentStepName']
            if parentStepNumber:
                parentOutputModName = origArgs[stepNumber]["InputFromOutputModule"]
                return self.findParentStepWithOutputDataset(origArgs, parentStepNumber, parentStepName, parentOutputModName)
            else:
                # this is Step1, return the InputDataset if any
                return origArgs[stepNumber].get("InputDataset")

    def setupGeneratorTask(self, task, taskConf):
        """
        _setupGeneratorTask_

        Set up an initial generator task for the top level step (cmsRun1)
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

        if taskConf["PileupConfig"]:
            self.setupPileup(task, taskConf['PileupConfig'], stepName="cmsRun1")

        # outputModules were added already, we just want to create merge tasks here
        if strToBool(taskConf.get('KeepOutput', True)):
            self.setupMergeTask(task, taskConf, "cmsRun1", outMods)

        return

    def setupTask(self, task, taskConf):
        """
        _setupTask_

        Build the task using the setupProcessingTask from StdBase
        and set the parents appropriately to handle a processing task,
        It's only called for the top level task and top level step (cmsRun1)
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
            self.setupPileup(task, taskConf['PileupConfig'], stepName="cmsRun1")

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
        self.stepParentageMapping.setdefault(origArgs['Step1']['StepName'], {})

        for i in range(2, self.stepChain + 1):
            currentStepNumber = "Step%d" % i
            currentCmsRun = "cmsRun%d" % i
            taskConf = {}
            for k, v in viewitems(origArgs[currentStepNumber]):
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
            prepId = self.getStepValue('PrepID', taskConf, self.prepID)

            currentCmssw = parentCmsswStep.addTopStep(currentCmsRun)
            currentCmssw.setStepType("CMSSW")
            template = StepFactory.getStepTemplate("CMSSW")
            template(currentCmssw.data)

            currentCmsswStepHelper = currentCmssw.getTypeHelper()
            currentCmsswStepHelper.setPrepId(prepId)
            currentCmsswStepHelper.setGlobalTag(globalTag)
            currentCmsswStepHelper.setupChainedProcessing(parentCmsRun, taskConf['InputFromOutputModule'])
            currentCmsswStepHelper.cmsswSetup(frameworkVersion, softwareEnvironment="", scramArch=scramArch)
            currentCmsswStepHelper.setConfigCache(self.configCacheUrl, taskConf['ConfigCacheID'], self.couchDBName)

            # multicore settings
            multicore = self.multicore
            eventStreams = self.eventStreams
            if taskConf['Multicore'] > 0:
                multicore = taskConf['Multicore']
            if taskConf.get("EventStreams") is not None and taskConf['EventStreams'] >= 0:
                eventStreams = taskConf['EventStreams']

            currentCmsswStepHelper.setNumberOfCores(multicore, eventStreams)

            # Pileup check
            taskConf["PileupConfig"] = parsePileupConfig(taskConf["MCPileup"], taskConf["DataPileup"])
            if taskConf["PileupConfig"]:
                self.setupPileup(task, taskConf['PileupConfig'], stepName=currentCmsRun)

            # Handling the output modules in order to decide whether we should
            # stage them out and report them in the Report.pkl file
            parentKeepOutput = strToBool(origArgs[parentStepNumber].get('KeepOutput', True))
            parentCmsswStepHelper.keepOutput(parentKeepOutput)
            childKeepOutput = strToBool(taskConf.get('KeepOutput', True))
            currentCmsswStepHelper.keepOutput(childKeepOutput)
            self.setupOutputModules(task, taskConf, currentCmsRun, childKeepOutput)

        # Closing out the task configuration. The last step output must be saved/merged
        currentCmsswStepHelper.keepOutput(True)

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
        for outputModuleName in configOutput:
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

        for outputModuleName in outputMods:
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

        taskConf["SplittingArguments"] = {}
        if taskConf["SplittingAlgo"] in ["EventBased", "EventAwareLumiBased"]:
            taskConf["EventsPerJob"], taskConf["EventsPerLumi"] = StdBase.calcEvtsPerJobLumi(taskConf.get("EventsPerJob"),
                                                                                             taskConf.get("EventsPerLumi"),
                                                                                             self.timePerEvent,
                                                                                             taskConf.get("RequestNumEvents"))
            self.eventsPerJob = taskConf["EventsPerJob"]
            self.eventsPerLumi = taskConf["EventsPerLumi"]
            taskConf["SplittingArguments"]["events_per_job"] = taskConf["EventsPerJob"]
            if taskConf["SplittingAlgo"] == "EventBased":
                taskConf["SplittingArguments"]["events_per_lumi"] = taskConf["EventsPerLumi"]
            else:
                taskConf["SplittingArguments"]["job_time_limit"] = 48 * 3600  # 2 days
            taskConf["SplittingArguments"]["lheInputFiles"] = taskConf["LheInputFiles"]
        elif taskConf["SplittingAlgo"] == "LumiBased":
            taskConf["SplittingArguments"]["lumis_per_job"] = taskConf["LumisPerJob"]
        elif taskConf["SplittingAlgo"] == "FileBased":
            taskConf["SplittingArguments"]["files_per_job"] = taskConf["FilesPerJob"]

        taskConf["SplittingArguments"].setdefault("include_parents", taskConf['IncludeParents'])
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
                    "ChainParentageMap": {"default": {}, "type": dict},
                    "FirstEvent": {"default": 1, "type": int, "validate": lambda x: x > 0,
                                   "null": False},
                    "FirstLumi": {"default": 1, "type": int, "validate": lambda x: x > 0,
                                  "null": False},
                    "ParentageResolved": {"default": False, "type": strToBool, "null": False},
                    ### Override StdBase parameter definition
                    "TimePerEvent": {"default": 12.0, "type": float, "validate": lambda x: x > 0},
                    "Memory": {"default": 2300.0, "type": float, "validate": lambda x: x > 0},
                    "Multicore": {"default": 1, "type": int, "validate": checkMemCore},
                    "EventStreams": {"type": int, "null": True, "default": 0, "validate": checkEventStreams}
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
            'InputStep': {'default': None, 'null': False, 'optional': firstTask},
            'StepName': {'null': False, 'optional': False, 'validate': taskStepName},
            'PrimaryDataset': {'default': None, 'optional': True,
                               'validate': primdataset, 'null': False}
            }

        baseArgs.update(arguments)
        StdBase.setDefaultArgumentsProperty(baseArgs)

        return baseArgs

    @staticmethod
    def getWorkloadAssignArgs():
        baseArgs = StdBase.getWorkloadAssignArgs()
        specArgs = {
            "ChainParentageMap": {"default": {}, "type": dict},
            ### Override StdBase assignment parameter definition
            "Memory": {"type": float, "validate": checkMemCore},
            "Multicore": {"type": int, "validate": checkMemCore},
            "EventStreams": {"type": int, "validate": checkEventStreams},
        }
        baseArgs.update(specArgs)
        StdBase.setDefaultArgumentsProperty(baseArgs)
        return baseArgs

    def validateSchema(self, schema):
        """
        _validateSchema_

        Settings that are not supported and will cause workflow injection to fail, are:
         * workflow with more than 10 steps
         * output from the last step *must* be saved
         * each step configuration must be a dictionary
         * StepChain argument must reflect the number of Steps in the request
         * trident configuration, where 2 steps have the same output module AND datatier
         * usual ConfigCacheID validation
         * and the usual Step arguments validation, as defined in the spec
        """
        numSteps = schema['StepChain']
        if numSteps > 10:
            msg = "Workflow exceeds the maximum allowed number of steps. "
            msg += "Limited to up to 10 steps, found %s steps." % numSteps
            self.raiseValidationException(msg)

        lastStep = "Step%s" % schema['StepChain']
        if not strToBool(schema[lastStep].get('KeepOutput', True)):
            msg = "Dropping the output (KeepOutput=False) of the last step is prohibited.\n"
            msg += "You probably want to remove that step completely and try again."
            self.raiseValidationException(msg=msg)

        outputModTier = []
        for i in range(1, numSteps + 1):
            stepNumber = "Step%s" % i
            if stepNumber not in schema:
                msg = "Step '%s' not present in the request schema." % stepNumber
                self.raiseValidationException(msg=msg)

            step = schema[stepNumber]
            # We can't handle non-dictionary steps
            if not isinstance(step, dict):
                msg = "Step '%s' not defined as a dictionary. " % stepNumber
                msg += "It could be an indicator of JSON error.\n"
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
                for modName, values in viewitems(configOutput):
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
        except WMSpecFactoryException:
            # just re-raise it to keep the error message clear
            raise
        except Exception as ex:
            self.raiseValidationException(str(ex))

        return

    def decideWorkQueueStartPolicy(self, reqDict):
        """
        Given a request dictionary, decides which WorkQueue start
        policy needs to be used in a given request.
        :param reqDict: a dictionary with the creation request information
        :return: a string with the start policy to be used.
        """
        if not reqDict["Step1"].get("InputDataset"):
            return "MonteCarlo"

        inputDset = reqDict["Step1"]["InputDataset"]
        if inputDset.endswith("/MINIAODSIM"):
            return "Dataset"
        else:
            return "Block"
