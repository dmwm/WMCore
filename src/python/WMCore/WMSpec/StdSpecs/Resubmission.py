#!/usr/bin/env python
"""
_Resubmission_

Resubmission module, this creates truncated workflows
with limited input for error recovery.
"""
from Utils.Utilities import makeList
from WMCore.Lexicon import couchurl, identifier, cmsname, dataset
from WMCore.WMSpec.StdSpecs.StdBase import StdBase
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.WMSpec.WMWorkloadTools import (loadSpecClassByType, validateArgumentsCreate,
                                           checkMemCore, checkEventStreams, checkTimePerEvent)


class ResubmissionWorkloadFactory(StdBase):
    """
    _ResubmissionWorkloadFactory_

    Build Resubmission workloads.
    """

    def buildWorkload(self, arguments):
        """
        _buildWorkload_

        Build a resubmission workload from a previous
        workload, it loads the workload and truncates it.
        """
        helper = WMWorkloadHelper()
        # where to find the original spec file
        originalRequestURL = "%s/%s" % (arguments['CouchURL'], arguments['CouchWorkloadDBName'])

        helper.loadSpecFromCouch(originalRequestURL, self.originalRequestName)

        helper.truncate(self.workloadName, self.initialTaskPath,
                        self.acdcServer, self.acdcDatabase,
                        self.collectionName)
        helper.ignoreOutputModules(self.ignoredOutputModules)

        # override a couple of parameters, if provided by user
        # Note that if it was provided by the user, then it's already part of the arguments too
        if "RequestPriority" in self.userArgs:
            helper.setPriority(arguments["RequestPriority"])
        if "Memory" in self.userArgs:
            helper.setMemory(arguments["Memory"])
        if "Multicore" in self.userArgs or "EventStreams" in self.userArgs:
            self.setCoresAndStreams(helper, arguments)
        if "TimePerEvent" in self.userArgs:
            helper.setTimePerEvent(arguments.get("TimePerEvent"))

        return helper

    def __call__(self, workloadName, arguments):
        StdBase.__call__(self, workloadName, arguments)
        self.originalRequestName = self.initialTaskPath.split('/')[1]
        return self.buildWorkload(arguments)

    def factoryWorkloadConstruction(self, workloadName, arguments, userArgs=None):
        """
        Resubmission factory override of the master StdBase factory.
        Builds the entire workload, with specific features to Resubmission
        requests, and also performs a sub-set of the standard validation.
        :param workloadName: string with the name of the workload
        :param arguments: dictionary with all the relevant create/assign parameters
        :param userArgs: dictionary with user specific parameters
        :return: the workload object
        """
        self.userArgs = userArgs or {}
        self.fixupArguments(arguments)
        self.validateSchema(schema=arguments)
        workload = self.__call__(workloadName, arguments)
        self.validateWorkload(workload)
        return workload

    @staticmethod
    def getWorkloadCreateArgs():
        specArgs = {"RequestType": {"default": "Resubmission"},
                    "ResubmissionCount": {"default": 1, "type": int},
                    "OriginalRequestType": {"null": False},
                    "OriginalRequestName": {"null": False},
                    "InitialTaskPath": {"optional": False,
                                        "validate": lambda x: len(x.split('/')) > 2},
                    "ACDCServer": {"default": "https://cmsweb.cern.ch/couchdb", "validate": couchurl,
                                   "attr": "acdcServer"},
                    "ACDCDatabase": {"default": "acdcserver", "validate": identifier,
                                     "attr": "acdcDatabase"},
                    "CollectionName": {"default": None, "null": True},
                    "IgnoredOutputModules": {"default": [], "type": makeList},
                    "SiteWhitelist": {"default": [], "type": makeList,
                                      "validate": lambda x: all([cmsname(y) for y in x])},
                    # it can be Chained or MC requests, so lets make it optional
                    "InputDataset": {"optional": True, "validate": dataset, "null": True},
                    ### Override StdBase parameter definition
                    "TimePerEvent": {"default": None, "type": float, "null": True, "validate": checkTimePerEvent},
                    "Memory": {"default": None, "type": float, "null": True, "validate": checkMemCore},
                    "Multicore": {"default": None, "type": int, "null": True, "validate": checkMemCore},
                    "EventStreams": {"default": None, "type": int, "null": True, "validate": checkEventStreams}
                    }

        StdBase.setDefaultArgumentsProperty(specArgs)
        return specArgs

    def fixupArguments(self, arguments):
        """
        This method will ensure that:
         * if the user provided some specific arguments, it will be passed down the chain
         * otherwise, the same argument from the original/parent workflow will be dumped
        The only arguments to be tweaked like that are:
            TimePerEvent, Memory, Multicore, EventStreams
        :param arguments: full set of arguments from creation+assignment definitions
        :return: nothing, updates are made in place
        """
        if arguments["OriginalRequestType"] == "ReReco":
            # top level arguments are already correct
            return

        specialArgs = ("TimePerEvent", "Memory", "Multicore", "EventStreams")
        argsDefinition = self.getWorkloadCreateArgs()
        for arg in specialArgs:
            if arg in self.userArgs:
                arguments[arg] = self.userArgs[arg]
                # these should not be persisted under the Step dictionary
                if arg in ("TimePerEvent", "Memory") and arguments["OriginalRequestType"] == "StepChain":
                    continue
            elif arg in ("TimePerEvent", "Memory") and arguments["OriginalRequestType"] == "StepChain":
                # there is only the top level argument, reuse it
                continue
            else:
                arguments[arg] = argsDefinition[arg]["default"]
                continue
            # now update the inner values as well
            specType = "Step" if arguments["OriginalRequestType"] == "StepChain" else "Task"
            for innerIdx in range(1, arguments.get("{}Chain".format(specType), 0) + 1):
                # innerKey is meant to be: Task1 or Step1, Task2 or Step2 ...
                innerKey = "{}{}".format(specType, innerIdx)
                # the value of either TaskName or StepName
                innerName = arguments[innerKey]["{}Name".format(specType)]
                # value to be defined inside the Task/Step
                if isinstance(self.userArgs[arg], dict):
                    arguments[innerKey][arg] = self.userArgs[arg][innerName]
                else:
                    arguments[innerKey][arg] = self.userArgs[arg]

    def setCoresAndStreams(self, workloadHelper, inputArgs):
        """
        Set helper for the Multicore and EventStreams parameters, which
        need to be dealt with in a different way depending on the parent
        spec type
        :param workloadHelper: WMWorkload object
        :param inputArgs: dictionary with the Resubmission input args
        """
        # simple and easy way to update it
        if not isinstance(inputArgs["Multicore"], dict):
            workloadHelper.setCoresAndStreams(inputArgs["Multicore"], inputArgs.get("EventStreams", 0))
        # still a simple way to update it
        elif inputArgs['OriginalRequestType'] == "TaskChain":
            workloadHelper.setCoresAndStreams(inputArgs["Multicore"], inputArgs.get("EventStreams", 0))
        # check if it's a StepChain then based on its steps mapping
        elif workloadHelper.getStepMapping():
            # map is supposed to be in the format of:
            # {'RecoPU_2021PU': ('Step1', 'cmsRun1'), 'Nano_2021PU': ('Step2', 'cmsRun2')}
            stepChainMap = workloadHelper.getStepMapping()
            # we need to create an easier map now
            coresByCmsRun = {}
            evtStreamsByCmsRun = {}
            for stepName in stepChainMap:
                cores = inputArgs["Multicore"][stepName]
                coresByCmsRun[stepChainMap[stepName][1]] = cores
                if inputArgs["EventStreams"] and isinstance(inputArgs["EventStreams"], dict):
                    streams = inputArgs["EventStreams"][stepName]
                elif inputArgs["EventStreams"]:
                    streams = inputArgs["EventStreams"]
                else:
                    streams = 0
                evtStreamsByCmsRun[stepChainMap[stepName][1]] = streams

            # Now iterate through the tasks and update it from within the steps
            for task in workloadHelper.taskIterator():
                if task.taskType() in ["Merge", "Harvesting", "Cleanup", "LogCollect"]:
                    continue
                for cmsRunName in coresByCmsRun:
                    stepHelper = task.getStepHelper(cmsRunName)
                    stepHelper.setNumberOfCores(coresByCmsRun[cmsRunName],
                                                evtStreamsByCmsRun[cmsRunName])

    def validateSchema(self, schema):
        """
        Since we skip the master validation for Resubmission specs, we better have
        some specific validation
        """
        if schema.get("ResubmissionCount", 1) > 1:
            # we cannot validate such schema
            return
        # load assignment + creation + resubmission creation args definition
        argumentDefinition = self.getWorkloadAssignArgs()
        parentSpecClass = loadSpecClassByType(schema['OriginalRequestType'])
        argumentDefinition.update(parentSpecClass.getWorkloadCreateArgs())
        argumentDefinition.update(self.getWorkloadCreateArgs())

        try:
            validateArgumentsCreate(schema, argumentDefinition)
        except Exception as ex:
            self.raiseValidationException(str(ex))

        # and some extra validation based on the parent workflow
        if schema['OriginalRequestType'] != "TaskChain":
            for param in ("TimePerEvent", "Memory"):
                if isinstance(schema.get(param), dict):
                    msg = "ACDC for parent spec of type: {} ".format(schema['OriginalRequestType'])
                    msg += "cannot have parameter: {} defined as a dictionary: {}".format(param,
                                                                                          schema[param])
                    self.raiseValidationException(msg)
