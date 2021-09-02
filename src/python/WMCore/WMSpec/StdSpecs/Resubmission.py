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
        if 'RequestPriority' in arguments:
            helper.setPriority(arguments["RequestPriority"])
        helper.setMemory(arguments['Memory'])
        helper.setCoresAndStreams(arguments['Multicore'], arguments.get("EventStreams", 0))
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
        """
        userArgs = userArgs or {}
        self.fixupArguments(arguments, userArgs)
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

    def fixupArguments(self, arguments, userArgs):
        """
        This method will ensure that:
         * if the user provided some specific arguments, it will be passed down the chain
         * otherwise, the same argument from the original/parent workflow will be dumped
        The only arguments to be tweaked like that are:
            TimePerEvent, Memory, Multicore, EventStreams
        :param arguments: full set of arguments from creation+assignment definitions
        :param userArgs: solely the key/value pair values provided by the client
        :return: nothing, updates are made in place
        """
        specialArgs = ("TimePerEvent", "Memory", "Multicore", "EventStreams")
        argsDefinition = self.getWorkloadCreateArgs()
        for arg in specialArgs:
            if arg not in userArgs:
                arguments[arg] = argsDefinition[arg]["default"]
            else:
                arguments[arg] = userArgs[arg]

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
