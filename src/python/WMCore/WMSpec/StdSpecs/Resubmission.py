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
from WMCore.WMSpec.WMWorkloadTools import loadSpecClassByType, validateArgumentsCreate


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
        if 'Memory' in arguments:
            helper.setMemory(arguments['Memory'])
        if 'Campaign' in arguments and not helper.getCampaign():
            helper.setCampaign(arguments["Campaign"])
        if 'RequestPriority' in arguments:
            helper.setPriority(arguments["RequestPriority"])
        if 'TimePerEvent' in arguments:
            for task in helper.taskIterator():
                task.setJobResourceInformation(timePerEvent=arguments["TimePerEvent"])

        return helper

    def __call__(self, workloadName, arguments):
        StdBase.__call__(self, workloadName, arguments)
        self.originalRequestName = self.initialTaskPath.split('/')[1]
        return self.buildWorkload(arguments)

    @staticmethod
    def getWorkloadCreateArgs():
        specArgs = {"RequestType" : {"default" : "Resubmission"},
                    "OriginalRequestType": {"null": False},
                    "OriginalRequestName": {"null": False},
                    "InitialTaskPath" : {"default" : "/SomeRequest/Task1", "optional": False,
                                         "validate": lambda x: len(x.split('/')) > 2},
                    "ACDCServer" : {"default" : "https://cmsweb.cern.ch/couchdb", "validate" : couchurl,
                                    "attr" : "acdcServer"},
                    "ACDCDatabase" : {"default" : "acdcserver", "validate" : identifier,
                                      "attr" : "acdcDatabase"},
                    "CollectionName": {"default" : None, "null" : True},
                    "IgnoredOutputModules": {"default": [], "type": makeList},
                    "SiteWhitelist": {"default": [], "type": makeList,
                                      "validate": lambda x: all([cmsname(y) for y in x])},
                    # it can be Chained or MC requests, so lets make it optional
                    "InputDataset" : {"optional": True, "validate" : dataset, "null" : True}}

        StdBase.setDefaultArgumentsProperty(specArgs)
        return specArgs

    def validateSchema(self, schema):
        """
        Since we skip the master validation for Resubmission specs, we better have
        some specific validation
        """
        argumentDefinition = {}
        if 'OriginalRequestType' in schema:
            parentSpecClass = loadSpecClassByType(schema['OriginalRequestType'])
            argumentDefinition = parentSpecClass.getWorkloadCreateArgs()

        argumentDefinition.update(self.getWorkloadCreateArgs())
        # RequestStatus has different validate function, at this point we must use
        # the creation one. Thus just save it to update the final arg definition
        createStatus = {'RequestStatus': dict(argumentDefinition['RequestStatus'])}
        argumentDefinition.update(self.getWorkloadAssignArgs())
        argumentDefinition.update(createStatus)

        try:
            validateArgumentsCreate(schema, argumentDefinition)
        except Exception as ex:
            self.raiseValidationException(str(ex))
