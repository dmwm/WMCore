#!/usr/bin/env python
"""
_Resubmission_

Resubmission module, this creates truncated workflows
with limited input for error recovery.
"""

from Utils.Utilities import makeList
from WMCore.Lexicon import couchurl, identifier
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.WMSpec.StdSpecs.StdBase import StdBase

class ResubmissionWorkloadFactory(StdBase):
    """
    _ResubmissionWorkloadFactory_

    Build Resubmission workloads.
    """

    def buildWorkload(self, originalRequestURL, arguments):
        """
        _buildWorkload_

        Build a resubmission workload from a previous
        workload, it loads the workload and truncates it.
        """

        helper = WMWorkloadHelper()
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
        #TODO remove the None case when reqmgr is retired
        return self.buildWorkload(arguments.get("OriginalRequestCouchURL", None), arguments)

    @staticmethod
    def getWorkloadArguments():
        specArgs = {"RequestType" : {"default" : "Resubmission"},
                    "InitialTaskPath" : {"default" : "/SomeRequest/Task1", "optional": False,
                                         "validate": lambda x: len(x.split('/')) > 2},
                    "ACDCServer" : {"default" : "https://cmsweb.cern.ch/couchdb", "validate" : couchurl,
                                    "attr" : "acdcServer"},
                    "ACDCDatabase" : {"default" : "acdcserver", "validate" : identifier,
                                      "attr" : "acdcDatabase"},
                    "CollectionName" : {"default" : None, "null" : True},
                    "IgnoredOutputModules" : {"default" : [], "type" : makeList}}
        StdBase.setDefaultArgumentsProperty(specArgs)
        return specArgs
