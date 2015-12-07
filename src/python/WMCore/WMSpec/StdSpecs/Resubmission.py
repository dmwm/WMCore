#!/usr/bin/env python
"""
_Resubmission_

Resubmission module, this creates truncated workflows
with limited input for error recovery.
"""

from WMCore.Lexicon import couchurl, identifier
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.WMSpec.StdSpecs.StdBase import StdBase
from WMCore.WMSpec.WMWorkloadTools import makeList

class ResubmissionWorkloadFactory(StdBase):
    """
    _ResubmissionWorkloadFactory_

    Build Resubmission workloads.
    """

    def buildWorkload(self, originalRequestURL):
        """
        _buildWorkload_

        Build a resubmission workload from a previous
        workload, it loads the workload and truncates it.
        """
        #TODO remove the dependency on reqmgr1
        if originalRequestURL == None:
            # reqmgr1 call (Due to reqmgr2 dependency imports here
            from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import loadWorkload
            from WMCore.RequestManager.RequestDB.Interface.Request.GetRequest import getRequestByName
            originalRequest = getRequestByName(self.originalRequestName)
            helper = loadWorkload(originalRequest)
        else:
            # reqmgr2 call
            helper = WMWorkloadHelper()
            helper.loadSpecFromCouch(originalRequestURL, self.originalRequestName)
            
        helper.truncate(self.workloadName, self.initialTaskPath,
                        self.acdcServer, self.acdcDatabase,
                        self.collectionName)
        helper.ignoreOutputModules(self.ignoredOutputModules)

        return helper

    def __call__(self, workloadName, arguments):
        StdBase.__call__(self, workloadName, arguments)
        self.originalRequestName = self.initialTaskPath.split('/')[1]
        #TODO remove the None case when reqmgr is retired
        return self.buildWorkload(arguments.get("OriginalRequestCouchURL", None))

    @staticmethod
    def getWorkloadArguments():
        specArgs = {"RequestType" : {"default" : "Resubmission"},
                    "InitialTaskPath" : {"default" : "/SomeRequest/Task1", 
                                         "optional" : False, "validate" : lambda x : len(x.split('/')) > 2,
                                         },
                    "ACDCServer" : {"default" : "https://cmsweb.cern.ch/couchdb", "validate" : couchurl,
                                    "attr" : "acdcServer"},
                    "ACDCDatabase" : {"default" : "acdcserver", "validate" : identifier,
                                      "attr" : "acdcDatabase"},
                    "CollectionName" : {"default" : None, "null" : True},
                    "IgnoredOutputModules" : {"default" : [], "type" : makeList}}
        StdBase.setDefaultArgumentsProperty(specArgs)
        return specArgs
