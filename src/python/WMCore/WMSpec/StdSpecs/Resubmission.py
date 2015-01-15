#!/usr/bin/env python
"""
_Resubmission_

Resubmission module, this creates truncated workflows
with limited input for error recovery.
"""

from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import loadWorkload
from WMCore.Lexicon import couchurl, identifier
from WMCore.RequestManager.RequestDB.Interface.Request.GetRequest import getRequestByName
from WMCore.WMSpec.StdSpecs.StdBase import StdBase
from WMCore.WMSpec.WMWorkloadTools import makeList

class ResubmissionWorkloadFactory(StdBase):
    """
    _ResubmissionWorkloadFactory_

    Build Resubmission workloads.
    """

    def buildWorkload(self):
        """
        _buildWorkload_

        Build a resubmission workload from a previous
        workload, it loads the workload and truncates it.
        """
        originalRequest = getRequestByName(self.originalRequestName)
        helper = loadWorkload(originalRequest)
        helper.truncate(self.requestName, self.initialTaskPath,
                        self.acdcServer, self.acdcDatabase,
                        self.collectionName)
        helper.ignoreOutputModules(self.ignoredOutputModules)

        return helper

    def __call__(self, workloadName, arguments):
        StdBase.__call__(self, workloadName, arguments)
        self.originalRequestName = self.initialTaskPath.split('/')[1]
        return self.buildWorkload()

    @staticmethod
    def getWorkloadArguments():
        specArgs = {"RequestType" : {"default" : "Resubmission", "optional" : True, "type" : str,
                                      "attr" : "requestType", "null" : False},
                    "InitialTaskPath" : {"default" : "/SomeRequest/Task1", "type" : str,
                                         "optional" : False, "validate" : lambda x : len(x.split('/')) > 2,
                                         "attr" : "initialTaskPath", "null" : False},
                    "ACDCServer" : {"default" : "https://cmsweb.cern.ch/couchdb", "type" : str,
                                    "optional" : False, "validate" : couchurl,
                                    "attr" : "acdcServer", "null" : False},
                    "ACDCDatabase" : {"default" : "acdcserver", "type" : str,
                                      "optional" : False, "validate" : identifier,
                                      "attr" : "acdcDatabase", "null" : False},
                    "CollectionName" : {"default" : None, "type" : str,
                                        "optional" : True, "validate" : None,
                                        "attr" : "collectionName", "null" : True},
                    "IgnoredOutputModules" : {"default" : [], "type" : makeList,
                                              "optional" : True, "validate" : None,
                                              "attr" : "ignoredOutputModules", "null" : False}}
        StdBase.setDefaultArgumentsProperty(specArgs)
        return specArgs
