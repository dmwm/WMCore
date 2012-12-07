#!/usr/bin/env python
"""
_ReReco_

Standard ReReco workflow.
"""

import os

from WMCore.WMSpec.StdSpecs.StdBase import StdBase

import WMCore.RequestManager.RequestDB.Interface.Request.GetRequest as GetRequest
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import loadWorkload


class ResubmissionWorkloadFactory(StdBase):
    """
    _ResubmissionWorkloadFactory_

    Build Resubmission workloads
    """

    def __call__(self, workloadName, arguments):

        requestName     = arguments['OriginalRequestName']
        originalRequest = GetRequest.getRequestByName(requestName)
        helper = loadWorkload(originalRequest)
        helper.truncate(arguments["RequestName"], arguments["InitialTaskPath"],
                        arguments["ACDCServer"], arguments["ACDCDatabase"],
                        arguments.get("CollectionName"))
        helper.ignoreOutputModules(arguments.get("IgnoredOutputModules", []))
        return helper

    def validateSchema(self, schema):
        """
        _validateSchema_

        It's an ACDC workflow, it needs ACDC data
        """
        requiredFields = ["OriginalRequestName", "InitialTaskPath",
                          "ACDCServer", "ACDCDatabase"]
        self.requireValidateFields(fields = requiredFields, schema = schema,
                                   validate = False)
