#!/usr/bin/env python
"""
_ResubmissionRequest_

Prepare a workflow to be resubmitted.
"""

import WMCore.RequestManager.RequestDB.Interface.Request.GetRequest as GetRequest
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import loadWorkload

from WMCore.RequestManager.RequestMaker.RequestMakerInterface import RequestMakerInterface
from WMCore.RequestManager.DataStructs.RequestSchema import RequestSchema
from WMCore.RequestManager.RequestMaker.Registry import registerRequestType
from WMCore.WMSpec.WMWorkload import WMWorkload

class ResubmissionRequest(RequestMakerInterface):
    """
    _ResubmissionRequest_

    """
    def __init__(self):
        RequestMakerInterface.__init__(self)

    def makeWorkload(self, schema):
        originalRequest = GetRequest.getRequestByName(schema['OriginalRequestName'])
        helper = loadWorkload(originalRequest)
        helper.truncate(schema["RequestName"], schema["InitialTaskPath"],
                        schema["ACDCServer"], schema["ACDCDatabase"])

        return helper.data

class ResubmissionSchema(RequestSchema):
    """
    _ResubmissionSchema_

    """
    def __init__(self):
        RequestSchema.__init__(self)
        self.validateFields = [
            "OriginalRequestName",
            "InitialTaskPath",
            "ACDCServer",
            "ACDCDatabase"
            ]

registerRequestType("Resubmission", ResubmissionRequest, ResubmissionSchema)
