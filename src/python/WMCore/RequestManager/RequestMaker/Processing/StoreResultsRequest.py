#!/usr/bin/env python
"""
_StoreResultsRequest_
"""

from WMCore.RequestManager.RequestMaker.RequestMakerInterface import RequestMakerInterface
from WMCore.RequestManager.DataStructs.RequestSchema import RequestSchema
from WMCore.RequestManager.RequestMaker.Registry import registerRequestType, retrieveRequestMaker


class StoreResultsRequest(RequestMakerInterface):
    """
    _StoreResultsRequest_

    RequestMaker to two file input data processing requests and workflows

    """
    def __init__(self):
        RequestMakerInterface.__init__(self)


class StoreResultsSchema(RequestSchema):
    """
    _StoreResults_

    Data Required for a standard cmsRun two file read processing request.

    """
    def __init__(self):
        RequestSchema.__init__(self)
        # not used yet
        self.validateFields = [
            'InputDatasets',
            'CMSSWVersion',
            'ScramArch',
            'Group',
            'DbsUrl'
            ]


registerRequestType("StoreResults", StoreResultsRequest, StoreResultsSchema)
