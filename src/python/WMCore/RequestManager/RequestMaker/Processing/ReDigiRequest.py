#!/usr/bin/env python
"""
_ReDigiRequest_

Create a ReDigi workload.
"""

from WMCore.RequestManager.RequestMaker.RequestMakerInterface import RequestMakerInterface
from WMCore.RequestManager.DataStructs.RequestSchema import RequestSchema
from WMCore.RequestManager.RequestMaker.Registry import registerRequestType
from WMCore.WMSpec.StdSpecs.ReDigi import reDigiWorkload

class ReDigiRequest(RequestMakerInterface):
    """
    _ReDigiRequest_

    RequestMaker to two file input data processing requests and workflows

    """
    def __init__(self):
        RequestMakerInterface.__init__(self)

    def makeWorkload(self, schema):
        return reDigiWorkload(schema['RequestName'], schema).data


class ReDigiSchema(RequestSchema):
    """
    _ReDigi_

    """
    def __init__(self):
        RequestSchema.__init__(self)
        self.validateFields = [
            "CMSSWVersion",
            "ScramArch",
            "GlobalTag",
            "InputDataset",
            "StepOneConfigCacheID",
            ]
        self.optionalFields = [
            "StepThreeConfigCacheID",
            "StepOneOutputModuleName"
            "StepTwoConfigCacheID",
            "StepTwoOutputModuleName",
            "SiteWhitelist",
            "SiteBlacklist",
            "BlockWhitelist",
            "BlockBlacklist",
            "RunWhitelist",
            "RunBlacklist",
            "CouchUrl",
            "CouchDBName",
            "DbsUrl",
            "UnmergedLFNBase",
            "MergedLFNBase",
            "MinMergeSize",
            "MaxMergeSize",
            "MaxMergeEvents"
            ]

    def validate(self):
        RequestSchema.validate(self)

registerRequestType("ReDigi", ReDigiRequest, ReDigiSchema)
