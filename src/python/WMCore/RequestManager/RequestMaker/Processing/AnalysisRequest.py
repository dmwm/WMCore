#!/usr/bin/env python
"""
_AnalysisRequest_
"""

from WMCore.RequestManager.RequestMaker.RequestMakerInterface import RequestMakerInterface
from WMCore.RequestManager.DataStructs.RequestSchema import RequestSchema
from WMCore.RequestManager.RequestMaker.Registry import registerRequestType
from WMCore.WMSpec.StdSpecs.Analysis import AnalysisWorkloadFactory


class AnalysisRequest(RequestMakerInterface):
    """
    _AnalysisRequest_

    """
    def __init__(self):
        RequestMakerInterface.__init__(self)

    def makeWorkload(self, schema):
        factory = AnalysisWorkloadFactory()
        return factory(schema['RequestName'], schema).data

class AnalysisSchema(RequestSchema):
    def __init__(self):
        RequestSchema.__init__(self)

        self.validateFields = []
        self.optionalFields = []

        self.validateFields = [
            "CMSSWVersion",
            "ScramArch",
            "InputDataset",
            "RequestorDN"
            ]
        self.optionalFields = [
            "SiteWhitelist",
            "SiteBlacklist",
            "BlockWhitelist",
            "BlockBlacklist",
            "RunWhitelist",
            "RunBlacklist",
            "CouchUrl",
            "CouchDBName",
            "DbsUrl",
            ]

    def validate(self):
        RequestSchema.validate(self)

registerRequestType("Analysis", AnalysisRequest, AnalysisSchema)
