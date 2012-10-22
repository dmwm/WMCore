#!/usr/bin/env python
"""
_ReRecoRequest_


Dual input processing request.

ParentDataset/ChildDataset -> cmsRun -> outputDatasets

"""

from WMCore.RequestManager.RequestMaker.RequestMakerInterface import RequestMakerInterface
from WMCore.RequestManager.DataStructs.RequestSchema import RequestSchema
from WMCore.RequestManager.RequestMaker.Registry import registerRequestType
from WMCore.WMSpec.StdSpecs.ReReco import rerecoWorkload

class ReRecoRequest(RequestMakerInterface):
    """
    _ReRecoRequest_

    RequestMaker to two file input data processing requests and workflows

    """
    def __init__(self):
        RequestMakerInterface.__init__(self)

    def makeWorkload(self, schema):
        return rerecoWorkload(schema['RequestName'], schema).data


class ReRecoSchema(RequestSchema):
    """
    _ReReco_

    Data Required for a standard cmsRun two file read processing request.

    """
    def __init__(self):
        RequestSchema.__init__(self)
        self.validateFields = [
            "CMSSWVersion",
            "ScramArch",
            "GlobalTag",
            "InputDataset"
            ]
        self.optionalFields = [
            "SiteWhitelist",
            "SiteBlacklist",
            "BlockWhitelist",
            "BlockBlacklist",
            "RunWhitelist",
            "RunBlacklist",
            "Scenario",
            "ProcessingConfig",
            "ProcessingVersion",
            "SkimConfig",
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
        if self['InputDataset'].count('/') != 3:
            raise RuntimeError, "Need three slashes in InputDataset "+self['InputDataset']

registerRequestType("ReReco", ReRecoRequest, ReRecoSchema)
