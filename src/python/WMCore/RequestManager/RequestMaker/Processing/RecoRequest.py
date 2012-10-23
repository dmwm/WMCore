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

class RecoRequest(RequestMakerInterface):
    """
    _ReRecoRequest_

    RequestMaker to two file input data processing requests and workflows

    """
    def __init__(self):
        RequestMakerInterface.__init__(self)

    def makeWorkload(self, schema):
        # FIXME
        return rerecoWorkload(schema['RequestName'], schema).data


class RecoSchema(RequestSchema):
    """
    _ReReco_

    Data Required for a standard cmsRun two file read processing request.

    """
    def __init__(self):
        RequestSchema.__init__(self)
        self.validateFields = [
            "CMSSWVersion",
            "GlobalTag",
            "InputDatasets",
            "DbsUrl",
            "LFNCategory",
            "OutputTiers"
            ]
        self.optionalFields = [
            "SiteWhitelist",
            "SiteBlackList",
            "BlockWhitelist",
            "BlockBlacklist",
            "RunWhitelist",
            "RunBlacklist",
            "Scenario"
            ]

    def validate(self):
        RequestSchema.validate(self)
        assert(isinstance(self['OutputTiers'], list))
        for tier in self['OutputTiers']:
            assert(tier in ['RECO', 'AOD', 'ALCA'])


registerRequestType("Reco", RecoRequest, RecoSchema)
