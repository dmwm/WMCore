#!/usr/bin/env python
"""
_PrivateMCRequest_

RequestMaker and RequestSchema implementations for a private user
one step MC generation request with a built in generator

"""



from WMCore.RequestManager.RequestMaker.RequestMakerInterface import RequestMakerInterface
from WMCore.RequestManager.DataStructs.RequestSchema import RequestSchema
from WMCore.RequestManager.RequestMaker.Registry import registerRequestType
from WMCore.WMSpec.StdSpecs.PrivateMC import PrivateMCWorkloadFactory

class PrivateMCRequest(RequestMakerInterface):
    """
    _PrivateMCRequest_

    RequestMaker to create PrivateMC requests and workflows

    """
    def __init__(self):
        RequestMakerInterface.__init__(self)

    def makeWorkload(self, schema):
        factory = PrivateMCWorkloadFactory()
        return factory(schema['RequestName'], schema).data


class PrivateMCSchema(RequestSchema):
    """
    _PrivateMC_

    Data Required for a PrivateMC cmsRun generation request.

    """
    def __init__(self):
        RequestSchema.__init__(self)
        self.setdefault("CMSSWVersion", None)
        self.setdefault("AnalysisConfigCacheDoc", None)
        self.setdefault("PileupDataset", None)
        self.validateFields = [
            "CMSSWVersion",
            "AnalysisConfigCacheDoc",
            "PrimaryDataset",
            ]



registerRequestType("PrivateMC", PrivateMCRequest, PrivateMCSchema)
