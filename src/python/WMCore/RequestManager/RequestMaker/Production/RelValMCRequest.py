#!/usr/bin/env python
"""
_RelValMCRequest_

RequestMaker and RequestSchema implementations for a normal,
one step MC generation request with a built in generator

"""



from WMCore.RequestManager.RequestMaker.RequestMakerInterface import RequestMakerInterface
from WMCore.RequestManager.DataStructs.RequestSchema import RequestSchema
from WMCore.RequestManager.RequestMaker.Registry import registerRequestType
from WMCore.WMSpec.StdSpecs.RelValMC import RelValMCWorkloadFactory

class RelValMCRequest(RequestMakerInterface):
    """
    _RelValMCRequest_

    RequestMaker to create MC requests and workflows

    """
    def __init__(self):
        RequestMakerInterface.__init__(self)

    def makeWorkload(self, schema):
        factory = RelValMCWorkloadFactory()
        return factory(schema['RequestName'], schema).data


class RelValMCSchema(RequestSchema):
    """
    _RelValMC_

    Data Required for a standard cmsRun MC generation request.

    """
    def __init__(self):
        RequestSchema.__init__(self)
        self.validateFields = [
            "CMSSWVersion", "Requestor", "ScramArch",
            "PrimaryDataset", "GlobalTag", "RequestSizeEvents",
            "GenConfigCacheID", "StepOneConfigCacheID", "StepTwoConfigCacheID",
            "GenOutputModuleName", "StepOneOutputModuleName"]

registerRequestType("RelValMC", RelValMCRequest, RelValMCSchema)
