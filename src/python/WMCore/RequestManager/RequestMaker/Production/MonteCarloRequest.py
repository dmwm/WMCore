#!/usr/bin/env python
"""
_MonteCarloRequest_

RequestMaker and RequestSchema implementations for a normal,
one step MC generation request with a built in generator

"""



from WMCore.RequestManager.RequestMaker.RequestMakerInterface import RequestMakerInterface
from WMCore.RequestManager.DataStructs.RequestSchema import RequestSchema
from WMCore.RequestManager.RequestMaker.Registry import registerRequestType
from WMCore.WMSpec.StdSpecs.MonteCarlo import MonteCarloWorkloadFactory

class MonteCarloRequest(RequestMakerInterface):
    """
    _MonteCarloRequest_

    RequestMaker to create MC requests and workflows

    """
    def __init__(self):
        RequestMakerInterface.__init__(self)

    def makeWorkload(self, schema):
        factory = MonteCarloWorkloadFactory()
        return factory(schema['RequestName'], schema).data


class MonteCarloSchema(RequestSchema):
    """
    _MonteCarlo_

    Data Required for a standard cmsRun MC generation request.

    """
    def __init__(self):
        RequestSchema.__init__(self)
        self.setdefault("CMSSWVersion", None)
        self.setdefault("ProdConfigCacheID", None)
        self.setdefault("PileupDataset", None)
        self.validateFields = [
            "CMSSWVersion",
            "ProdConfigCacheID",
            "PrimaryDataset"
            ]



registerRequestType("MonteCarlo", MonteCarloRequest, MonteCarloSchema)
