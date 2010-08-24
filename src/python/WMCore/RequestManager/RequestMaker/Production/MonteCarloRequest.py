#!/usr/bin/env python
"""
_MonteCarloRequest_

RequestMaker and RequestSchema implementations for a normal,
one step MC generation request with a built in generator

"""



from WMCore.RequestManager.RequestMaker.RequestMakerInterface import RequestMakerInterface
from WMCore.RequestManager.DataStructs.RequestSchema import RequestSchema
from WMCore.RequestManager.RequestMaker.Registry import registerRequestType


class MonteCarloRequest(RequestMakerInterface):
    """
    _MonteCarloRequest_

    RequestMaker to create MC requests and workflows

    """
    def __init__(self):
        RequestMakerInterface.__init__(self)


class MonteCarloSchema(RequestSchema):
    """
    _MonteCarlo_

    Data Required for a standard cmsRun MC generation request.

    """
    def __init__(self):
        RequestSchema.__init__(self)
        self.setdefault("ProductionChannel", None)
        self.setdefault("Label", None)
        self.setdefault("Activity", "Production")
        self.setdefault("CMSSWVersion", None)
        self.setdefault("Category", "MC")
        self.setdefault("Configuration", None)
        self.setdefault("PileupDataset", None)
        self.setdefault("FinalDestination", None)
        self.setdefault("PSetHash", None)
        self.validateFields = [
            "ProductionChannel",
            "CMSSWVersion",
            "Label",
            "Configuration",
            "PSetHash",
            ]



registerRequestType("MonteCarlo", MonteCarloRequest, MonteCarloSchema)
