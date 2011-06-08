#!/usr/bin/env python
"""
_MonteCarloFromGENRequest_

Process MC GEN files
"""

import time

from WMCore.RequestManager.RequestMaker.RequestMakerInterface import RequestMakerInterface
from WMCore.RequestManager.DataStructs.RequestSchema import RequestSchema
from WMCore.RequestManager.RequestMaker.Registry import registerRequestType
from WMCore.WMSpec.StdSpecs.MonteCarloFromGEN import monteCarloFromGENWorkload

class MonteCarloFromGENRequest(RequestMakerInterface):
    """
    _MonteCarloFromGENRequest_

    """
    def __init__(self):
        RequestMakerInterface.__init__(self)

    def makeWorkload(self, schema):
        return monteCarloFromGENWorkload(schema['RequestName'], schema).data


class MonteCarloFromGENSchema(RequestSchema):
    """
    _MonteCarloFromGENSchema_

    """
    def __init__(self):
        RequestSchema.__init__(self)
        self.setdefault("CMSSWVersion", None)
        self.setdefault("ProcessingVersion", None)
        self.setdefault("GlobalTag", None)
        self.setdefault("InputDataset", None)
        self.setdefault("DBSURL", None)
        self.validateFields = [
            "CMSSWVersion",
            "ProcConfigCacheID",
            "GlobalTag",
            "InputDataset"
            ]



registerRequestType("MonteCarloFromGEN", MonteCarloFromGENRequest, MonteCarloFromGENSchema)
