#!/usr/bin/env python
"""
_DataRequest_


Single input processing request split by file

InputDataset -> cmsRun -> outputDatasets

"""

import time

from WMCore.RequestManager.RequestMaker.RequestMakerInterface import RequestMakerInterface
from WMCore.RequestManager.DataStructs.RequestSchema import RequestSchema
from WMCore.RequestManager.RequestMaker.Registry import registerRequestType
from WMCore.WMSpec.StdSpecs.DataProcessing import dataProcessingWorkload

class DataProcessingRequest(RequestMakerInterface):
    """
    _DataRequest_

    RequestMaker to create data processing requests and workflows

    """
    def __init__(self):
        RequestMakerInterface.__init__(self)

    def makeWorkload(self, schema):
        return dataProcessingWorkload(schema['RequestName'], schema).data


class DataProcessingSchema(RequestSchema):
    """
    _Data_

    Data Required for a standard cmsRun dataset processing request that
    splits the input by file

    """
    def __init__(self):
        RequestSchema.__init__(self)
        self.setdefault("CMSSWVersion", None)
        self.setdefault("ProcessingVersion", None)
        self.setdefault("GlobalTag", None)
        self.setdefault("InputDataset", None)
        self.setdefault("DBSURL", None)
        self.setdefault("Multicore", None)
        self.validateFields = [
            "CMSSWVersion",
            "ConfigCacheID",
            "GlobalTag",
            "InputDataset"
            ]



registerRequestType("DataProcessing", DataProcessingRequest, DataProcessingSchema)
