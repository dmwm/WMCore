#!/usr/bin/env python
"""
_FileBasedRequest_


Single input processing request split by file

InputDataset -> cmsRun -> outputDatasets

"""

import time

from WMCore.RequestManager.RequestMaker.RequestMakerInterface import RequestMakerInterface
from WMCore.RequestManager.DataStructs.RequestSchema import RequestSchema
from WMCore.RequestManager.RequestMaker.Registry import registerRequestType


class FileBasedRequest(RequestMakerInterface):
    """
    _FileBasedRequest_

    RequestMaker to create data processing requests and workflows

    """
    def __init__(self):
        RequestMakerInterface.__init__(self)



class FileBasedSchema(RequestSchema):
    """
    _FileBased_

    Data Required for a standard cmsRun dataset processing request that
    splits the input by file

    """
    def __init__(self):
        RequestSchema.__init__(self)
        self.setdefault("CMSSWVersion", None)
        self.setdefault("ProcessingVersion", None)
        self.setdefault("ProdConfigCacheID", None)
        self.setdefault("GlobalTag", None)
        self.setdefault("InputDataset", None)
        self.setdefault("DBSURL", None)

        self.validateFields = [
            "CMSSWVersion",
            "ProdConfigCacheID",
            "GlobalTag",
            "InputDataset"
            ]



registerRequestType("FileProcessing", FileBasedRequest, FileBasedSchema)


