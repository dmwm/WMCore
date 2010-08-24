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
        self.setdefault("Activity", "Processing")
        self.setdefault("CMSSWVersion", None)
        self.setdefault("Category", "Data")
        self.setdefault("ProcessingVersion", None)
        self.setdefault("Configuration", None)
        self.setdefault("Conditions", None)
        self.setdefault("FinalDestination", None)
        self.setdefault("PSetHash", None)
        self.setdefault("InputDataset", None)

        self.setdefault("FilesPerJob", 1)
        self.setdefault("DBSURL", None) # default to global?

        self.setdefault("BlockList", [])
        self.setdefault("SiteList",  [])



        self.validateFields = [
            "CMSSWVersion",
            "ProcessingVersion",
            "Configuration",
            "Conditions",
            "PSetHash",
            "InputDataset",
            ]



registerRequestType("FileProcessing", FileBasedRequest, FileBasedSchema)


