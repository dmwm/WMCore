#!/usr/bin/env python
"""
_Request_

Data Object representing a request


"""


class Request(dict):
    """
    _Request_


    """
    def __init__(self):
        dict.__init__(self)
        #  //
        # // General Request Info
        #//
        self.setdefault("RequestName", None)
        self.setdefault("RequestType", None)
        self.setdefault("RequestStatus", None)
        self.setdefault("RequestPriority", None)
        self.setdefault("RequestWorkflow", None)
        self.setdefault("RequestSizeEvents", None)
        self.setdefault("RequestSizeFiles", None)
        self.setdefault("AcquisitionEra", None)

        #  //
        # // ReqMgr specifics
        #//
        self.setdefault("ReqMgrRequestID", None)
        self.setdefault("ReqMgrURL", None)
        self.setdefault("ReqMgrRequestBasePriority", None)

        #  //
        # // Requestor and group information
        #//
        self.setdefault("Group", None)
        self.setdefault("Requestor", None)
        self.setdefault("ReqMgrGroupID", None)
        self.setdefault("ReqMgrRequestorID", None)

        #  //
        # // Output information
        #//
        self.setdefault("OutputDatasets", [])

        #  //
        # // input dataset and software dependencies
        #//
        self.setdefault("SoftwareVersions", [])
        self.setdefault("InputDatasets", [])
        self.setdefault("InputDatasetTypes", {})


        #  //
        # // Workflow information
        #//
        self.setdefault("WorkflowSpec", None)

