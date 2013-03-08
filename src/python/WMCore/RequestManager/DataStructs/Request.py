"""
Data Object representing a request

"""


class Request(dict):
    def __init__(self):
        dict.__init__(self)
        #  //
        # // General Request Info
        #//
        self.setdefault("RequestName", None)
        self.setdefault("RequestType", None)
        self.setdefault("RequestStatus", None)
        self.setdefault("RequestPriority", None)
        self.setdefault("RequestNumEvents", None)
        self.setdefault("RequestSizeFiles", None)
        self.setdefault("AcquisitionEra", None)

        #  //
        # // ReqMgr specifics
        #//
        self.setdefault("ReqMgrRequestID", None)
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