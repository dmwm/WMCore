"""
Data Object representing a request

"""


class Request(dict):
    def __init__(self):
        dict.__init__(self)
        self.setdefault("RequestName", None)
        self.setdefault("RequestType", None)
        self.setdefault("RequestStatus", None)
        self.setdefault("RequestPriority", None)
        self.setdefault("RequestNumEvents", None)
        self.setdefault("RequestSizeFiles", None)
        self.setdefault("AcquisitionEra", None)
        self.setdefault("Group", None)
        self.setdefault("Requestor", None)
        self.setdefault("RequestWorkflow", None)
        self.setdefault("OutputDatasets", [])
        self.setdefault("SoftwareVersions", [])
        self.setdefault("InputDatasets", [])
        self.setdefault("InputDatasetTypes", {})