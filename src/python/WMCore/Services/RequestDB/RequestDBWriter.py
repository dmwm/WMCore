from WMCore.Wrappers.JsonWrapper import JSONEncoder
from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader
from WMCore.Database.CMSCouch import Document

class RequestDBWriter(RequestDBReader):

    def __init__(self, couchURL, dbName = None, couchapp = "ReqMgr"):
        # set the connection for local couchDB call
        # inherited from WMStatsReader
        self._commonInit(couchURL, dbName, couchapp)
        self._propertyNeedToBeEncoded = ["RequestTransition",
                                         "SiteWhitelist",
                                         "SiteBlacklist",
                                         "BlockWhitelist",
                                         "SoftwareVersions",
                                         "InputDatasetTypes",
                                         "InputDatasets",
                                         "OutputDatasets",
                                         "Teams"]

    def insertGenericRequest(self, doc):
        
        doc = Document(doc["RequestName"], doc) 
        result = self.couchDB.commitOne(doc)
        self.updateRequestStatus(doc["RequestName"], "new")
        return result

    def updateRequestStatus(self, request, status):
        status = {"RequestStatus": status}
        return self.couchDB.updateDocument(request, self.couchapp, "updaterequest",
                    status)

    def updateRequestProperty(self, request, propDict, dn = None):
        encodeProperty = {}
        for key, value in propDict.items():
            if isinstance(value, list) or isinstance(value, dict):
                encodeProperty[key] = JSONEncoder().encode(value)
            else:
                encodeProperty[key] = value
        if dn:
            encodeProperty["DN"] = dn
        return self.couchDB.updateDocument(request, self.couchapp, "updaterequest",
                    encodeProperty)