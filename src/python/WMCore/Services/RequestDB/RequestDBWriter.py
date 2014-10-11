from WMCore.Wrappers.JsonWrapper import JSONEncoder
from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader
from WMCore.Database.CMSCouch import Document

class RequestDBWriter(RequestDBReader):

    def __init__(self, couchURL, dbName = None, couchapp = "ReqMgr"):
        # set the connection for local couchDB call
        # inherited from WMStatsReader
        self._commonInit(couchURL, dbName, couchapp)


    def insertGenericRequest(self, doc):
        
        doc = Document(doc["RequestName"], doc) 
        result = self.couchDB.commitOne(doc)
        self.updateRequestStatus(doc["RequestName"], "new")
        return result

    def updateRequestStatus(self, request, status):
        status = {"RequestStatus": status}
        return self.couchDB.updateDocument(request, self.couchapp, "updaterequest",
                    status)
