from WMCore.Database.CMSCouch import Document
from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader


class RequestDBWriter(RequestDBReader):
    def __init__(self, couchURL, couchapp="ReqMgr"):
        # set the connection for local couchDB call
        # inherited from WMStatsReader
        self._commonInit(couchURL, couchapp)

    def insertGenericRequest(self, doc):

        doc = Document(doc["RequestName"], doc)
        result = self.couchDB.commitOne(doc)
        self.updateRequestStatus(doc["RequestName"], "new")
        return result

    def updateRequestStatus(self, request, status, dn=None):
        status = {"RequestStatus": status}
        if dn:
            status["DN"] = dn
        return self.couchDB.updateDocument(request, self.couchapp, "updaterequest",
                                           status)

    def updateRequestStats(self, request, stats):
        """
        This function is only available ReqMgr couch app currently (not T0)
        WQ specific function
        param: stats: dict of {'total_jobs': 0, 'input_lumis': 0,
                               'input_events': 0, 'input_num_files': 0}
        """
        return self.couchDB.updateDocument(request, self.couchapp, "totalstats",
                                           fields=stats)

    def updateRequestProperty(self, request, propDict, dn=None):
        if dn:
            propDict["DN"] = dn
        return self.couchDB.updateDocument(request, self.couchapp, "updaterequest",
                                           propDict, useBody=True)
