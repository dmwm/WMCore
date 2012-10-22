import time
import logging
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Lexicon import splitCouchServiceURL, sanitizeURL
from WMCore.Wrappers.JsonWrapper import JSONEncoder

class WMStatsReader():

    def __init__(self, couchURL, dbName = None):
        couchURL = sanitizeURL(couchURL)['url']
        # set the connection for local couchDB call
        if dbName:
            self.couchURL = couchURL
            self.dbName = dbName
        else:
            self.couchURL, self.dbName = splitCouchServiceURL(couchURL)
        self.couchServer = CouchServer(self.couchURL)
        self.couchDB = CouchServer(self.couchURL).connectDatabase(self.dbName, False)

    def workflowsByStatus(self, statusList):
        keys = statusList
        options = {"stale": "update_after"}
        result = self.couchDB.loadView("WMStats", "requestByStatus", options, keys)
        workflowList = []
        for item in result["rows"]:
            workflowList.append(item["id"])
        return workflowList

    def replicate(self, target):
        self.couchServer.replicate(self.dbName, target,
                                   continuous = True)
