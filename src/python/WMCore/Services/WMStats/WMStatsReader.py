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

    def workflowsByStatus(self, statusList, format = "list", stale = "update_after"):
        keys = statusList
        options = {}
        if stale:
            options = {"stale": stale}
        result = self.couchDB.loadView("WMStats", "requestByStatus", options, keys)

        if format == "dict":
            workflowDict = {}
            for item in result["rows"]:
                workflowDict[item["id"]] = None
            return workflowDict
        else:
            workflowList = []
            for item in result["rows"]:
                workflowList.append(item["id"])
            return workflowList

    def workflowStatus(self, stale = "update_after"):
        """
        _workflowStatus_

        Return a dictionary with all available workflows,
        grouped by status and with the timestamp of the status
        """
        options = {}
        if stale:
            options = {"stale" : stale}
        result = self.couchDB.loadView("WMStats", "requestByStatus", options)

        stateDict = {}
        for item in result['rows']:
            if item["key"] not in stateDict:
                stateDict[item["key"]] = {}
            stateDict[item["key"]][item["id"]] = item["value"]

        return stateDict

    def getDBInstance(self):
        return self.couchDB
