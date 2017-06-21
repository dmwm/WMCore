from __future__ import (division, print_function)
from WMCore.Database.CMSCouch import CouchServer, Database
from WMCore.Lexicon import splitCouchServiceURL

class FWJRDBAPI():

    def __init__(self, couchURL, dbName=None):
        """
        setting up comon variables for inherited class.
        inherited class should call this in their init function
        """
        if isinstance(couchURL, Database):
            self.couchDB = couchURL
            self.couchURL = self.couchDB['host']
            self.dbName = self.couchDB.name
            self.couchServer = CouchServer(self.couchURL)
        else:
            if dbName == None:
                self.couchURL, self.dbName = splitCouchServiceURL(couchURL)
            else:
                self.couchURL = couchURL
                self.dbName = dbName
            self.couchServer = CouchServer(self.couchURL)
            self.couchDB = self.couchServer.connectDatabase(self.dbName, False)
        self.couchapp = "FWJRDump"
        self.defaultStale = {"stale": "update_after"}


    def setDefaultStaleOptions(self, options):
        if not options:
            options = {}
        if 'stale' not in options:
            options.update(self.defaultStale)
        return options

    def _setNoStale(self):
        """
        Use this only for the unittest
        """
        self.defaultStale = {}

    def _getCouchView(self, view, options, keys = []):

        options = self.setDefaultStaleOptions(options)

        if keys and isinstance(keys, basestring):
            keys = [keys]
        return self.couchDB.loadView(self.couchapp, view, options, keys)


    def _filterCouchInfo(self, couchInfo):
        # remove the couch specific information
        for key in ['_rev', '_attachments']:
            if  key in couchInfo:
                del couchInfo[key]
        return


    def _formatCouchData(self, data, key = "id", detail = True, filterCouch = True,
                         returnDict = False):
        result = {}
        for row in data['rows']:
            if 'error' in row:
                continue
            if "doc" in row:
                if filterCouch:
                    self._filterCouchInfo(row["doc"])
                result[row[key]] = row["doc"]
            else:
                result[row[key]] = row["value"]
        if detail or returnDict:
            return result
        else:
            return result.keys()

    def getFWJRByArchiveStatus(self, status, limit=None, skip=None):
        """
        'status': list of the status or status string
        """
        options = {}
        options["include_docs"] = True

        if limit != None:
            options["limit"] = limit
        if skip != None:
            options["skip"] = skip
        keys = status
        return self._getCouchView("reportsByArchiveStatus", options, keys)

    def updateArchiveUploadedStatus(self, docID):

        return self.couchDB.updateDocument(docID, self.couchapp, "archiveStatus")

    def isAllFWJRArchived(self, workflow):
        keys = [[workflow, "ready"]]
        options = {"reduce": True, "group": True}
        result = self._getCouchView("byWorkflowAndArchiveStatus", options, keys)
        if len(result["rows"]) == 0:
            return True
        else:
            return False


    def outputByWorkflowName(self):

        options = {"group": True, "stale": "ok", "reduce": True}
        # site of data should be relatively small (~1M) for put in the memory
        # If not, find a way to stream
        return self._getCouchView("outputByWorkflowName", options)


    def getFWJRWithSkippedFiles(self):
        options = {"reduce": True, "group": True, "include_docs": False}
        return self._getCouchView("skippedFileInfoByTaskAndSite", options)

