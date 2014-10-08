import time
import logging
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Lexicon import splitCouchServiceURL, sanitizeURL
from WMCore.Wrappers.JsonWrapper import JSONEncoder

class RequestDBReader():

    def __init__(self, couchURL, dbName = None, couchapp = "ReqMgr"):
        couchURL = sanitizeURL(couchURL)['url']
        # set the connection for local couchDB call
        self._commonInit(couchURL, dbName, couchapp)
        
    def _commonInit(self, couchURL, dbName, couchapp):
        """
        setting up comon variables for inherited class.
        inherited class should call this in their init function
        """
        if dbName:
            self.couchURL = couchURL
            self.dbName = dbName
        else:
            self.couchURL, self.dbName = splitCouchServiceURL(couchURL)
        self.couchServer = CouchServer(self.couchURL)
        self.couchDB = self.couchServer.connectDatabase(self.dbName, False)
        self.couchapp = couchapp
        self.defaultStale = {"stale": "update_after"}
        
    
    def setDefaultStaleOptions(self, options):
        if not options:
            options = {}  
        if not options.has_key('stale'):
            options.update(self.defaultStale)
        return options
    
    def _setNoStale(self):
        """
        Use this only for the unittest
        """        
        self.defaultStale = {}
    def _getCouchView(self, view, options, keys = []):
        
        options = self.setDefaultStaleOptions(options)
            
        if keys and type(keys) == str:
            keys = [keys]
        return self.couchDB.loadView(self.couchapp, view, options, keys)
            
        
    def _formatCouchData(self, data, key = "id"):
        result = {}
        for row in data['rows']:
            if row.has_key('error'):
                continue
            result[row[key]] = row["doc"]
        return result
            
    def _getRequestByNames(self, requestNames, detail = True):
        """
        'status': list of the status
        """
        options = {}
        options["include_docs"] = detail
        result = self.couchDB.allDocs(options, requestNames)
        return result
        
    def _getRequestByStatus(self, statusList, detail = True):
        """
        'status': list of the status
        """
        options = {}
        options["include_docs"] = detail
        keys = statusList
        return self._getCouchView("bystatus", options, keys)
  
    def _getAllDocsByIDs(self, ids, include_docs = True):
        """
        keys is [id, ....]
        returns document
        """
        if len(ids) == 0:
            return []
        options = {}
        options["include_docs"] =  include_docs
        result = self.couchDB.allDocs(options, ids)
        
        return result
    
    def getDBInstance(self):
        return self.couchDB
    
    
    def getRequestByNames(self, requestNames):
        if len(requestNames) == 0:
            return {}
        data = self._getRequestByNames(requestNames, True)

        requestInfo = self._formatCouchData(data)
        return requestInfo
    
    def getRequestByStatus(self, statusList):
        
        data = self._getRequestByStatus(statusList, False)
        result = []
        for row in data['rows']:
            if row.has_key('error'):
                continue
            result.append(row['id'])
        return result
    