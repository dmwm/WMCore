"""
ReqMgr request handling.

"""

import time
import cherrypy
from datetime import datetime, timedelta

from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import validate_str, validate_strlist


class Requests(RESTEntity):
    
    def __init__(self, app, api, config, mount, db_handler):
        # main CouchDB database where requests/workloads are stored
        self.reqmgrCouch = config.reqmgrCouchURL
        self.wmstatsCouch = config.wmstatsCouchURL
        
    def validate(self, apiobj, method, api, param, safe):
        if method in ['GET']:
            permittedParams = ["statusList", "names", "type", "prepID", "inputDataset", "outputDataset", "dateRange"]
            validate_strlist("statusList", param, safe, rx.RX_REQUEST_NAME, optional=True)
            validate_strlist("names", param, safe, rx.RX_REQUEST_NAME, optional=True)
            validate_str("type", param, safe, rx.RX_REQUEST_NAME, optional=True)
            validate_str("prepID", param, safe, rx.RX_REQUEST_NAME, optional=True)
            validate_str("inputDataset", param, safe, rx.RX_REQUEST_NAME, optional=True)
            validate_str("outputDataset", param, safe, rx.RX_REQUEST_NAME, optional=True)
            validate_strlist("dateRagne", param, safe, rx.RX_REQUEST_NAME, optional=True)
            
    @restcall
    def get(self, jobInfo=False, **kwargs):
        """
        Returns request info depending on the conditions set by kwargs
        Currently defined kwargs are following.
        statusList, requestNames, requestType, prepID, inputDataset, outputDataset, dateRange
        If jobInfo is True, returns jobInfomation about the request as well.
        """
        statusList = kwargs.get("statusList", False)
        names = kwargs.get("names", False)
        type = kwargs.get("type", False)
        prepID = kwargs.get("prepID", False)
        inputDataset = kwargs.get("inputDataset", False)
        outputDataset = kwargs.get("outputDataset", False)
        dateRange = kwargs.get("dateRange", False)

        
        if statusList:
            requestInfo = self._getReqMgrView("bystatus" , {}, statusList, "list")
        if names:
            requestInfo = self._getReuestsByNames(names)
        if prepID:
            requestInfo = self._getReqMgrView("byprepid", {}, prepID, "list")
        if inputDataset:
            requestInfo = self._getReqMgrView("byinputdataset", {}, inputDataset, "list")
        if outputDataset:
            requestInfo = self._getReqMgrView("byoutputdataset", {}, outputDataset, "list")
        if dateRange:
            requestInfo = self._getReqMgrView("bydate", {}, dateRange, "list")
        if jobInfo:
            requestWithJobInfo = self._getJobInfo(requestInfo, cache = True)
    
    def _getStaleView(self, couchdb, couchapp, view, options, keys, format):
        
        if not options:
            options = {}
        options["stale"] = "update_after"
        options["include_docs"] = True
        result = couchdb.loadView("ReqMgr", view, options, keys)

        if format == "dict":
            requestInfo = {}
            for item in result["rows"]:
                requestInfo[item["id"]] = None
            return requestInfo
        else:
            requestInfo = []
            for item in result["rows"]:
                requestInfo.append(item["id"])
            return requestInfo
        
    
    def _getReqMgrView(self, view, options, keys, format):
        
        return self._getStaleView(self.reqMgrCouch, "ReqMgr", view, options, keys, format)
    
    
    def _getWMStatsView(self, view, options, keys, format):
        
        return self._getStaleView(self.wmstatsCouch, "WMStats", view, options, keys, format)
       
    
    def _getReuestsByNames(self, names, stale="update_after"):
        """
        TODO: names can be regular expression or list of names
        """
        return requestInfo
    
    def _combineRequest(self, requestInfo, requestAgentUrl, cache):
        keys = {}
        requestAgentUrlList = []
        for row in requestAgentUrl["rows"]:
            request = row["key"][0]
            if not keys[request]: 
                keys[request] = []
            keys[request].append(row["key"][1])
        
        for request in requestInfo: 
            for agentUrl in keys[request]: 
                requestAgentUrlList.append([request, agentUrl]);

        return requestAgentUrlList;
    
    def _getActiveRequestJobInfo(self, requestInfo, cache = False):
        """
        always use server cache
        1. match with the data with cache.
        2. if data is not in cache
        3. retrieve only that data and up date cache.
        4. remove requests from the cache if requestInfo doesn't contain the information
        5. don't update cache time (only should be updated when whole cache is refreshed)
        """
        
        options = {"reduce": True, "group": True, "descending": True}

        requestAgentUrl = self._getWMStatsView('requestAgentUrl', options, [], "list")
        assembleRequestKeys = self._combineRequest(requestInfo, requestAgentUrl, cache)
        requestInfo = self._getWMStatsView('latestRequest', options, assembleRequestKeys, "list")
        # update cache
        return requestInfo
    