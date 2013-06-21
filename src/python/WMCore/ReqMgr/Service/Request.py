"""
ReqMgr request handling.

"""

import time
import cherrypy
from datetime import datetime, timedelta

from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import validate_str, validate_strlist

from WMCore.ReqMgr.Service.Auxiliary import ReqMgrBaseRestEntity
import WMCore.ReqMgr.Service.RegExp as rx


class Request(ReqMgrBaseRestEntity):
    def __init__(self, app, api, config, mount, db_handler):
        # main CouchDB database where requests/workloads are stored
        self.db_name = config.couch_reqmgr_db
        ReqMgrBaseRestEntity.__init__(self, app, api, config, mount, db_handler)

    def validate(self, apiobj, method, api, param, safe):
        if method in ['GET']:
            permittedParams = ["statusList", "names", "type", "prepID", "inputDataset", 
                               "outputDataset", "dateRange", "campaign", "workqueue", "team"]
            validate_strlist("statusList", param, safe, '*', optional=True)
            validate_strlist("names", param, safe, rx.RX_REQUEST_NAME, optional=True)
            validate_str("type", param, safe, "*", optional=True)
            validate_str("prepID", param, safe, "*", optional=True)
            validate_str("inputDataset", param, safe, rx.RX_REQUEST_NAME, optional=True)
            validate_str("outputDataset", param, safe, rx.RX_REQUEST_NAME, optional=True)
            validate_strlist("dateRagne", param, safe, rx.RX_REQUEST_NAME, optional=True)
            validate_str("campaign", param, safe, "*", optional=True)
            validate_str("workqueue", param, safe, "*", optional=True)
            validate_str("team", param, safe, "*", optional=True)
            
    @restcall
    def get(self, **kwargs):
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
        campaign = kwargs.get("campaign", False)
        workqueue = kwargs.get("workqueue", False)
        team = kwargs.get("team", False)

        requestInfo =[]
        
        if statusList and not team:
            requestInfo[1] = self._getReqMgrView("bystatus" , {}, statusList, "list")
        if statusList and team:
            requestInfo[2] = self._getReqMgrView("byteamandstatus", {}, team, "list")
        if names:
            requestInfo[3] = self._getReuestsByNames(names)
        if prepID:
            requestInfo[4] = self._getReqMgrView("byprepid", {}, prepID, "list")
        if inputDataset:
            requestInfo[5] = self._getReqMgrView("byinputdataset", {}, inputDataset, "list")
        if outputDataset:
            requestInfo[6] = self._getReqMgrView("byoutputdataset", {}, outputDataset, "list")
        if dateRange:
            requestInfo[7] = self._getReqMgrView("bydate", {}, dateRange, "list")
        if campaign:
            requestInfo[8] = self._getReqMgrView("bycampaign", {}, campaign, "list")
        if workqueue:
            requestInfo[9] = self._getReqMgrView("byworkqueue", {}, workqueue, "list")
        
        #get interaction of the request
        return self._intersectionOfRequestInfo(requestInfo);
        
    def _intersectionOfRequestInfo(self, requestInfo):
        return requestInfo[0]    
        
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
        request_doc = self.db_handler.document(self.db_name, names)
        return rows([request_doc])
        
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
    

    @restcall
    def get(self, request_name, all):
        """
        Returns most recent list of requests in the system.
        Query particular request if request_name is specified.
        Return complete list of all requests in the system if all is set.
            If all is not set, check "default_view_requests_since_num_days"
            config value and show only requests not older than this
            number of days.
        
        """
        if request_name:
            request_doc = self.db_handler.document(self.db_name, request_name)
            return rows([request_doc])
        else:
            options = {"descending": True}
            if not all:
                past_days = self.config.default_view_requests_since_num_days
                current_date = list(time.gmtime()[:6])
                from_date = datetime(*current_date) - timedelta(days=past_days)
                options["endkey"] = list(from_date.timetuple()[:6])
            request_docs = self.db_handler.view(self.db_name,
                                                "ReqMgr", "bydate",
                                                options=options)
            return rows([request_docs])