"""
ReqMgr request handling.

"""

import time
import cherrypy
from datetime import datetime, timedelta

from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import validate_str, validate_strlist

import WMCore.ReqMgr.Service.RegExp as rx

from RequestStatus import REQUEST_STATUS_LIST, REQUEST_STATUS_TRANSITION
from RequestType import REQUEST_TYPES


class Request(RESTEntity):
    def __init__(self, app, api, config, mount, db_handler):
        # main CouchDB database where requests/workloads are stored
        RESTEntity.__init__(self, app, api, config, mount)
        self.reqmgrDB = api.db_handler.get_db(config.couch_reqmgr_db)
    
    def validate(self, apiobj, method, api, param, safe):
        # to make validate successful
        # move the validated argument to safe
        # make param empty
        # other wise raise the error 
        
        if method in ['GET', 'PUT', 'POST']:
            for prop in param.kwargs:
                safe.kwargs[prop] = param.kwargs[prop]
            
            for prop in safe.kwargs:
                del param.kwargs[prop]
#             
#             permittedParams = ["statusList", "names", "type", "prepID", "inputDataset", 
#                                "outputDataset", "dateRange", "campaign", "workqueue", "team"]
#             validate_strlist("statusList", param, safe, '*')
#             validate_strlist("names", param, safe, rx.RX_REQUEST_NAME)
#             validate_str("type", param, safe, "*", optional=True)
#             validate_str("prepID", param, safe, "*", optional=True)
#             validate_str("inputDataset", param, safe, rx.RX_REQUEST_NAME, optional=True)
#             validate_str("outputDataset", param, safe, rx.RX_REQUEST_NAME, optional=True)
#             validate_strlist("dateRagne", param, safe, rx.RX_REQUEST_NAME)
#             validate_str("campaign", param, safe, "*", optional=True)
#             validate_str("workqueue", param, safe, "*", optional=True)
#             validate_str("team", param, safe, "*", optional=True)
            
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
        result = couchdb.loadView(couchapp, view, options, keys)

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
        
        return self._getStaleView(self.reqmgrDB, "ReqMgr", view, options, keys, format)
    
    
    def _getWMStatsView(self, view, options, keys, format):
        
        return self._getStaleView(self.wmstatsCouch, "WMStats", view, options, keys, format)
       
    
    def _getReuestsByNames(self, names, stale="update_after"):
        """
        TODO: names can be regular expression or list of names
        """
        request_doc = self.reqmgrDB.document(self.db_name, names)
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
    def post(self, **kwargs):
        result = self.reqmgrDB.updateDocument(doc['_id'], 'WMStats',
                                    'insertRequest',
                                    fields={'doc': JSONEncoder().encode(doc)})
        
class RequestStatus(RESTEntity):
    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)


    def validate(self, apiobj, method, api, param, safe):
        validate_str("transition", param, safe, rx.RX_BOOL_FLAG, optional=True)
    
    
    @restcall
    def get(self, transition):
        """
        Return list of allowed request status.
        If transition, return exhaustive list with all request status
        and their defined transitions.
        
        """
        if transition == "true":
            return rows(REQUEST_STATUS_TRANSITION)
        else:
            return rows(REQUEST_STATUS_LIST)
    
    
    
class RequestType(RESTEntity):
    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)
    
    
    def validate(self, apiobj, method, api, param, safe):
        pass
    
    
    @restcall
    def get(self):
        return rows(REQUEST_TYPES)
