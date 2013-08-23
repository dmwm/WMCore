"""
ReqMgr request handling.

"""

import time
import cherrypy
from datetime import datetime, timedelta

import WMCore.Lexicon
from WMCore.Database.CMSCouch import CouchError
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.WMSpec.StdSpecs.StdBase import WMSpecFactoryException
from WMCore.WMSpec.WMWorkloadTools import loadSpecByType
from WMCore.Wrappers import JsonWrapper

from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Auth import authz_match
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import validate_str, validate_strlist

import WMCore.ReqMgr.Service.RegExp as rx
from WMCore.ReqMgr.Auth import getWritePermission
from WMCore.ReqMgr.DataStructs.Request import initialize_request_args, generateRequestName
from WMCore.ReqMgr.DataStructs.RequestStatus import REQUEST_STATE_LIST, check_allowed_transition
from WMCore.ReqMgr.DataStructs.RequestStatus import REQUEST_STATE_TRANSITION
from WMCore.ReqMgr.DataStructs.RequestType import REQUEST_TYPES
from WMCore.ReqMgr.DataStructs.RequestError import InvalidStateTransition


class Request(RESTEntity):
    def __init__(self, app, api, config, mount):
        # main CouchDB database where requests/workloads are stored
        RESTEntity.__init__(self, app, api, config, mount)
        self.reqmgr_db = api.db_handler.get_db(config.couch_reqmgr_db)
        # this need for the post validtiaon 
        self.reqmgr_aux_db = api.db_handler.get_db(config.couch_reqmgr_aux_db)
        
    def validate(self, apiobj, method, api, param, safe):
        # to make validate successful
        # move the validated argument to safe
        # make param empty
        # other wise raise the error 
        
        if method in ['GET']:
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
            
        TODO:
        stuff like this has to filtered out from result of this call:
            _attachments: {u'spec': {u'stub': True, u'length': 51712, u'revpos': 2, u'content_type': u'application/json'}}
            _id: maxa_RequestString-OVERRIDE-ME_130621_174227_9225
            _rev: 4-c6ceb2737793aaeac3f1cdf591593da4        

        """
        if len(kwargs) == 0:
            kwargs['status'] = "running"
            options = {"descending": True, 'include_docs': True, 'limit': 200}
            request_docs = self.reqmgr_db.loadView("ReqMgr", "bystatus", options)
            return rows([request_docs])
            
        # list of status
        status = kwargs.get("status", False)
        # list of request names
        name = kwargs.get("name", False)
        type = kwargs.get("type", False)
        prep_id = kwargs.get("prep_id", False)
        inputdataset = kwargs.get("inputdataset", False)
        outputdataset = kwargs.get("outputdataset", False)
        date_range = kwargs.get("date_range", False)
        campaign = kwargs.get("campaign", False)
        workqueue = kwargs.get("workqueue", False)
        team = kwargs.get("team", False)
        
        # eventhing should be stale view. this only needs for test
        _nostale = kwargs.get("_nostale", False)
        option = {}
        if not _nostale:
            option['stale'] = "update_after"
            
        request_info =[]
        
        if status and not team:
            request_info.append(self.get_reqmgr_view("bystatus" , option, status))
        if status and team:
            request_info.append(self.get_reqmgr_view("byteamandstatus", option, [[team, status]]))
        if name:
            request_info.append(self._get_request_by_name(name))
        if prep_id:
            request_info.append(self.get_reqmgr_view("byprepid", option, prep_id))
        if inputdataset:
            request_info.append(self.get_reqmgr_view("byinputdataset", option, inputdataset))
        if outputdataset:
            request_info.append(self.get_reqmgr_view("byoutputdataset", option, outputdataset))
        if date_range:
            request_info.append(self.get_reqmgr_view("bydate", option, date_range))
        if campaign:
            request_info.append(self.get_reqmgr_view("bycampaign", option, campaign))
        if workqueue:
            request_info.append(self.get_reqmgr_view("byworkqueue", option, workqueue))
        
        #get interaction of the request
        result = self._intersection_of_request_info(request_info);
        return [result]
        
    def _intersection_of_request_info(self, request_info):
        requests = {}
        if len(request_info) < 1:
            return requests
         
        request_key_set = set(request_info[0].keys())
        for info in request_info:
            request_key_set = set(request_key_set) & set(info.keys())
        #TODO: need to assume some data maight not contains include docs
        for request_name in request_key_set:
            requests[request_name] = request_info[0][request_name]
        return requests    
        
    def _get_couch_view(self, couchdb, couchapp, view, options, keys):
        
        if not options:
            options = {}
        options.setdefault("include_docs", True)
        if type(keys) == str:
            keys = [keys]
        result = couchdb.loadView(couchapp, view, options, keys)
        
        request_info = {}
        for item in result["rows"]:
            request_info[item["id"]] = item.get('doc', None)
            if request_info[item["id"]] != None:
                self.filterCouchInfo(request_info[item["id"]])
        return request_info
    
    
    #TODO move this out of this class
    def filterCouchInfo(self, couchInfo):
        del couchInfo["_rev"]
        del couchInfo["_id"]
        del couchInfo["_attachments"]
                
    def get_reqmgr_view(self, view, options, keys):
        return self._get_couch_view(self.reqmgr_db, "ReqMgr", view,
                                    options, keys)
    
    
    def get_wmstats_view(self, view, options, keys):
        return self._get_couch_view(self.wmstatsCouch, "WMStats", view,
                                    options, keys)
    
    def _get_request_by_name(self, name, stale="update_after"):
        """
        TODO: names can be regular expression or list of names
        """
        request_doc = self.reqmgr_db.document(name)
        self.filterCouchInfo(request_doc)
        return {name: request_doc}
        
    def _combine_request(self, request_info, requestAgentUrl, cache):
        keys = {}
        requestAgentUrlList = []
        for row in requestAgentUrl["rows"]:
            request = row["key"][0]
            if not keys[request]: 
                keys[request] = []
            keys[request].append(row["key"][1])

        for request in request_info: 
            for agentUrl in keys[request]: 
                requestAgentUrlList.append([request, agentUrl]);

        return requestAgentUrlList;

    @restcall
    def put(self, workload, request_args):
        
        if workload == None:
            (workload, request_args) = self.initialize_clone(request_args["OriginalRequestName"])
            return self.post(workload, request_args)
        
        # if is not just updating status
        if len(request_args) > 1 or not request_args.has_key("RequestStatus"):
            workload.updateArguments(request_args)
            # trailing / is needed for the savecouchUrl function
            workload.saveCouch(self.config.couch_host, self.config.couch_reqmgr_db)
        
        report = self.reqmgr_db.updateDocument(workload.name(), "ReqMgr", "updaterequest",
                                             fields=request_args)
        return report 
    
    @restcall
    def delete(self, request_name):
        cherrypy.log("INFO: Deleting request document '%s' ..." % request_name)
        try:
            self.reqmgr_db.delete_doc(request_name)
        except CouchError, ex:
            msg = "ERROR: Delete failed."
            cherrypy.log(msg + " Reason: %s" % ex)
            raise cherrypy.HTTPError(404, msg)        
        # TODO
        # delete should also happen on WMStats
        cherrypy.log("INFO: Delete '%s' done." % request_name)
        
    
    @restcall
    def post(self, workload, request_args):
        """
        Create and update couchDB with  a new request. 
        request argument is passed from validation 
        (validation convert cherrypy.request.body data to argument)
                        
        TODO:
        this method will have some parts factored out so that e.g. clone call
        can share functionality.
        
        NOTES:
        1) do not strip spaces, #4705 will fails upon injection with spaces ; 
            currently the chain relies on a number of things coming in #4705
        
        2) reqInputArgs = Utilities.unidecode(JsonWrapper.loads(body))
            (from ReqMgrRESTModel.putRequest)
                
        """
        cherrypy.log("INFO: Create request, input args: %s ..." % request_args)
        
        # storing the request document into Couch

        workload.saveCouch(request_args["CouchURL"], request_args["CouchWorkloadDBName"],
                           metadata=request_args)
        
        #TODO should return something else instead on whole schema
        return [request_args]
        

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
            return rows(REQUEST_STATE_TRANSITION)
        else:
            return rows(REQUEST_STATE_LIST)
    
    
    
class RequestType(RESTEntity):
    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)
    
    
    def validate(self, apiobj, method, api, param, safe):
        pass
    
    
    @restcall
    def get(self):
        return rows(REQUEST_TYPES)
