"""
ReqMgr request handling.

"""

import time
import cherrypy
from datetime import datetime, timedelta

from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import validate_str

from WMCore.ReqMgr.Service.Auxiliary import ReqMgrBaseRestEntity
import WMCore.ReqMgr.Service.RegExp as rx

from RequestStatus import REQUEST_STATUS_LIST, REQUEST_STATUS_TRANSITION
from RequestType import REQUEST_TYPES


class Request(ReqMgrBaseRestEntity):
    def __init__(self, app, api, config, mount, db_handler):
        # main CouchDB database where requests/workloads are stored
        self.db_name = config.couch_reqmgr_db
        ReqMgrBaseRestEntity.__init__(self, app, api, config, mount, db_handler)

        
    def validate(self, apiobj, method, api, param, safe):
        validate_str("request_name", param, safe, rx.RX_REQUEST_NAME, optional=True)
        validate_str("all", param, safe, rx.RX_BOOL_FLAG, optional=True)
        
    
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
            if all == "false":
                past_days = self.config.default_view_requests_since_num_days
                current_date = list(time.gmtime()[:6])
                from_date = datetime(*current_date) - timedelta(days=past_days)
                options["endkey"] = list(from_date.timetuple()[:6])
            request_docs = self.db_handler.view(self.db_name,
                                                "ReqMgr", "bydate",
                                                options=options)
            return rows([request_docs])
        
        
        
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