"""
ReqMgr request handling.

"""

import time
import cherrypy
from datetime import datetime, timedelta

from WMCore.Database.CMSCouch import CouchServer, Database, Document, CouchError
from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import validate_str

import WMCore.ReqMgr.service.regexp as rx


class Request(RESTEntity):    
    def __init__(self, app, api, config, mount, db_pool):
        self.db_pool = db_pool
        self.config = config
        RESTEntity.__init__(self, app, api, config, mount)
        
        
    def validate(self, apiobj, method, api, param, safe):
        validate_str("request_name", param, safe, rx.RX_REQUEST_NAME, optional=True)
        validate_str("all", param, safe, rx.RX_BOOL_FLAG, optional=True)
        
    
    @restcall
    @tools.expires(secs=-1)
    def get(self, request_name, all):
        """
        Returns most recent list of requests in the system.
        Query particular request if request_name is specified.
        Return complete list of all requests in the system if all is set.
            If all is not set, check "default_view_requests_since_num_days"
            config value and show only requests not older than this
            number of days.
        
        """
        couchdb = self.db_pool.reqmgr_couchdb
        if request_name:
            try:
                request_doc = couchdb.document(request_name)
            except CouchError, ex:
                msg = ("ERROR: Query of '%s' request failed, reason: %s" %
                       (request_name, ex))
                cherrypy.log(msg)
                raise cherrypy.HTTPError(400, msg)
            return rows([request_doc])
        else:
            options = {"descending": True}
            if not all:
                past_days = self.config.default_view_requests_since_num_days
                current_date = list(time.gmtime()[:6])
                from_date = datetime(*current_date) - timedelta(days=past_days)
                options["endkey"] = list(from_date.timetuple()[:6])
            request_docs = couchdb.loadView("ReqMgr", "bydate", options=options)
            return rows([request_docs])
            
    # TODO
    # other methods put(), post() will be used to modify and create requests        