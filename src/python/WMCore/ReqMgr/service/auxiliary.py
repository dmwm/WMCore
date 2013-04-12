"""
Hello world example using WMCore.REST handling framework.

"""

import re

import WMCore
from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import validate_str


class HelloWorld(RESTEntity):
    
    def validate(self, apiobj, method, api, param, safe):
        """
        Validate request input data.
        Has to be implemented, otherwise the service fails to start.
        If it's not implemented correctly (e.g. just pass), the arguments
        are not passed in the method at all.
        
        """
        RX_NAME = re.compile(r"[A-Za-z0-9]")
        validate_str("name", param, safe, RX_NAME, optional=True)


    @restcall
    @tools.expires(secs=-1)
    def get(self, name):
        """
        Hello world API call.
        
        :arg str name: name to appear in the result message.
        :returns: row with response, here 1 item list with message.
        
        """
        msg = "Hello "
        msg += name or "world"
        #return rows(["Hello: world"]) returns the same as above
        return msg



class Info(RESTEntity):
    
    def __init__(self, app, api, config, mount, db_pool):
        self.db_pool = db_pool
        self.config = config
        RESTEntity.__init__(self, app, api, config, mount)
        
    def validate(self, apiobj, method, api, param, safe):
        pass

    
    @restcall
    @tools.expires(secs=-1)
    def get(self):
        wmcore_reqmgr_version = WMCore.__version__
        
        couchdb = self.db_pool.reqmgr_couchdb
        reqmgr_db_info = couchdb.info()
        reqmgr_db_info["reqmgr_couch_url"] = self.config.couch_host 
        
        # retrieve the last injected request in the system
        # curl ... /reqmgr_workload_cache/_design/ReqMgr/_view/bydate?descending=true&limit=1
        options = {"descending": True, "limit": 1} 
        reqmgr_last_injected_request = couchdb.loadView("ReqMgr",
                                                        "bydate",
                                                         options=options)
        result = {"wmcore_reqmgr_version": wmcore_reqmgr_version,
                  "reqmgr_db_info": reqmgr_db_info,
                  "reqmgr_last_injected_request": reqmgr_last_injected_request}
        # NOTE:
        # "return result" only would return dict with keys without (!!) values set
        return rows([result])