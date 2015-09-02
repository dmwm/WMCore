"""
Hello world example using WMCore.REST handling framework.
Info class giving information about ReqMgr database.
Teams, Groups, Software versions handling for ReqMgr.

"""

import logging
import cherrypy

from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader
from WMCore.WMStats.DataStructs.DataCache import DataCache

from WMCore.REST.Format import JSONFormat

class ActiveRequestJobInfo(RESTEntity):
    """
    This class need to move under WMStats server when wmstats server created
    """
    def __init__(self, app, api, config, mount):
        # main CouchDB database where requests/workloads are stored
        RESTEntity.__init__(self, app, api, config, mount)  
        wmstats_url = "%s/%s" % (self.config.couch_host, self.config.couch_wmstats_db)
        reqdb_url = "%s/%s" % (self.config.couch_host, self.config.couch_reqmgr_db)
        self.wmstats = WMStatsReader(wmstats_url, reqdb_url, reqdbCouchApp = "ReqMgr")             
        
    def validate(self, apiobj, method, api, param, safe):
        args_length = len(param.args)
        return            

    
    @restcall(formats = [('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def get(self):
        # This assumes DataCahe is periodically updated. 
        # If data is not updated, need to check, dataCacheUpdate log
        return rows([DataCache.getlatestJobData()])