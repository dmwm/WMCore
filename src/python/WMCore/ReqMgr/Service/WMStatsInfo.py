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
from WMCore.REST.Format import JSONFormat
from WMCore.REST.Error import MethodWithoutQueryString


class WMStatsInfo(RESTEntity):
    """
    This class need to move under WMStats server when wmstats server created
    """
    def __init__(self, app, api, config, mount):
        # main CouchDB database where requests/workloads are stored
        RESTEntity.__init__(self, app, api, config, mount)
        wmstats_url = "%s/%s" % (self.config.couch_host, self.config.couch_wmstats_db)
        reqdb_url = "%s/%s" % (self.config.couch_host, self.config.couch_reqmgr_db)
        self.wmstats = WMStatsReader(wmstats_url, reqdbURL=reqdb_url, reqdbCouchApp="ReqMgr")

    def validate(self, apiobj, method, api, param, safe):
        args_length = len(param.args)
        if args_length == 1:
            safe.args.append(param.args[0])
            param.args.pop()
        else:
            raise MethodWithoutQueryString
        return


    @restcall(formats = [('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def get(self, request_name):
        result = self.wmstats.getRequestSummaryWithJobInfo(request_name)
        return rows([result])



