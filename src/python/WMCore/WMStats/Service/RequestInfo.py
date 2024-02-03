"""
Hello world example using WMCore.REST handling framework.
Info class giving information about ReqMgr database.
Teams, Groups, Software versions handling for ReqMgr.

"""
import cherrypy
from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader

from WMCore.REST.Format import PrettyJSONFormat, PrettyJSONHTMLFormat, JSONFormat

class RequestInfo(RESTEntity):
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
        return


    @restcall(formats = [('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def get(self, request_name):
        result = self.wmstats.getRequestSummaryWithJobInfo(request_name)
        return rows([result])


class FinishedStatusInfo(RESTEntity):
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
        return


    @restcall(formats = [('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def get(self, request_name):

        try:
            result = self.wmstats.isWorkflowCompletedWithLogCollectAndCleanUp(request_name)
        except KeyError:
            raise cherrypy.HTTPError(404, "Cannot find request: %s" % request_name)

        return rows([result])

class JobDetailInfo(RESTEntity):
    """
    This class need to move under WMStats server when wmstats server created
    """
    def __init__(self, app, api, config, mount, t0flag=False):
        # main CouchDB database where requests/workloads are stored
        RESTEntity.__init__(self, app, api, config, mount)
        wmstats_url = "%s/%s" % (self.config.couch_host, self.config.couch_wmstats_db)
        reqdb_url = "%s/%s" % (self.config.couch_host, self.config.couch_reqmgr_db)
        if t0flag:
            couchAppName = "T0Request"
        else:
            couchAppName = "ReqMgr"
        self.wmstats = WMStatsReader(wmstats_url, reqdbURL=reqdb_url, reqdbCouchApp=couchAppName)

    def validate(self, apiobj, method, api, param, safe):
        args_length = len(param.args)
        if args_length == 1:
            safe.args.append(param.args[0])
            param.args.pop()

        prop = 'sample_size'
        safe.kwargs[prop] = int(param.kwargs.get(prop, 1))
        if prop in param.kwargs:
            del param.kwargs[prop]
        return


    @restcall(formats=[('text/plain', PrettyJSONFormat()), ('text/html', PrettyJSONHTMLFormat()), ('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def get(self, request_name, sample_size):

        result = self.wmstats.getTaskJobSummaryByRequest(request_name, sample_size, usePycurl=True)
        return rows([result])

class TeamInfo(RESTEntity):
    """
    This class need to move under WMStats server when wmstats server created
    """
    def __init__(self, app, api, config, mount):
        # main CouchDB database where requests/workloads are stored
        RESTEntity.__init__(self, app, api, config, mount)
        wmstats_url = "%s/%s" % (self.config.couch_host, self.config.couch_wmstats_db)
        self.wmstats = WMStatsReader(wmstats_url)

    def validate(self, apiobj, method, api, param, safe):
        args_length = len(param.args)
        if args_length == 1:
            safe.args.append(param.args[0])
            param.args.pop()
        return


    @restcall(formats = [('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def get(self):
        result = self.wmstats.agentsByTeam(filterDrain=False)
        return rows(result)
