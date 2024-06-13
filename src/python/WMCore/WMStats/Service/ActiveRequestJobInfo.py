"""
Gets the cache data from server cache. This shouldn't update the server cache.
Just wait for the server cache to be updated
"""
from __future__ import (division, print_function)
from memory_profiler import profile
from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Error import DataCacheEmpty
from WMCore.WMStats.DataStructs.DataCache import DataCache
from WMCore.REST.Format import JSONFormat, PrettyJSONFormat
from WMCore.ReqMgr.DataStructs.RequestStatus import ACTIVE_STATUS_FILTER
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader


def updateCache():
    try:
        print("This worked")
        wmstatsDB = WMStatsReader(wmstats_url = "https://cmsweb-test9.cern.ch/couchdb/wmstats", reqmgrdb_url = "https://cmsweb-test9.cern.ch/couchdb/reqmgr_workload_cache",
                                          reqdbCouchApp="ReqMgr", logger=self.logger)
        print("This worked two")
        jobData = wmstatsDB.getActiveData(WMSTATS_JOB_INFO, jobInfoFlag=self.getJobInfo)
        print("This worked three")
        tempData = wmstatsDB.getActiveData(WMSTATS_NO_JOB_INFO, jobInfoFlag=False)
        print("This worked four")
        jobData.update(tempData)
        print("This worked five")
        return jobData
    except Exception as ex:
        print("Something went wrong")

class ActiveRequestJobInfo(RESTEntity):
    """
    get all the active requests with job information attatched
    """

    def __init__(self, app, api, config, mount):
        # main CouchDB database where requests/workloads are stored
        RESTEntity.__init__(self, app, api, config, mount)
    
    def validate(self, apiobj, method, api, param, safe):
        return

    @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    @profile
    def get(self):
        # This assumes DataCahe is periodically updated.
        # If data is not updated, need to check, dataCacheUpdate log
        x = DataCache()
        x.setlatestJobData(updateCache())
        return rows([x.getlatestJobData()])


class FilteredActiveRequestJobInfo(RESTEntity):
    """
    get all the active requests with job information attatched
    """

    def __init__(self, app, api, config, mount):
        # main CouchDB database where requests/workloads are stored
        RESTEntity.__init__(self, app, api, config, mount)

    def validate(self, apiobj, method, api, param, safe):

        for prop in param.kwargs:
            safe.kwargs[prop] = param.kwargs[prop]

        for prop in safe.kwargs:
            del param.kwargs[prop]

        return
    @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    @profile
    def get(self, mask=None, **input_condition):
        # This assumes DataCahe is periodically updated.
        # If data is not updated, need to check, dataCacheUpdate log
        x = DataCache()
        x.setlatestJobData(updateCache())
        return rows(x.filterDataByRequest(input_condition, mask))


class ProtectedLFNList(RESTEntity):
    """
    API which provides a list of ALL possible unmerged LFN bases (including
    transient datasets/LFNs).
    """

    def __init__(self, app, api, config, mount):
        # main CouchDB database where requests/workloads are stored
        RESTEntity.__init__(self, app, api, config, mount)

    def validate(self, apiobj, method, api, param, safe):
        return

    @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    @profile
    def get(self):
        x = DataCache()
        x.setlatestJobData(updateCache())
        # This assumes DataCahe is periodically updated.
        # If data is not updated, need to check, dataCacheUpdate log
        if x.isEmpty():
            raise DataCacheEmpty()
        else:
            return rows(x.filterData(ACTIVE_STATUS_FILTER, ["OutputModulesLFNBases"]))


class ProtectedLFNListOnlyFinalOutput(RESTEntity):
    """
    Same as ProtectedLFNList API, however this one only provides LFNs that are not
    transient, so only final output LFNs.
    """

    def __init__(self, app, api, config, mount):
        # main CouchDB database where requests/workloads are stored
        RESTEntity.__init__(self, app, api, config, mount)

    def validate(self, apiobj, method, api, param, safe):
        return
  
    @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    @profile
    def get(self):
        # This assumes DataCahe is periodically updated.
        # If data is not updated, need to check, dataCacheUpdate log
        x = DataCache()
        x.setlatestJobData(updateCache())
        return rows(x.getProtectedLFNs())


class GlobalLockList(RESTEntity):
    def __init__(self, app, api, config, mount):
        # main CouchDB database where requests/workloads are stored
        RESTEntity.__init__(self, app, api, config, mount)

    def validate(self, apiobj, method, api, param, safe):
        return
 
    @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    @profile
    def get(self):
        x = DataCache()
        x.setlatestJobData(updateCache())
        # This assumes DataCahe is periodically updated.
        # If data is not updated, need to check, dataCacheUpdate log
        if x.isEmpty():
            raise DataCacheEmpty()
        else:
            return rows(x.filterData(ACTIVE_STATUS_FILTER,
                                             ["InputDataset", "OutputDatasets", "MCPileup", "DataPileup"]))

