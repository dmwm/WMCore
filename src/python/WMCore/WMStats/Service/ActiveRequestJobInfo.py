"""
Gets the cache data from server cache. This shouldn't update the server cache.
Just wait for the server cache to be updated
"""
from __future__ import (division, print_function)
from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Error import DataCacheEmpty
from WMCore.WMStats.DataStructs.DataCache import DataCache
from WMCore.REST.Format import JSONFormat, PrettyJSONFormat
from WMCore.ReqMgr.DataStructs.RequestStatus import ACTIVE_STATUS_FILTER


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
    def get(self):
        # This assumes DataCahe is periodically updated.
        # If data is not updated, need to check, dataCacheUpdate log
        return rows([DataCache.getlatestJobData()])


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
    def get(self, mask=None, **input_condition):
        # This assumes DataCahe is periodically updated.
        # If data is not updated, need to check, dataCacheUpdate log
        return rows(DataCache.filterDataByRequest(input_condition, mask))


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
    def get(self):
        # This assumes DataCahe is periodically updated.
        # If data is not updated, need to check, dataCacheUpdate log
        if DataCache.isEmpty():
            raise DataCacheEmpty()
        else:
            return rows(DataCache.filterData(ACTIVE_STATUS_FILTER, ["OutputModulesLFNBases"]))


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
    def get(self):
        # This assumes DataCahe is periodically updated.
        # If data is not updated, need to check, dataCacheUpdate log
        return rows(DataCache.getProtectedLFNs())


class GlobalLockList(RESTEntity):
    def __init__(self, app, api, config, mount):
        # main CouchDB database where requests/workloads are stored
        RESTEntity.__init__(self, app, api, config, mount)

    def validate(self, apiobj, method, api, param, safe):
        return

    @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def get(self):
        # This assumes DataCahe is periodically updated.
        # If data is not updated, need to check, dataCacheUpdate log
        if DataCache.isEmpty():
            raise DataCacheEmpty()
        else:
            return rows(DataCache.filterData(ACTIVE_STATUS_FILTER,
                                             ["InputDataset", "OutputDatasets", "MCPileup", "DataPileup"]))
