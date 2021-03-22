#!/usr/bin/env python
"""
_PluginInterface_

Base class for AnalyticsDataCollector plug-ins

"""

from builtins import object
class PluginInterface(object):
    """Interface for policies"""
    def __init__(self, **args):
        """ initialize args if needed"""
        pass

    def __call__(self, requestDocs, localSummaryCouchDB, centralRequestCouchDB):
        """
        this needs to be overwritten
        requestDocs is request info collected from other resources, which has couchdb doc format.
        localSummaryCouchDB, centralWMStatsCouchDB are WMCore.Services.WMStats.WMStatsWriter instances
        for local wmagent_summary db and central wmstats couchdb respectfully.
        If data needed to be written to wmstats, write to local it will be replicated,
        Only in case data doesn't exist in local, write to central wmstats couchdb.
        requestDocs can be modified and it will be updated outside the plug in,
        So don't push requestDocs to couchdb directly here.
        """
        msg = "%s.__call__ is not implemented" % self.__class__.__name__
        raise NotImplementedError(msg)
