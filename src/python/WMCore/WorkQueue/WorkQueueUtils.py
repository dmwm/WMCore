#!/usr/bin/env python
"""Various helper functions for workqueue"""

__all__ = ['get_remote_queue', 'get_dbs', 'sitesFromStorageEelements']

__queues = {}
def get_remote_queue(queue, logger):
    """
    Get an object to talk to a remote queue
    """
    from WMCore.WorkQueue.WorkQueue import WorkQueue
    # tests generally get the queue object passed in direct
    if isinstance(queue, WorkQueue):
        return queue
    try:
        return __queues[queue]
    except KeyError:
        from WMCore.Services.WorkQueue.WorkQueue import WorkQueue as WorkQueueDS
        __queues[queue] = WorkQueueDS({'endpoint' : queue, 'logger' : logger})
        return __queues[queue]

__dbses = {}
def get_dbs(url):
    """Return DBS object for url"""
    try:
        return __dbses[url]
    except KeyError:
        from WMCore.Services.DBS.DBSReader import DBSReader
        __dbses[url] = DBSReader(url)
        return __dbses[url]

__sitedb = None
def sitesFromStorageEelements(ses):
    """Return Sites given Storage Elements"""
    global __sitedb
    if not __sitedb:
        from WMCore.Services.SiteDB.SiteDB import SiteDBJSON as SiteDB
        __sitedb = SiteDB()
    result = set()
    for se in ses:
        try:
            site = __sitedb.seToCMSName(se)
        except:
            print "Unable to get site name for %s" % se
        else:
            result.add(site)
    return list(result)