#!/usr/bin/env python
"""Various helper functions for workqueue"""

__all__ = ['get_remote_queue', 'get_dbs', 'sitesFromStorageEelements',
           'queueConfigFromConfigObject', 'queueFromConfig']

import os
import logging

# Should probably import this but don't want to create the dependency
WMBS_REST_NAMESPACE = 'WMCore.HTTPFrontEnd.WMBS.WMBSRESTModel'
WWBS_MONITOR_NAMESPACE = 'WMCore.HTTPFrontEnd.WMBS.WMBSMonitorPage'

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
            sites = __sitedb.seToCMSName(se)
        except:
            print "Unable to get site name for %s" % se
        else:
            result.update(sites)
    return list(result)

__cmsSiteNames = []
def cmsSiteNames():
    """Get all cms sites"""
    global __cmsSiteNames
    if __cmsSiteNames:
        return __cmsSiteNames
    global __sitedb
    if not __sitedb:
        from WMCore.Services.SiteDB.SiteDB import SiteDBJSON as SiteDB
        __sitedb = SiteDB()
    try:
        __cmsSiteNames = __sitedb.getAllCMSNames()
    except:
        pass
    return __cmsSiteNames

def makeLocationsList(siteWhitelist, siteBlacklist):
    """
    _makeLocationsList_

    Make a location list based on the intersection between a site white list
    and blacklist, if none specified then all sites are listed.
    """
    sites = cmsSiteNames()
    if siteWhitelist:
        # Just get the CMS sites matching the whitelists
        sites = list(set(sites) & set(siteWhitelist))
    if siteBlacklist:
        # Get all CMS sites less the blacklist
        sites = list(set(sites) - set (siteBlacklist))
    return sites

def queueFromConfig(config):
    """Create a queue from the config object"""
    config = queueConfigFromConfigObject(config)
    if config.WorkQueueManager.level == 'GlobalQueue':
        from WMCore.WorkQueue.WorkQueue import globalQueue
        return globalQueue(**config.WorkQueueManager.queueParams)
    elif config.WorkQueueManager.level == 'LocalQueue':
        from WMCore.WorkQueue.WorkQueue import localQueue
        return localQueue(**config.WorkQueueManager.queueParams)
    else:
        from WMCore.WorkQueue.WorkQueue import WorkQueue
        return WorkQueue(**config.WorkQueueManager.queueParams)

def queueConfigFromConfigObject(config):
    """From a config object create a config dict suitable for a queue object"""
    from os import path
    wqManager = config.section_('WorkQueueManager')
    if not hasattr(wqManager, 'componentDir'):
        wqManager.componentDir = path.join(config.General.WorkDir,
                                           'WorkQueueManager')
    if not hasattr(wqManager, 'namespace'):
        wqManager.namespace = 'WMComponent.WorkQueueManager.WorkQueueManager'
    if not hasattr(wqManager, 'logLevel'):
        wqManager.logLevel = 'INFO'
    if not hasattr(wqManager, 'pollInterval'):
        wqManager.pollInterval = 600

    # WorkQueue config
    if not hasattr(wqManager, 'queueParams'):
        wqManager.queueParams = {}
    qConfig = wqManager.queueParams

    if hasattr(wqManager, 'couchurl'):
        wqManager.queueParams['CouchUrl'] = wqManager.couchurl
    if hasattr(wqManager, 'dbname'):
        wqManager.queueParams['DbName'] = wqManager.dbname
    if hasattr(wqManager, 'inboxDatabase'):
        wqManager.queueParams['InboxDbName'] = wqManager.inboxDatabase

    # pull some info we need from other areas of the config
    if not "BossAirConfig" in qConfig and hasattr(config, 'BossAir'):
        qConfig["BossAirConfig"] = config
        qConfig['BossAirConfig'].section_("Agent").agentName = config.Agent.agentName
    if not "JobDumpConfig" in qConfig and hasattr(config, 'JobStateMachine'):
        qConfig["JobDumpConfig"] = config
    if not "CacheDir" in qConfig and getattr(config.WorkQueueManager, 'componentDir', None):
        qConfig['CacheDir'] = os.path.join(config.WorkQueueManager.componentDir, 'cache')

    # alert api needs full agent config
    if hasattr(config, 'Alert'):
        qConfig['Config'] = config.Alert

    try:
        monitorURL = ''
        queueFlag = False
        for webapp in config.listWebapps_():
            webapp = config.webapp_(webapp)
            for page in webapp.section_('views').section_('active'):

                if not queueFlag and hasattr(page, "model") \
                   and page.section_('model').object == WMBS_REST_NAMESPACE:
                    qConfig['WMBSUrl'] = 'http://%s:%s/%s/%s' % (webapp.Webtools.host,
                                                              webapp.Webtools.port,
                                                              webapp._internal_name.lower(),
                                                              page._internal_name)
                    queueFlag = True

                if page.object == WWBS_MONITOR_NAMESPACE:
                    monitorURL = 'http://%s:%s/%s/%s' % (webapp.Webtools.host,
                                                      webapp.Webtools.port,
                                                      webapp._internal_name.lower(),
                                                      page._internal_name)
        #if not queueFlag:
        #    raise RuntimeError

    except RuntimeError:
        msg = """Unable to determine WMBS monitor URL, Either:
        Configure a WMBSRESTModel webapp_ section or,
        Add a WorkQueueManager.queueParams.WMBSUrl setting."""
        raise RuntimeError, msg

    if not qConfig.has_key('Teams') and hasattr(config.Agent, 'teamName'):
        qConfig['Teams'] = config.Agent.teamName
    if not 'logger' in qConfig:
        import threading
        myThread = threading.currentThread()
        if not hasattr(myThread, 'logger'):
            loggingLevelName = getattr(wqManager, 'logLevel', 'INFO')
            logging.basicConfig(format='%(asctime)-15s %(levelname)-8s %(module)s: %(message)s',
                                level = getattr(logging, loggingLevelName))
            myThread.logger = logging.getLogger('workqueue')
        qConfig['logger'] = myThread.logger

    # ReqMgr params
    if not hasattr(wqManager, 'reqMgrConfig'):
        wqManager.reqMgrConfig = {}

    return config
