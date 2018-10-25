#!/usr/bin/env python
"""Various helper functions for workqueue"""

__all__ = ['get_remote_queue', 'get_dbs',
           'queueConfigFromConfigObject', 'queueFromConfig']

import logging
import os

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
        __queues[queue] = WorkQueueDS({'endpoint': queue, 'logger': logger})
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

__USE_CRIC = os.getenv("WMAGENT_USE_CRIC", False) or os.getenv("WMCORE_USE_CRIC", False)
__sitedb = None  # FIXME: rename it to __cric
__cmsSiteNames = []


def cmsSiteNames():
    """Get all cms sites"""
    global __cmsSiteNames
    if __cmsSiteNames:
        return __cmsSiteNames
    logging.info("cmsSiteNames Using CRIC Service: %s", __USE_CRIC)
    global __sitedb
    if not __sitedb:
        if __USE_CRIC:
            from WMCore.Services.CRIC.CRIC import CRIC
            __sitedb = CRIC()
        else:
            from WMCore.Services.SiteDB.SiteDB import SiteDBJSON as SiteDB
            __sitedb = SiteDB()
    try:
        if __USE_CRIC:
            __cmsSiteNames = __sitedb.getAllPSNs()
        else:
            __cmsSiteNames = __sitedb.getAllCMSNames()
    except Exception:
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
        sites = list(set(sites) - set(siteBlacklist))
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

    # setup CRIC or SiteDB computing resource
    wqManager.queueParams['useCric'] = getattr(wqManager, 'useCric', False)

    # pull some info we need from other areas of the config
    if "BossAirConfig" not in qConfig and hasattr(config, 'BossAir'):
        qConfig["BossAirConfig"] = config
        qConfig['BossAirConfig'].section_("Agent").agentName = config.Agent.agentName
    if "JobDumpConfig" not in qConfig and hasattr(config, 'JobStateMachine'):
        qConfig["JobDumpConfig"] = config
    if "CacheDir" not in qConfig and getattr(config.WorkQueueManager, 'componentDir', None):
        qConfig['CacheDir'] = os.path.join(config.WorkQueueManager.componentDir, 'cache')

    # alert api needs full agent config
    if hasattr(config, 'Alert'):
        qConfig['Config'] = config.Alert

    if 'Team' not in qConfig and hasattr(config.Agent, 'teamName'):
        qConfig['Team'] = config.Agent.teamName
    if 'logger' not in qConfig:
        import threading
        myThread = threading.currentThread()
        if not hasattr(myThread, 'logger'):
            loggingLevelName = getattr(wqManager, 'logLevel', 'INFO')
            logging.basicConfig(format='%(asctime)-15s %(levelname)-8s %(module)s: %(message)s',
                                level=getattr(logging, loggingLevelName))
            myThread.logger = logging.getLogger('workqueue')
        qConfig['logger'] = myThread.logger

    # ReqMgr params
    if not hasattr(wqManager, 'reqMgrConfig'):
        wqManager.reqMgrConfig = {}

    return config
