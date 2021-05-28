#!/usr/bin/env python
"""Various helper functions for workqueue"""

import logging
import os
from WMCore.Services.CRIC.CRIC import CRIC


__dbses = {}


def get_dbs(url):
    """Return DBS object for url"""
    try:
        return __dbses[url]
    except KeyError:
        from WMCore.Services.DBS.DBSReader import DBSReader
        __dbses[url] = DBSReader(url)
        return __dbses[url]

__cric = None
__cmsSiteNames = []


def cmsSiteNames():
    """Get all cms sites"""
    global __cmsSiteNames
    if __cmsSiteNames:
        return __cmsSiteNames
    global __cric
    if not __cric:
        __cric = CRIC()

    try:
        __cmsSiteNames = __cric.getAllPSNs()
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

    # Rucio-related config
    if hasattr(wqManager, 'rucioUrl'):
        qConfig['rucioUrl'] = wqManager.rucioUrl
    if hasattr(wqManager, 'rucioAuthUrl'):
        qConfig['rucioAuthUrl'] = wqManager.rucioAuthUrl

    if hasattr(wqManager, 'couchurl'):
        qConfig['CouchUrl'] = wqManager.couchurl
    if hasattr(wqManager, 'dbname'):
        qConfig['DbName'] = wqManager.dbname
    if hasattr(wqManager, 'inboxDatabase'):
        qConfig['InboxDbName'] = wqManager.inboxDatabase

    # pull some info we need from other areas of the config
    if "BossAirConfig" not in qConfig and hasattr(config, 'BossAir'):
        qConfig["BossAirConfig"] = config
        qConfig['BossAirConfig'].section_("Agent").agentName = config.Agent.agentName
    if "JobDumpConfig" not in qConfig and hasattr(config, 'JobStateMachine'):
        qConfig["JobDumpConfig"] = config
    if "CacheDir" not in qConfig and getattr(config.WorkQueueManager, 'componentDir', None):
        qConfig['CacheDir'] = os.path.join(config.WorkQueueManager.componentDir, 'cache')

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
