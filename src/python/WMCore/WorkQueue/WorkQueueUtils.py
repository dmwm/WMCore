#!/usr/bin/env python
"""Various helper functions for workqueue"""

__all__ = ['get_remote_queue', 'get_dbs', 'sitesFromStorageEelements',
           'queueConfigFromConfigObject', 'queueFromConfig']

# Should probably import this but don't want to create the dependency
WORKQUEUE_REST_NAMESPACE = 'WMCore.HTTPFrontEnd.WorkQueue.WorkQueueRESTModel'
WORKQUEUE_MONITOR_NAMESPACE = 'WMCore.HTTPFrontEnd.WorkQueue.WorkQueueMonitorPage'

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

    qConfig["BossAirConfig"] = getattr(config.WorkQueueManager, "BossAirConfig", None)
    qConfig["JobDumpConfig"] = getattr(config.WorkQueueManager, "JobDumpConfig", None)

    try:
        monitorURL = ''
        queueFlag = False
        for webapp in config.listWebapps_():
            webapp = config.webapp_(webapp)
            for page in webapp.section_('views').section_('active'):

                if not queueFlag and hasattr(page, "model") \
                   and page.section_('model').object == WORKQUEUE_REST_NAMESPACE:
                    qConfig['QueueURL'] = 'http://%s:%s/%s/%s' % (webapp.Webtools.host,
                                                              webapp.Webtools.port,
                                                              webapp._internal_name.lower(),
                                                              page._internal_name)
                    queueFlag = True

                if page.object == WORKQUEUE_MONITOR_NAMESPACE:
                    monitorURL = 'http://%s:%s/%s/%s' % (webapp.Webtools.host,
                                                      webapp.Webtools.port,
                                                      webapp._internal_name.lower(),
                                                      page._internal_name)
        if not queueFlag:
            raise RuntimeError

    except RuntimeError:
        msg = """Unable to determine WorkQueue QueueURL, Either:
        Configure a WorkQueueRESTModel webapp_ section or,
        Add a WorkQueueManager.queueParams.QueueURL setting."""
        raise RuntimeError, msg

    if not qConfig.has_key('Teams') and hasattr(config.Agent, 'teamName'):
        qConfig['Teams'] = config.Agent.teamName
    if not 'logger' in qConfig:
        import threading
        myThread = threading.currentThread()
        if not hasattr(myThread, 'logger'):
            import logging
            myThread.logger = logging.getLogger()
        qConfig['logger'] = myThread.logger

    # ReqMgr params
    if not hasattr(wqManager, 'reqMgrConfig'):
        wqManager.reqMgrConfig = {}
    wqManager.reqMgrConfig['QueueURL'] = qConfig['QueueURL']
    wqManager.reqMgrConfig['MonitorURL'] = monitorURL

    return config
