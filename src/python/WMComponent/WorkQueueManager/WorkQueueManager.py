#!/usr/bin/env python
"""
WorkQueuemanager component

Runs periodic tasks for WorkQueue
"""




import threading
from os import path

from WMCore.Agent.Harness import Harness

from WMComponent.WorkQueueManager.WorkQueueManagerWorkPoller import WorkQueueManagerWorkPoller
from WMComponent.WorkQueueManager.WorkQueueManagerReqMgrPoller import WorkQueueManagerReqMgrPoller
from WMComponent.WorkQueueManager.WorkQueueManagerLocationPoller import WorkQueueManagerLocationPoller
from WMComponent.WorkQueueManager.WorkQueueManagerCleaner import WorkQueueManagerCleaner
from WMComponent.WorkQueueManager.WorkQueueManagerWMBSFileFeeder import WorkQueueManagerWMBSFileFeeder

from WMCore.WorkQueue.WorkQueue import localQueue, globalQueue, WorkQueue

# Should probably import this but don't want to create the dependency
WORKQUEUE_REST_NAMESPACE = 'WMCore.HTTPFrontEnd.WorkQueue.WorkQueueRESTModel'
WORKQUEUE_MONITOR_NAMESPACE = 'WMCore.HTTPFrontEnd.WorkQueue.WorkQueueMonitorPage'

class WorkQueueManager(Harness):
    """WorkQueuemanager component

    Runs periodic tasks for WorkQueue
    """

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        self.config = self.setConfig(config)
        self.logger = None


    def setConfig(self, config):
        """
        Set various defaults if not provided by the user
        """
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
        qConfig.setdefault('CacheDir', path.join(wqManager.componentDir, 'wf'))

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

        # ReqMgr params
        if not hasattr(wqManager, 'reqMgrConfig'):
            wqManager.reqMgrConfig = {}
        wqManager.reqMgrConfig['QueueURL'] = qConfig['QueueURL']
        wqManager.reqMgrConfig['MonitorURL'] = monitorURL

        return config

    def preInitialization(self):
        print "WorkQueueManager.preInitialization"

        # Add event loop to worker manager
        myThread = threading.currentThread()
        self.logger = myThread.logger
        pollInterval = self.config.WorkQueueManager.pollInterval

        ### Global queue special functions
        if self.config.WorkQueueManager.level == 'GlobalQueue':

            # Get work from ReqMgr, report back & delete finished requests
            myThread.workerThreadManager.addWorker(
                                WorkQueueManagerReqMgrPoller( 
                                        self.instantiateQueue(self.config),
                                        getattr(self.config.WorkQueueManager, 
                                                'reqMgrConfig', {})
                                        ),
                                 pollInterval)

        ### local queue special function
        elif self.config.WorkQueueManager.level == 'LocalQueue':

            # pull work from parent queue
            myThread.workerThreadManager.addWorker(
                                WorkQueueManagerWorkPoller(self.instantiateQueue(self.config)), 
                                pollInterval)

            # inject acquired work into wmbs
            myThread.workerThreadManager.addWorker(
                                WorkQueueManagerWMBSFileFeeder(self.instantiateQueue(self.config)), 
                                pollInterval)

        ### general functions

        # Data location updates
        myThread.workerThreadManager.addWorker(
                                    WorkQueueManagerLocationPoller(self.instantiateQueue(self.config)),
                                    pollInterval)

        # Clean finished work & apply end policies
        myThread.workerThreadManager.addWorker(
                                WorkQueueManagerCleaner(self.instantiateQueue(self.config)), 
                                pollInterval)

        return

    def instantiateQueue(self, config):
        """
        Create an appropriate queue
        """
        config.WorkQueueManager.queueParams.setdefault('logger', self.logger)
        self.logger.info("Creating %s queue" % config.WorkQueueManager.level)
        if config.WorkQueueManager.level == 'GlobalQueue':
            return globalQueue(**config.WorkQueueManager.queueParams)
        elif config.WorkQueueManager.level == 'LocalQueue':
            return localQueue(**config.WorkQueueManager.queueParams)
        else:
            return WorkQueue(**config.WorkQueueManager.queueParams)
