#!/usr/bin/env python


"""
Checks for finished subscriptions
Upon finding finished subscriptions, notifies WorkQueue and kills them
"""

__revision__ = "$Id: WorkQueueManager.py,v 1.2 2010/02/12 14:34:37 swakef Exp $"
__version__ = "$Revision: 1.2 $"

import logging
import threading
from os import path

from WMCore.Agent.Harness import Harness
from WMCore.WMFactory import WMFactory

from WMComponent.WorkQueueManager.WorkQueueManagerWorkPoller import WorkQueueManagerWorkPoller
from WMComponent.WorkQueueManager.WorkQueueManagerReqMgrPoller import WorkQueueManagerReqMgrPoller
from WMComponent.WorkQueueManager.WorkQueueManagerLocationPoller import WorkQueueManagerLocationPoller
from WMComponent.WorkQueueManager.WorkQueueManagerFlushPoller import WorkQueueManagerFlushPoller
from WMComponent.WorkQueueManager.WorkQueueManagerReportPoller import WorkQueueManagerReportPoller

from WMCore.WorkQueue.WorkQueue import localQueue, globalQueue, WorkQueue
from WMCore.Services.RequestManager.RequestManager \
     import RequestManager as RequestManagerDS

class WorkQueueManager(Harness):

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)

        self.config = self.setConfig(config)

        self.logger = None
        self.wq = None
        self.reqMgr = None
        
        print "WorkQueueManager.__init__"

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

        # ReqMgr params
        if not hasattr(wqManager, 'reqMgrConfig'):
            wqManager.reqMgrConfig = {}

        # WorkQueue config
        if not hasattr(wqManager, 'queueParams'):
            wqManager.queueParams = {}
        qConfig = wqManager.queueParams
        qConfig.setdefault('CacheDir', path.join(wqManager.componentDir, 'wf'))

        return config

    def preInitialization(self):
        print "WorkQueueManager.preInitialization"

        # Add event loop to worker manager
        myThread = threading.currentThread()
        self.logger = myThread.logger

        self.instantiateQueues(self.config)

        pollInterval = self.config.WorkQueueManager.pollInterval

        # Update Locations 
        myThread.workerThreadManager.addWorker(WorkQueueManagerLocationPoller(self.wq), self.wq.params['LocationRefreshInterval'])
        # Get work from ReqMgr & flush expired negotiations
        if self.config.WorkQueueManager.level == 'GlobalQueue':
            myThread.workerThreadManager.addWorker(WorkQueueManagerFlushPoller(self.wq), self.wq.params['NegotiationTimeout'])
            myThread.workerThreadManager.addWorker(WorkQueueManagerReqMgrPoller(self.reqMgr, self.wq, getattr(self.config.WorkQueueManager, 'reqMgrConfig', {})), pollInterval)
        # If we have a parent we need to get work and report back
        if self.wq.params['ParentQueue']:
            # Get work from RequestManager or parent
            myThread.workerThreadManager.addWorker(WorkQueueManagerWorkPoller(self.wq), pollInterval)
            # Report to parent queue
            myThread.workerThreadManager.addWorker(WorkQueueManagerReportPoller(self.wq), self.wq.params['ReportInterval'])
        
        return

    def instantiateQueues(self, config):
        """
        Create an appropriate queue
        """
        config.WorkQueueManager.queueParams.setdefault('logger', self.logger)
        self.logger.info("Creating %s queue" % config.WorkQueueManager.level)
        if config.WorkQueueManager.level == 'GlobalQueue':
            self.wq = globalQueue(**config.WorkQueueManager.queueParams)
            reqMgrParams = {'logger' : self.logger}
            if hasattr(self.config.WorkQueueManager, 'reqMgrConfig'):
                reqMgrParams.update(self.config.WorkQueueManager.reqMgrConfig)
            self.reqMgr = RequestManagerDS(reqMgrParams)
        elif config.WorkQueueManager.level == 'LocalQueue':
            self.wq = localQueue(**config.WorkQueueManager.queueParams)
        else:
            self.wq = WorkQueue(**config.WorkQueueManager.queueParams)
        logging.info("Queue instantiated")
