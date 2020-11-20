#!/usr/bin/env python
"""
WorkQueuemanager component

Runs periodic tasks for WorkQueue
"""
from __future__ import print_function

import threading

from WMComponent.WorkQueueManager.WorkQueueManagerCleaner import WorkQueueManagerCleaner
from WMComponent.WorkQueueManager.WorkQueueManagerLocationPoller import WorkQueueManagerLocationPoller
from WMComponent.WorkQueueManager.WorkQueueManagerReqMgrPoller import WorkQueueManagerReqMgrPoller
from WMComponent.WorkQueueManager.WorkQueueManagerWMBSFileFeeder import WorkQueueManagerWMBSFileFeeder
from WMComponent.WorkQueueManager.WorkQueueManagerWorkPoller import WorkQueueManagerWorkPoller
from WMCore.Agent.Harness import Harness
from WMCore.WorkQueue.WorkQueueUtils import queueFromConfig, queueConfigFromConfigObject


class WorkQueueManager(Harness):
    """WorkQueuemanager component

    Runs periodic tasks for WorkQueue
    """

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        self.config = queueConfigFromConfigObject(config)

    def preInitialization(self):
        print("WorkQueueManager.preInitialization")

        # Add event loop to worker manager
        myThread = threading.currentThread()
        pollInterval = self.config.WorkQueueManager.pollInterval
        dataLocationInterval = getattr(self.config.WorkQueueManager, "dataLocationInterval", 1 * 60 * 60)

        ### Global queue special functions
        if self.config.WorkQueueManager.level == 'GlobalQueue':

            # Get work from ReqMgr, report back & delete finished requests
            myThread.workerThreadManager.addWorker(
                    WorkQueueManagerReqMgrPoller(
                            queueFromConfig(self.config),
                            getattr(self.config.WorkQueueManager,
                                    'reqMgrConfig', {})
                    ),
                    pollInterval)

        ### local queue special function
        elif self.config.WorkQueueManager.level == 'LocalQueue':

            # pull work from parent queue
            myThread.workerThreadManager.addWorker(
                    WorkQueueManagerWorkPoller(queueFromConfig(self.config),
                                               self.config),
                    pollInterval)

            # inject acquired work into wmbs
            myThread.workerThreadManager.addWorker(
                    WorkQueueManagerWMBSFileFeeder(queueFromConfig(self.config),
                                                   self.config),
                    pollInterval)

        ### general functions

        # Data location updates
        myThread.workerThreadManager.addWorker(
                WorkQueueManagerLocationPoller(queueFromConfig(self.config),
                                               self.config),
                dataLocationInterval)

        # Clean finished work & apply end policies
        myThread.workerThreadManager.addWorker(
                WorkQueueManagerCleaner(queueFromConfig(self.config),
                                        self.config),
                pollInterval)

        return
