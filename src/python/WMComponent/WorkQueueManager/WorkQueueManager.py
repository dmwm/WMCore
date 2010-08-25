#!/usr/bin/env python


"""
Checks for finished subscriptions
Upon finding finished subscriptions, notifies WorkQueue and kills them
"""

__revision__ = "$Id: WorkQueueManager.py,v 1.1 2009/12/01 21:55:21 sryu Exp $"
__version__ = "$Revision: 1.1 $"

import logging
import threading

from WMCore.Agent.Harness import Harness
from WMCore.WMFactory import WMFactory

from WMComponent.WorkQueueManager.WorkQueueManagerPoller import WorkQueueManagerPoller


class WorkQueueManager(Harness):

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        self.pollTime = 1

        self.config = config
        
        print "WorkQueueManager.__init__"

    def preInitialization(self):
        print "WorkQueueManager.preInitialization"

        # Add event loop to worker manager
        myThread = threading.currentThread()

        pollInterval = self.config.WorkQueueManager.pollInterval
        logging.info("Setting poll interval to %s seconds" % pollInterval)
        myThread.workerThreadManager.addWorker(WorkQueueManagerPoller(self.config), pollInterval)

        return
