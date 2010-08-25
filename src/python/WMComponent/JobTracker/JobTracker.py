#!/usr/bin/env python


"""
Checks for finished subscriptions
Upon finding finished subscriptions, notifies WorkQueue and kills them
"""

__revision__ = "$Id: JobTracker.py,v 1.1 2009/10/02 21:28:45 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import logging
import threading

from WMCore.Agent.Harness import Harness
from WMCore.WMFactory import WMFactory

from WMComponent.JobTracker.JobTrackerPoller import JobTrackerPoller

#from WMCore.WorkQueue.WorkQueue import WorkQueue




class JobTracker(Harness):

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        self.pollTime = 1

        self.config = config
        
	print "JobTracker.__init__"

    def preInitialization(self):
	print "JobTracker.preInitialization"

        # Add event loop to worker manager
        myThread = threading.currentThread()

        pollInterval = self.config.JobTracker.pollInterval
        logging.info("Setting poll interval to %s seconds" % pollInterval)
        myThread.workerThreadManager.addWorker(JobTrackerPoller(self.config), pollInterval)

        return
