#!/usr/bin/env python


"""
Checks for finished subscriptions
Upon finding finished subscriptions, notifies WorkQueue and kills them
"""

__revision__ = "$Id: JobArchiver.py,v 1.1 2009/09/29 16:33:46 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import logging
import threading

from WMCore.Agent.Harness import Harness
from WMCore.WMFactory import WMFactory

from WMComponent.JobArchiver.JobArchiverPoller import JobArchiverPoller

#from WMCore.WorkQueue.WorkQueue import WorkQueue




class JobArchiver(Harness):

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        self.pollTime = 1

        self.config = config
        
	print "JobArchiver.__init__"

    def preInitialization(self):
	print "JobArchiver.preInitialization"

        # Add event loop to worker manager
        myThread = threading.currentThread()

        pollInterval = self.config.JobArchiver.pollInterval
        logging.info("Setting poll interval to %s seconds" % pollInterval)
        myThread.workerThreadManager.addWorker(JobArchiverPoller(self.config), pollInterval)

        return
