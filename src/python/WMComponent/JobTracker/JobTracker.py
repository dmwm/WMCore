#!/usr/bin/env python


"""
Checks for finished subscriptions
Upon finding finished subscriptions, notifies WorkQueue and kills them
"""

__revision__ = "$Id: JobTracker.py,v 1.2 2010/02/11 17:09:48 mnorman Exp $"
__version__ = "$Revision: 1.2 $"

import logging
import threading

from WMCore.Agent.Harness import Harness
#from WMCore.WMFactory import WMFactory

from WMComponent.JobTracker.JobTrackerPoller import JobTrackerPoller



class JobTracker(Harness):
    """
    Checks for finished subscriptions
    Upon finding finished subscriptions, notifies WorkQueue and kills them

    """

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        self.pollTime = 1

        self.config = config
        
        print "JobTracker.__init__"

    def preInitialization(self):
        """
        Sets up the worker thread

        """
        logging.info("JobTracker.preInitialization")

        # Add event loop to worker manager
        myThread = threading.currentThread()

        pollInterval = self.config.JobTracker.pollInterval
        logging.info("Setting poll interval to %s seconds" %pollInterval)
        myThread.workerThreadManager.addWorker(JobTrackerPoller(self.config), pollInterval)

        return
