"""
__JobUpdater__

Check and calculate changes in job information and
updates the records of pending jobs in the batch
system

Created on Apr 16, 2013

@author: dballest
"""

import logging
import threading

from WMComponent.JobUpdater.JobUpdaterPoller import JobUpdaterPoller

from WMCore.Agent.Harness import Harness

class JobUpdater(Harness):
    """
    Component class for the JobUpdater module,
    runs a single worker, the JobUpdaterPoller
    """

    def __init__(self, config):
        """
        __init__

        Initialize the Harness
        """
        Harness.__init__(self, config)

    def preInitialization(self):
        """
        _preInitialization_

        Sets up the worker thread
        """
        logging.info("JobTracker.preInitialization")

        myThread = threading.currentThread()

        pollInterval = self.config.JobUpdater.pollInterval
        logging.info("Setting poll interval to %s seconds" % pollInterval)
        myThread.workerThreadManager.addWorker(JobUpdaterPoller(self.config), pollInterval)

        return
