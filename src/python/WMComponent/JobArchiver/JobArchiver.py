#!/usr/bin/env python


"""
Checks for finished jobs
Upon finding jobs cleans out their logs.
"""
from __future__ import print_function




import logging
import threading

from WMCore.Agent.Harness import Harness

from WMComponent.JobArchiver.JobArchiverPoller import JobArchiverPoller

#from WMCore.WorkQueue.WorkQueue import WorkQueue




class JobArchiver(Harness):
    """
    Checks for finished jobs
    Upon finding jobs cleans out their logs.
    """

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        self.pollTime = 1

        self.config = config

        print("JobArchiver.__init__")

    def preInitialization(self):
        """
        Handles the setup of the worker thread.

        """
        logging.info("JobArchiver.preInitialization")

        # Add event loop to worker manager
        myThread = threading.currentThread()

        pollInterval = self.config.JobArchiver.pollInterval
        logging.info("Setting poll interval to %s seconds" % pollInterval)
        myThread.workerThreadManager.addWorker(JobArchiverPoller(self.config), pollInterval)

        return
