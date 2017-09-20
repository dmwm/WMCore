#!/usr/bin/env python

"""
Creates jobs for new subscriptions

"""

import logging
import threading

from WMComponent.JobSubmitter.JobSubmitterPoller import JobSubmitterPoller
from WMCore.Agent.Harness import Harness


class JobSubmitter(Harness):
    """
    Creates jobs for new subscriptions

    """

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        self.pollTime = 1

        logging.info("JobSubmitter.__init__")

    def preInitialization(self):
        """
        Setup the worker thread for jobSubmitter

        """
        logging.info("JobSubmitter.preInitialization")

        # Add event loop to worker manager
        myThread = threading.currentThread()

        pollInterval = self.config.JobSubmitter.pollInterval
        logging.info("Setting poll interval to %s seconds", pollInterval)
        myThread.workerThreadManager.addWorker(JobSubmitterPoller(self.config),
                                               pollInterval)

        return
