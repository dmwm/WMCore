#!/usr/bin/env python
"""
_ArhciveDataReporterr_

"""
from __future__ import (division, print_function)
import logging
import threading

from WMCore.Agent.Harness import Harness
from WMComponent.ArchiveDataReporter.ArchiveDataPoller import ArchiveDataPoller

class ArchiveDataReporter(Harness):
    """
    Component class for AgentStatusWatcher module
    """

    def __init__(self, config):
        """
        __init__

        Initialize the Harness
        """
        Harness.__init__(self, config)

        return

    def preInitialization(self):
        """
        _preInitialization_

        Sets up the worker thread
        """
        logging.info("ArhciveDataReporter.preInitialization")
        pollInterval = self.config.ArchiveDataReporter.pollInterval
        myThread = threading.currentThread()

        if not self.config.ArchiveDataReporter.WMArchiveURL:
            logging.info("No archive url is set: Doing nothing")
        else:
            logging.info("Setting ResourcesUpdate poll interval to %s seconds" % pollInterval)
            myThread.workerThreadManager.addWorker(ArchiveDataPoller(self.config), pollInterval)

        return
