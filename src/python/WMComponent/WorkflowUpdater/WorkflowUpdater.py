#!/usr/bin/env python
"""
Component responsible for locally updating workflow specs and sandboxes.
"""

import logging
import threading
from pprint import pformat

from WMComponent.WorkflowUpdater.WorkflowUpdaterPoller import WorkflowUpdaterPoller
from WMCore.Agent.Harness import Harness


class WorkflowUpdater(Harness):
    """
    Create a WorkflowUpdaterPoller component as a daemon
    """

    def __init__(self, config):
        """
        Initialize it with the agent configuration parameters.
        :param config: a Configuration object with the component configuration
        """
        # call the base class
        Harness.__init__(self, config)

    def preInitialization(self):
        """
        Step that actually adds the worker thread properly
        """
        pollInterval = self.config.WorkflowUpdater.pollInterval
        logging.info("Starting %s with configuration:\n%s", self.__class__.__name__,
                     pformat(self.config.WorkflowUpdater.dictionary_()))

        myThread = threading.currentThread()
        myThread.workerThreadManager.addWorker(WorkflowUpdaterPoller(self.config),
                                               pollInterval)
