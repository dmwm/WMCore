#!/usr/bin/env python
"""
_AgentStatusWatcher_

This component is a general purpose component for resources and processes handling
"""

import logging
import threading

from WMCore.Agent.Harness import Harness
from WMComponent.AgentStatusWatcher.ResourceControlUpdater import ResourceControlUpdater
from WMComponent.AgentStatusWatcher.AgentStatusPoller import AgentStatusPoller
from WMComponent.AgentStatusWatcher.DrainStatusPoller import DrainStatusPoller

class AgentStatusWatcher(Harness):
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
        logging.info("AgentStatusWatcher.preInitialization")
        resourceUpdaterPollInterval = self.config.AgentStatusWatcher.resourceUpdaterPollInterval
        agentPollInterval = self.config.AgentStatusWatcher.agentPollInterval
        drainStatusPollInterval = self.config.AgentStatusWatcher.drainStatusPollInterval
        myThread = threading.currentThread()

        logging.info("Setting AgentStatusPoller poll interval to %s seconds", agentPollInterval)
        myThread.workerThreadManager.addWorker(AgentStatusPoller(self.config),
                                               agentPollInterval)

        logging.info("Setting ResourceControlUpdater poll interval to %s seconds", resourceUpdaterPollInterval)
        myThread.workerThreadManager.addWorker(ResourceControlUpdater(self.config),
                                               resourceUpdaterPollInterval)

        if not hasattr(self.config, "Tier0Feeder"):
            # Don't set up DrainStausPoller for Tier0
            logging.info("Setting DrainStatusPoller poll interval to %s seconds", drainStatusPollInterval)
            myThread.workerThreadManager.addWorker(DrainStatusPoller(self.config),
                                               drainStatusPollInterval)
        return
