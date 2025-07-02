#!/usr/bin/env python
"""
_AgentWatchdog_

This component is the default watchdog for WMAgent.
"""

import logging
import threading

from WMCore.Agent.Harness import Harness
from WMComponent.AgentWatchdog.AgentWatchdogPoller import AgentWatchdogPoller


class AgentWatchdog(Harness):
    """
    Component class for AgentWatchdog module
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
        logging.info("AgentWatchdog.preInitialization")
        agentWatchdogPollInterval = self.config.AgentWatchdog.pollInterval
        myThread = threading.currentThread()

        if not hasattr(self.config, "Tier0Feeder"):
            logging.info("Setting AgentWatchdogPoller poll interval to %s seconds", agentWatchdogPollInterval)
            agentStatusPollerThread = myThread.workerThreadManager.addWorker(AgentWatchdogPoller(self.config),
                                                                             agentWatchdogPollInterval)
        return
