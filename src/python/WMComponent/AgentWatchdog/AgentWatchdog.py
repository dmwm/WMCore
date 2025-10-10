#!/usr/bin/env python
"""
_AgentWatchdog_

This component is the default watchdog for WMAgent.

A simple watchdog mechanism based on OS signal handling, implemented to help in the monitoring
of the health of all WMAgent components.
"""

import logging
import threading

from WMCore.Agent.Harness import Harness
from WMComponent.AgentWatchdog.AgentWatchdogPoller import AgentWatchdogPoller
from WMComponent.AgentWatchdog.AgentWatchdogScanner import AgentWatchdogScanner


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

        # Do not wait between runs.
        # NOTE: For AgentWatchdogPoller, the configured pooling interval will actually be used as a wait
        #       time inside the main thread to drive the cyclic process for regular refresh of all timers.
        #       The result will be, that we will be executing every time for exactly the amount
        #       of time configured as polling cycle in the component config and wait for 0 seconds
        #       between re-execution. In contrast to how all other WMAgent components behave,
        #       where, they execute for a random amount of time (driven by their resource consumption
        #       and other delays during the execution itself), and then pause for the time configured
        #       as component's poll interval before re-executing the main thread algorithm method again.
        #       If we were to follow the order established for the other components, the value here should
        #       have been:
        #       agentWatchdogPollInterval = self.config.AgentWatchdog.pollInterval
        agentWatchdogPollerInterval = 0
        agentWatchdogScannerInterval = self.config.AgentWatchdog.AgentWatchdogScanner.pollInterval
        currThread = threading.current_thread()

        if getattr(self.config.AgentWatchdog.AgentWatchdogPoller, 'enabled', False):
            logging.info("Setting AgentWatchdogPoller poll interval to %s seconds", agentWatchdogPollerInterval)
            agentWatchdogPollerThread = currThread.workerThreadManager.addWorker(AgentWatchdogPoller(self.config),
                                                                             agentWatchdogPollerInterval)
            logging.info("AgentWatchdog thread PID: %s", currThread.native_id)
            logging.info("AgentWatchdogPoller thread PID: %s", agentWatchdogPollerThread.native_id)

        if getattr(self.config.AgentWatchdog.AgentWatchdogScanner, 'enabled', False):
            logging.info("Setting AgentWatchdogScanner poll interval to %s seconds", agentWatchdogScannerInterval)
            agentWatchdogScannerThread = currThread.workerThreadManager.addWorker(AgentWatchdogScanner(self.config),
                                                                             agentWatchdogScannerInterval)
            logging.info("AgentWatchdog thread PID: %s", currThread.native_id)
            logging.info("AgentWatchdogScanner thread PID: %s", agentWatchdogScannerThread.native_id)

        return
