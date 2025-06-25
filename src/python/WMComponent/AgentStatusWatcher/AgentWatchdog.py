"""
A simple watchdog mechanism based on OS signal handling, implemented to help in the monitoring
of the health of all WMAgent components by AgentStatusWatcher.

This watchdog is to be watching only AgentStatusPoller thread's run time overflow and will
take care of proper restarts only to this component, while at the same time AgentStatusPoller
is to be endowed with a mechanism to monitor the health of all other components' process trees
and take the proper (restart) actions in case of unhealthy component is found.
"""

import threading
import os
import signal
import logging
import time

class AgentWatchdog(BaseWorkerThread):
    """
    A basic watchdog class
    """
    def  __init__(self, config):
        self.config = config
        self.watchedPid = None
        self.watchedComponent = None
        self.watchdogTimeout = self.config.AgentWatchdog.watchdogTimeout

    def setup(self, parameters):
        """
        __setup__

        Setup the name of the component to be watched
        """
        self.setWatchedComponent()

    def setWatchedPid(self, pid):
        """
        A simple function to add the watched component PID
        :param pid: The PID to be added
        """
        self.watchedPid = pid

    def setWatchedComponent(self, componentName='AgentStatusWatcher'):
        """
        A simple function to add the watched component name
        :param componentName: the component name to be watched (Default: AgentStatusWatcher')
        """
        self.watchedComponent = componentName

    def algorithm(self):
        """
        __algorithm__

        The main algorithm for the watchdog thread
        """
        
