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
import subprocess

from Utils.Timers import timeFunction
from Utils.wmcoreDTools import getComponentThreads, restart, forkRestart
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.WMInit import connectToDB

class AgentWatchdog(BaseWorkerThread):
    """
    A basic watchdog class
    """
    def  __init__(self, config):
        BaseWorkerThread.__init__(self)
        self.config = config
        # myThread = threading.currentThread()

        self.watchedPid = None
        self.watchedComponent = None
        # self.watchdogTimeout = self.config.AgentWatchdog.watchdogTimeout
        self.watchdogTimeout = 5
        self.timers = {}

    def _timer(self, interval):
        """
        """
        pass

    def setupTimer(self, timerName, interval):
        """
        """
        pass

    @timeFunction
    def algorithm(self, parameters=None):
        """
        Spawn and register watchdog timers
        """
        logging.info(f"Started to watch {self.watchedComponent}")
        try:
            # pidTree = getComponentThreads(self.watchedComponent)
            logging.info(f"Sleeping for {self.watchdogTimeout} secs")
            time.sleep(self.watchdogTimeout)
            logging.info(f"Restarting {self.watchedComponent}")
            exitCode = forkRestart(componentsList=[self.watchedComponent], useWmcoreD=True)
            # exitCode = forkRestart(config=self.config, componentsList=[self.watchedComponent], useWmcoreD=False)
            logging.info(f"Exit code from forkRestart of {self.watchedComponent}: {exitCode}")
        except Exception as ex:
            logging.error(f"Exception: {str(ex)}")

    def setup(self, parameters=None):
        """
        __setup__

        Setup the name of the component to be watched
        """
        self.setWatchedComponent('ErrorHandler')
        return

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
