"""
A simple watchdog mechanism based on OS signal handling, implemented to help in the monitoring
of the health of all WMAgent components.

This AgentWatchdogScanner thread is to be scanning all other components' process trees
and will take the proper (restart) actions in case of unhealthy component is found.
While at the same time the AgentWatchdogPoller is to be endowed with a mechanism
to monitor for run time overflow in all agent components and will take care of
proper restarts of the misbehaved ones.


And we shall call this component with the affectionate nickname: Cerberus ;)
"""

import threading
import os
import signal
import logging
import time
import subprocess
import psutil
import inspect
import random
import json
import re

from pprint import pformat

from Utils.ProcFS import processStatus
from Utils.Timers import timeFunction
from Utils.wmcoreDTools import getComponentThreads, restart, forkRestart, isComponentAlive
from WMComponent.AgentWatchdog.Timer import Timer, _countdown, WatchdogAction, TimerException
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.WMInit import connectToDB


class AgentWatchdogScanner(BaseWorkerThread):
    """
    The basic AgentWatchdogScanner class
    """
    def  __init__(self, config):
        BaseWorkerThread.__init__(self)
        self.config = config
        self.mainThread = threading.currentThread()

        # self.watchedComponents = self.config.AgentWatchdog.watchedComponents
        # self.actionLimit = self.config.AgentWatchdog.actionLimit

        self.mainThread.parents = [ thread['pid'] for thread in processStatus(self.mainThread.native_id)]
        logging.info(f"{self.mainThread.name}: Initialized with parents {self.mainThread.parents}.")
        self.mainThread.enum = [ pid.native_id for pid in threading.enumerate() ]
        logging.info(f"{self.mainThread.name}: Initialized with the following threads: {self.mainThread.enum}.")
        self.mainThread.main = threading.main_thread().native_id
        logging.debug(f"{self.mainThread.name}: Initialized with main_thread {self.mainThread.main}.")

    def checkCompAlive(self):
        """
        # Iterate through all components and check if they have a healthy process tree:
        # Restart any of them found to have problems.
        # NOTE: Exclude AgentWatchdog and itself from this logic, because:
        #       * AgentWatchdog should never be touched since it monitors all others poling cycles
        #       * AgentWatchdog is going to spawn threads dynamically and will fail all checks
        #       * AgentWatchdog cannot restart itself
        """
        logging.info(f"{self.mainThread.name}: Checking all components' threads.")
        for component in [comp for comp in self.config.listComponents_() if comp != 'AgentWatchdog']:
            if not isComponentAlive(self.config, component=component):
                try:
                    logging.warning(f"{self.mainThread.name}: Restarting Unhealthy component: {component}")
                    forkRestart(self.config, componentsList=[component])
                except Exception as ex:
                    logging.error(f"{self.mainThread.name}: Failed to restart component: {component}. Full ERROR: {str(ex)}")
                    raise
    @timeFunction
    def algorithm(self, parameters=None):
        """
        Spawn and register watchdog timers
        """
        # TODO: To implement logic with limited number of restarts, similar to AgentWatchdogPoller
        try:
            currThread = threading.currentThread()
            logging.info(f"{self.mainThread.name}: Polling cycle started with current pid: {currThread.native_id}.")
            self.checkCompAlive()
        except Exception as ex:
            logging.exception(f"{self.mainThread.name}: Exception occurred while executing the AgentWatchdogScanner logic.")
