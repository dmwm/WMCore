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
from WMCore.Services.AlertManager.AlertManagerAPI import AlertManagerAPI


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
        self.pollInterval = self.config.AgentWatchdog.AgentWatchdogScanner.pollInterval

        self.mainThread.parents = [ thread['pid'] for thread in processStatus(self.mainThread.native_id)]
        logging.info(f"Initialized with parents {self.mainThread.parents}.")
        self.mainThread.enum = [ pid.native_id for pid in threading.enumerate() ]
        logging.info(f"Initialized with the following threads: {self.mainThread.enum}.")
        self.mainThread.main = threading.main_thread().native_id
        logging.debug(f"Initialized with main_thread {self.mainThread.main}.")

        self.alertManagerUrl = self.config.Alert.alertManagerUrl
        self.alertDestinationMap = self.config.Alert.alertDestinationMap
        self.alertManager = AlertManagerAPI(self.alertManagerUrl)
        logging.info(f"Setting up an alertManager instance for AgentWatchdogScanner and redirecting alerts to: {self.alertManagerUrl}.")

    def sendAlert(self, alertMessage, severity='low'):
        """
        A generic method for sending alerts from AgentWatchdogScanner
        :param alertMesasge: A string with the alert contents
        :param severity:     Default: 'low'
        NOTE: The rest of the alertAPI parameters come from the agent configuration at init time
        """
        currAgent = getattr(self.config.Agent, 'hostName')
        alertName = "AgentWatchdogScanner"
        summary = f"Alert from WMAgent {currAgent}"
        description = alertMessage
        service = f"AgentWatchdogScanner@{currAgent}"
        self.alertManager.sendAlert(alertName, severity, summary, description, service, tag=self.alertDestinationMap['alertAgentWatchdogScanner'])

    def checkCompAlive(self):
        """
        # Iterate through all components and check if they have a healthy process tree:
        # Restart any of them found to have problems.
        # NOTE: Exclude AgentWatchdog and itself from this logic, because:
        #       * AgentWatchdog should never be touched since it monitors all others poling cycles
        #       * AgentWatchdog is going to spawn threads dynamically and will fail all checks
        #       * AgentWatchdog cannot restart itself
        """
        logging.info(f"Checking all components' threads.")
        for component in [comp for comp in self.config.listComponents_() if comp != 'AgentWatchdog']:
            if not isComponentAlive(self.config, component=component):
                try:
                    forkRestart(self.config, componentsList=[component])
                    msg = f"Restarted Unhealthy component: {component}"
                    logging.warning(f"{msg}")
                    self.sendAlert(msg)
                except Exception as ex:
                    logging.error(f"Failed to restart component: {component}. Full ERROR: {str(ex)}")
                    raise

    def setup(self, parameters):
        # Wait one full cycle before starting the whole component to allow all others to initialize properly
        logging.info("Waiting for 10s to let others initialize properly ... ")

        time.sleep(10)
        logging.info("Start Monitoring the components' threads ... ")

    @timeFunction
    def algorithm(self, parameters=None):
        """
        Spawn and register watchdog timers
        """
        # TODO: To implement logic with limited number of restarts, similar to AgentWatchdogPoller
        try:
            currThread = threading.currentThread()
            logging.info(f"Polling cycle started with current pid: {currThread.native_id}.")
            self.checkCompAlive()
        except Exception as ex:
            logging.exception(f"Exception occurred while executing the AgentWatchdogScanner logic.")
