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


class AgentWatchdogPoller(BaseWorkerThread):
    """
    A basic watchdog class
    """
    def  __init__(self, config):
        BaseWorkerThread.__init__(self)
        self.config = config
        # myThread = threading.currentThread()

        self.watchedPid = None
        self.watchdogTimeout = self.config.AgentWatchdog.watchdogTimeout
        self.pollInterval = self.config.AgentWatchdog.pollInterval
        self.watchedComponents = self.config.AgentWatchdog.watchedComponents
        self.timers = {}

    def resetTimer():
        """
        A simple call back function to be used for resetting timers in the AgentWatchdog class
        """
        return True

    def _timer(self, pid=None, compName=None, interval=None):
        """
        A simple timer function, which is to be started in a separate thread and is to
        expect one particular PID to reset it. It is supposed to be the one with the
        shortest polling interval. If the timer reaches to the end of its interval, without
        being reset, it would reset the component it is associated with.
        :param pid:      The pid allowed to reset the timer
        :param compName: The component name this timer is associated with
        :param interval: The interval or the timer
        :return:         Nothing
        """
        timerName = compName
        signal.signal(signal.SIGCONT, resetTimer)
        while interval:
            time.sleep(1)
            interval -= 1
            logging.info(f"timer {timerName}: {interval}")
        logging.info(f"Reached the end of timer: {timerName}")


    def setupTimer(self, compName):
        """
        We spawn a separate thread for every timer
        """
        ## here to walk the pidTree of the component and find the pid with shortest polling interval
        
        ## here to find the correct polling interval and merge it with the watchdog timeout
        
        ## here to spawn the thread for the timer
        timerThread = threading.Thread(target=self._timer , args=(), kwargs={"compName": compName, "interval": 10})

        ## here to add the timer's thread to the timers list in the AgentWatchdog object
        self.timers[compName] = timerThread
        self.timers[compName].start()
        self.timers[compName].join()

    @timeFunction
    def algorithm(self, parameters=None):
        """
        Spawn and register watchdog timers
        """
        logging.info(f"Component polling cycle started with the following list of watched components: {list(self.timers.keys())}")
        try:
            # pidTree = getComponentThreads(self.watchedComponent)
            logging.info(f"Sleeping for {self.pollInterval} secs")
            time.sleep(self.pollInterval)
            # logging.info(f"Restarting {self.watchedComponent}")
            # exitCode = forkRestart(componentsList=[self.watchedComponent], useWmcoreD=True)
            # exitCode = forkRestart(config=self.config, componentsList=[self.watchedComponent], useWmcoreD=False)
            # logging.info(f"Exit code from forkRestart of {self.watchedComponent}: {exitCode}")
        except Exception as ex:
            logging.error(f"Exception: {str(ex)}")

    def setup(self, parameters=None):
        """
        __setup__

        Setup the name of the component to be watched
        """
        for compName in self.watchedComponents:
            self.setWatchedComponent(compName)
        return

    def setWatchedPid(self, pid):
        """
        A simple function to add the watched component PID
        :param pid: The PID to be added
        """
        self.watchedPid = pid

    def setWatchedComponent(self, compName=None):
        """
        A simple function to add a watched component. We start a single timer per component
        And the component's thread with the shortest polling interval is supposed to reset the timer.
        :param compName: the component name to be watched (Default: AgentStatusWatcher')
        """
        logging.info(f"Start watching {compName}")
        self.setupTimer(compName)
