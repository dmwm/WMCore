"""
A simple watchdog mechanism based on OS signal handling, implemented to help in the monitoring
of the health of all WMAgent components by AgentStatusWatcher.

This watchdog is to be watching only AgentStatusPoller thread's run time overflow and will
take care of proper restarts only to this component, while at the same time AgentStatusPoller
is to be endowed with a mechanism to monitor the health of all other components' process trees
and take the proper (restart) actions in case of unhealthy component is found.

And we shall call this component with the affectionate nickname: Cerberus ;)
"""

import multiprocessing
import threading
import os
import signal
import logging
import time
import subprocess

from pprint import pformat

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
        self.mainThread = threading.currentThread()
        self.mainThread.setName('Cerberus')
        self.mainProcess = multiprocessing.current_process()

        # Register the expected signal handler:
        self.expectedSignal = signal.SIGCONT
        signal.signal(self.expectedSignal, self.resetTimer)

        self.watchedPid = None
        self.watchdogTimeout = self.config.AgentWatchdog.watchdogTimeout
        self.pollInterval = self.config.AgentWatchdog.pollInterval
        self.watchedComponents = self.config.AgentWatchdog.watchedComponents
        self.timers = {}

    def resetTimer(self, *args, **kwargs): # timerName=None, pid=None):
        """
        A simple call back function to be used for resetting timers in the AgentWatchdog class
        It will be actually redirecting the received signal to all of the currently running timers/threads,
        and only the one to whom this signal was intended would react, by recognizing the senders pid
        """
        myThread = threading.currentThread()
        logging.info(f"{myThread.name}: resetTimer args: {pformat(args)}")
        logging.info(f"{myThread.name}: resetTimer kwargs: {pformat(kwargs)}")
        logging.info(f"{myThread.name}: all timers: {pformat(self.timers)}")
        for timerName, timerThread in self.timers.items():
            logging.info(f"Redirecting signal to timer: {timerName}")
            os.kill(timerThread.native_id, self.expectedSignal)
        return True

    def _timer(self, expPid=None, compName=None, interval=None):
        """
        A simple timer function, which is to be started in a separate thread and is to
        expect one particular PID to reset it. It is supposed to be the one with the
        shortest polling interval. If the timer reaches to the end of its interval, without
        being reset, it would reset the component it is associated with.
        :param expPid:   The expected pid allowed to reset the timer, signals from anybody else would be ignored
        :param compName: The component name this timer is associated with
        :param interval: The interval or the timer
        :return:         Nothing
        """

        # Set the default expPid to the main thread's pid if None given
        # NOTE: This will be the case where all agent's components are not sending
        #       the reset signal directly to the timer's thread/process but rather to the
        #       main thread of AgentWatchdogPoller which is to redirect this signal to the
        #       correct timer.
        if not expPid:
            expPid = self.mainThread.native_id

        myThread = threading.currentThread()
        timerName = myThread.name
        if str(timerName) != str(compName):
            raise(NameError)

        startTime = time.time()
        endTime = startTime + interval

        while True:
            sigInfo = signal.sigtimedwait([self.expectedSignal], self._countdown(endTime))
            if sigInfo:
                logging.info(f"Timer: {timerName}: Received signal: {pformat(sigInfo)}")
                if sigInfo.si_pid == expPid:
                    # Resetting the timer starting again from the current time
                    logging.info(f"Timer: {timerName}: Resetting timer")
                    endTime = time.time() + interval
                else:
                    # Continue to wait for signal from the correct origin
                    logging.info(f"Timer: {timerName}: Continue to wait for signal from the correct origin")
                    continue
            else:
                logging.info(f"Timer: {timerName}: Reached the end of timer.")
                break


    def _countdown(self, endTime):
        """
        __countDown__

        Aux function to return the remaining time before reaching the endTime
        :param endTime: The end time in seconds since the epoch.
        :retun:         Remaining time in seconds or 0 if the endTime has already passed
                        No negative values are returned
        """
        remTime = endTime - time.time()
        if remTime < 0:
            remTime = 0
        return remTime

    def setupTimer(self, compName):
        """
        We spawn a separate thread for every timer
        """
        ## here to walk the pidTree of the component and find the pid with shortest polling interval

        ## here to find the correct polling interval and merge it with the watchdog timeout
        timerInterval = self.watchdogTimeout

        ## here to spawn the thread for the timer
        # NOTE: It is a must for the thread name and the compName to match,
        #       because latter the thread name will be used to identify the timer.
        timerThread = threading.Thread(target=self._timer, name=compName, args=(), kwargs={"compName": compName,
                                                                             "interval": timerInterval})
        # timerThread = multiprocessing.Process(target=self._timer , args=(), kwargs={"compName": compName,
        #                                                                             "interval": timerInterval})

        ## here to add the timer's thread to the timers list in the AgentWatchdog object
        self.timers[compName] = timerThread
        self.timers[compName].start()
        # self.timers[compName].join()

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
