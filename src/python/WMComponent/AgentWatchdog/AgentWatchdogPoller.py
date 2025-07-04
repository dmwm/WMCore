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
import psutil
import inspect
import random

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

    def resetTimer(self, sigNum, currFrame, **kwargs):
        """
        A simple call back function to be used for resetting timers in the AgentWatchdog class
        It will be actually redirecting the received signal to all of the currently running timers/threads,
        and only the one to whom this signal was intended would react, by recognizing the senders pid
        """
        return True
        # myThread = threading.currentThread()
        # sigInfo = {}
        # # sigInfo = signal.sigwaitinfo([self.expectedSignal])
        # logging.info(f"{myThread.name} with pid:{myThread.native_id}: resetTimer sigNum: {sigNum}")
        # logging.info(f"{myThread.name} with pid:{myThread.native_id}: resetTimer currFrame: {currFrame}")
        # logging.info(f"{myThread.name} with pid:{myThread.native_id}: resetTimer kwargs: {pformat(kwargs)}")
        # logging.info(f"{myThread.name} with pid:{myThread.native_id}: all timers: {pformat(self.timers)}")
        # logging.info(f"{myThread.name} with pid:{myThread.native_id}: received signal info: {pformat(sigInfo)}")
        # logging.info(f"{myThread.name} with pid:{myThread.native_id}: current signal queue: {pformat(signal.sigpending())}")

        # if inspect.isframe(currFrame):
        #     logging.info(f"{myThread.name} with pid:{myThread.native_id}: current stack frame contents: {inspect.getframeinfo(currFrame)}")    
        # for timerName, timerThread in self.timers.items():
        #     logging.info(f"Redirecting signal to timer: {timerName}")
        #     try:
        #         # signal.pthread_kill(timerThread.native_id, self.expectedSignal)
        #         os.kill(timerThread.native_id, self.expectedSignal)
        #     except ProcessLookupError:
        #         logging.warning(f"Missing timer: {timerName}. It will be recreated on the next AgentWatchdogPoller cycle.")
        # return True

    def _timer(self, expPids=[], compName=None, interval=None):
        """
        A simple timer function, which is to be started in a separate thread and is to
        expect one particular PID to reset it. It is supposed to be the one with the
        shortest polling interval. If the timer reaches to the end of its interval, without
        being reset, it would reset the component it is associated with.
        :param expPids:  The list of expected pids allowed to reset the timer, signals from anybody else would be ignored
        :param compName: The component name this timer is associated with
        :param interval: The interval or the timer
        :return:         Nothing
        """

        # Add by default the main thread's pid to the expPids list
        # NOTE: This will be needed in the case where all agent's components are not sending
        #       the reset signal directly to the timer's thread/process but rather to the
        #       main thread of AgentWatchdogPoller which is then to redirect this signal to the
        #       correct timer.
        expPids.append(self.mainThread.native_id)

        myThread = threading.currentThread()
        timerName = myThread.name
        if str(timerName) != str(compName):
            raise(NameError)

        startTime = time.time()
        endTime = startTime + interval

        while True:
            sigInfo = signal.sigtimedwait([self.expectedSignal], self._countdown(endTime))
            if sigInfo:
                logging.info(f"Timer: {timerName} with pid: {myThread.native_id} : Received signal: {pformat(sigInfo)}")
                if sigInfo.si_pid in expPids:
                    # Resetting the timer starting again from the current time
                    logging.info(f"Timer: {timerName} with pid: {myThread.native_id} : Resetting timer")
                    endTime = time.time() + interval
                else:
                    # Continue to wait for signal from the correct origin
                    logging.info(f"Timer: {timerName} with pid: {myThread.native_id} : Continue to wait for signal from the correct origin. Remaining time: {self._countdown(endTime)}")
                    continue
            else:
                logging.info(f"Timer: {timerName} with pid: {myThread.native_id} : Reached the end of timer.")
                break


    def _countdown(self, endTime):
        """
        __countdown__

        Aux function to return the remaining time before reaching the endTime
        :param endTime: The end time in seconds since the epoch.
        :return:        Remaining time in seconds or 0 if the endTime has already passed
                        No negative values are returned
        """
        remTime = endTime - time.time()
        if remTime < 0:
            remTime = 0
        return remTime

    def setupTimer(self, compName):
        """
        We spawn a separate thread for every timer.
        :param compName: The component name this timer  is associated with.
        :return:         Nothing
        """
        # Here to walk the pidTree of the component and set all expected pids
        # which are to be allowed to reset the timer
        compPidTree = getComponentThreads(self.config, compName)
        expPids = compPidTree['RunningThreads']

        # Here to add the full set of possible origin pids due to the signal redirection
        # (current thread, main thread, parent thread, etc.)
        expPids.append(2840827)

        # Here to find the correct timer's interval
        # NOTE: We estimate the timer's interval by finding the pid with the shortest polling cycle and:
        #       * Merge it with the watchdog timeout, in order to implement some static
        #         hysteresis in the watchdog logic
        #       * Add some random factor between 1-10% on top of it, in order to avoid periodical
        #         overlaps between the timer's interval and the component's polling cycle,
        #         which would cause oscillations (the component being periodically rebooted due
        #         to lost signals caused by the intervals overlaps explained above)
        timerInterval = 0
        timerInterval += self.watchdogTimeout
        timerInterval *= random.uniform(1.01, 1.1)

        # Here to spawn the thread for the timer
        # NOTE: For the time being, it is a must the thread name and the compName to match,
        #       because later the thread name will be used to identify the timer.
        #       In the future, if we move to spawning a timer per component's thread, then
        #       we might end up with multiple timers per component, and those two may differ.
        timerName = compName
        timerThread = threading.Thread(target=self._timer, name=timerName, args=(), kwargs={"compName": compName,
                                                                                            "interval": timerInterval})
        # timerThread = multiprocessing.Process(target=self._timer , args=(), kwargs={"compName": compName,
        #                                                                             "interval": timerInterval})

        ## here to add the timer's thread to the timers list in the AgentWatchdog object
        self.timers[timerName] = {}
        self.timers[timerName]['expPids'] = expPids
        self.timers[timerName]['name'] = timerName
        self.timers[timerName]['thread'] = timerThread
        self.timers[timerName]['thread'].start()

    @timeFunction
    def algorithm(self, parameters=None):
        """
        Spawn and register watchdog timers
        """
        startTime = time.time()
        endTime = startTime + self.pollInterval
        currThread = threading.currentThread()
        logging.info(f"{self.mainThread.name} with main pid: {self.mainThread.native_id} and current pid: {currThread.native_id} : Full pidTree: {pformat(psutil.Process(self.mainThread.native_id).threads())}")

        while True:
            logging.info(f"{self.mainThread.name} with main pid: {self.mainThread.native_id} and current pid: {currThread.native_id} : Polling cycle started with the following list of watched components: {list(self.timers.keys())}")

            sigInfo = signal.sigtimedwait([self.expectedSignal], self._countdown(endTime))
            if sigInfo:
                # Resetting the correct timer:
                logging.info(f"{self.mainThread.name}: Received signal: {pformat(sigInfo)}")

                # First, find the correct timer:
                correctTimer=None
                for _, timer in self.timers.items():
                    if sigInfo.si_pid in timer['expPids']:
                        correctTimer = timer
                        break

                # Second, redirect the signal to the timer's thread:
                if correctTimer:
                    # try to reset it:
                    try:
                        logging.info(f"{self.mainThread.name}: Resetting timer: {correctTimer['name']}")
                        os.kill(correctTimer['thread'].native_id, self.expectedSignal)
                    except ProcessLookupError:
                        logging.warning(f"{self.mainThread.name}: Missing timer: {correctTimer['name']}. It will be recreated on the next AgentWatchdogPoller cycle, but the current signal is lost and the component may be restarted soon.")
                else:
                    # Ignore signals from unknown origin:
                    logging.info(f"{self.mainThread.name}: The sender's pid: {sigInfo.si_pid} was not recognized. Ignoring the signal")

            else:
                logging.info(f"{self.mainThread.name}: Reached the end of polling cycle. Re-configuring expired timers.")
                endTime = time.time() + self.pollInterval
                for timer in self.timers:
                    if not self.timers[timer]['thread'].is_alive():
                        logging.info(f"{self.mainThread.name}: Re-configuring expired timer: {timer}.")
                        self.setupTimer(timer)


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
