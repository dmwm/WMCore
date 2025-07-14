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
import json

from pprint import pformat

from Utils.ProcFS import processStatus
from Utils.Timers import timeFunction
from Utils.wmcoreDTools import getComponentThreads, restart, forkRestart
from WMComponent.AgentWatchdog.Timer import Timer, _countdown, actionTuple
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.WMInit import connectToDB


def tReset(configFile, compName, tPid=None, tHash=None):
    """
    _tReset_

    Resets a given watchdog timer. The timer can be identified by component name or by the timer's PID

    :param configFile:     Either path to the WMAgent configuration file or a WMCore.Configuration instance.
    :param compName:       The name of the component this timer is associated with. This also determines
                           the place where the component's timer will be searched for.
    :param tPid:           The timer PID, if known in advance. To be used for extra identification. (Default: None)
    :param tHash:          The timer Hash, if known in advance. To be used for extra identification. (Default: None)
    :return:               int ExitCode - 0 in case of success, nonzero value otherwise
    """

    exitCode = 0
    if isinstance(configFile, Configuration):
        config = configFile
    else:
        config = loadConfigurationFile(configFile)

    compDir = config.section_(compName).componentDir
    compDir = os.path.expandvars(compDir)


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
        signal.signal(self.expectedSignal, self.sigHandler)

        self.watchedPid = None
        self.watchdogTimeout = self.config.AgentWatchdog.watchdogTimeout
        self.pollInterval = self.config.AgentWatchdog.pollInterval
        self.watchedComponents = self.config.AgentWatchdog.watchedComponents
        self.timers = {}
        # self.mainThread.parent = getComponentThreads(self.config, "AgentWatchdog")['Parent']
        self.mainThread.parents = [ thread['pid'] for thread in processStatus(self.mainThread.native_id)]
        logging.info(f"{self.mainThread.name}: Initialized with parents {self.mainThread.parents}.")
        self.mainThread.enum = [ pid.native_id for pid in threading.enumerate() ]
        logging.info(f"{self.mainThread.name}: Initialized with enum threads  {self.mainThread.enum}.")
        self.mainThread.main = threading.main_thread().native_id
        logging.info(f"{self.mainThread.name}: Initialized with main_thread {self.mainThread.main}.")

    def sigHandler(self, sigNum, currFrame, **kwargs):
        """
        A simple call back function to be used for resetting timers in the AgentWatchdog class
        It will be actually redirecting the received signal to all of the currently running timers/threads,
        and only the one to whom this signal was intended would react, by recognizing the senders pid
        """
        currThread = threading.currentThread()
        logging.info(f"{currThread.name}, pid: {currThread.native_id}, sigHandler for sigNum: {sigNum}")
        return True

    def setupTimer(self, compName):
        """
        We spawn a separate thread for every timer.
        :param compName: The component name this timer  is associated with.
        :return:         Nothing
        """
        currThread = threading.currentThread()
        logging.info(f"{currThread.name}, pid: {currThread.native_id}, setting up timer for component: {compName}.")

        # Here to walk the pidTree of the component and set all expected pids
        # which are to be allowed to reset the timer
        compPidTree = getComponentThreads(self.config, compName)
        expPids = compPidTree['RunningThreads']

        # Here to add the full set of possible origin pids due to the signal redirection
        # (current thread, main thread, parent thread, etc.)
        # NOTE: This will be needed in the case where all agent's components are not sending
        #       the reset signal directly to the timer's thread/process but rather to the
        #       main thread of AgentWatchdogPoller which is then to redirect this signal to the
        #       correct timer.
        expPids.append(self.mainThread.native_id)
        expPids.extend([thr.native_id for thr in threading.enumerate()])
        expPids = list(set(expPids))

        # Here to find the correct timer's interval
        # NOTE: We estimate the timer's interval by finding the pid with the shortest polling cycle and:
        #       * Merge it with the watchdog timeout, in order to implement some static
        #         hysteresis in the watchdog logic. In the future instead of the shortest polling cycle
        #         (which does not reflect how long a component has run, but rather for how long we wait
        #         between component runs), we should use an estimator based on the runtime distribution
        #         of the slowest thread in the component.
        #       * Add some random factor between 1-10% on top of it, in order to avoid periodical
        #         overlaps between the timer's interval and the component's polling cycle,
        #         which would cause oscillations (the component being periodically rebooted due
        #         to lost signals caused by the intervals overlaps explained above)
        timerInterval = 0

        # Now lets add some disturbance to the force:
        timerInterval += self.watchdogTimeout
        timerInterval *= random.uniform(1.01, 1.1)

        # Here to spawn the thread for the timer
        # NOTE: For the time being, it is a must the thread name and the compName to match,
        #       because later the thread name will be used to identify the timer.
        #       In the future, if we move to spawning a timer per component's thread, then
        #       we might end up with multiple timers per component, and those two may differ.
        timerName = compName
        # timerThread = threading.Thread(target=self._timer, name=timerName, args=(), kwargs={"compName": compName,
        #                                                                                     "interval": timerInterval})
        # timerThread = multiprocessing.Process(target=self._timer , args=(), kwargs={"compName": compName,
        #                                                                             "interval": timerInterval})

        # Create the action description to be executed at the end of the timer.
        action = actionTuple(forkRestart, [], {'config': self.config, 'componentsList': [compName]})
        logging.debug(f"{currThread.name}, pid: {currThread.native_id}, action function: {action}.")

        # Here to add the timer's thread to the timers list in the AgentWatchdog object
        self.timers[timerName] = Timer(name=timerName,
                                       compName=compName,
                                       expPids=expPids,
                                       action=action,
                                       interval=timerInterval)

        self.timers[timerName].start()

        # Here to preserve the timer on disk, such that its parameters can later be found by the
        # components itself, and it can be reset on time.
        # NOTE: This must happen upon timer's startup, because otherwise the timer attributes
        #       would be incomplete and later the component won't be able to reset it.
        compDir = self.config.section_(compName).componentDir
        compDir = os.path.expandvars(compDir)
        timerPath = compDir + '/' + 'ComponentTimer'
        # NOTE: The so preserved timer would reflect only the state of the timer
        #       at init time, and won't be updated later until recreated. So any
        #       dynamic property like endTime, remTime may not reflect the real
        #       state of the timer as it is currently in memory.
        # TODO: To implement a mechanism for dumping the timer's content/state
        #       upon receiving a signal.SIGUSR1. This way the main thread can update
        #       the contents of its timers on a regular basis(e.g. with a rate depending
        #       on the AgentWatchDog polling cycle).
        with open(timerPath, 'w') as timerFile:
            json.dump(self.timers[timerName].dictionary_(), timerFile , indent=4)

    @timeFunction
    def algorithm(self, parameters=None):
        """
        Spawn and register watchdog timers
        """
        currThread = threading.currentThread()

        startTime = time.time()
        endTime = startTime + self.pollInterval

        # logging.info(f"{self.mainThread.name} with main pid: {self.mainThread.native_id} and current pid: {currThread.native_id} : Full pidTree: {pformat(psutil.Process(self.mainThread.native_id).threads())}")
        logging.info(f"{self.mainThread.name}: Polling cycle started with current pid: {currThread.native_id}, main pid: {threading.main_thread().native_id}, list of watched components: {list(self.timers.keys())}")
        logging.info(f"{self.mainThread.name}: Checking and Re-configuring previously expired timers.")
        logging.info(f"{self.mainThread.name}: Current enum: {[pid.native_id for pid in  threading.enumerate()]}.")
        logging.info(f"{self.mainThread.name}: Current threads: {[th.id for th in psutil.Process(threading.main_thread().native_id).threads()]}.")

        for timer in self.timers:
            if not self.timers[timer].is_alive():
                logging.info(f"{self.mainThread.name}: Re-configuring expired timer: {timer}.")
                self.setupTimer(timer)

        while True:
            sigInfo = signal.sigtimedwait([self.expectedSignal], _countdown(endTime))
            if sigInfo:
                # Resetting the correct timer:
                logging.info(f"{self.mainThread.name} with main pid: {threading.main_thread().native_id}: Received signal: {pformat(sigInfo)}")

                # First, find the correct timer:
                correctTimer = None
                for timer in self.timers.values():
                    # logging.info(f"timer: {timer}")
                    # logging.info(f"timer.expPids: {timer.expPids}")
                    if sigInfo.si_pid in timer.expPids:
                        correctTimer = timer
                        break

                # Second, redirect the signal to the timer's thread:
                if correctTimer:
                    # try to reset it:
                    try:
                        logging.info(f"{self.mainThread.name}: Resetting timer: {correctTimer.name}")
                        logging.info(f"{self.mainThread.name}: Timer expPids: {correctTimer.expPids}")
                        logging.info(f"{self.mainThread.name}: sending signal from: {currThread.native_id}")
                        logging.info(f"{self.mainThread.name}: sending signal to: {correctTimer.native_id}")
                        os.kill(correctTimer.native_id, self.expectedSignal)
                    except ProcessLookupError:
                        logging.warning(f"{self.mainThread.name}: Missing timer: {correctTimer.name}. It will be recreated on the next AgentWatchdogPoller cycle, but the current signal is lost and the component may be restarted soon.")
                else:
                    # Ignore signals from unknown origin:
                    logging.info(f"{self.mainThread.name}: The sender's pid: {sigInfo.si_pid} was not recognized. Ignoring the signal")
                    continue
            else:
                logging.info(f"{self.mainThread.name}: Reached the end of polling cycle.")
                break


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
