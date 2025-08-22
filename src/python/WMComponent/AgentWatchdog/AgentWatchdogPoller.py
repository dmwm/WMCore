"""
A simple watchdog mechanism based on OS signal handling, implemented to help in the monitoring
of the health of all WMAgent components.

This AgentWatchdogPoller thread is to be watching the thread's of all other components
for run time overflow and will take care of proper restarts of the misbehaved ones.
While at the same time the AgentWatchdogScanner is to be endowed with a mechanism
to monitor the health of all other components' process trees and take the proper
(restart) actions in case of unhealthy component is found.

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
import re

from pprint import pformat

from Utils.ProcFS import processStatus
from Utils.Timers import timeFunction
from Utils.wmcoreDTools import getComponentThreads, restart, forkRestart, isComponentAlive
from WMComponent.AgentWatchdog.Timer import Timer, _countdown, WatchdogAction, TimerException
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.WMInit import connectToDB


class AgentWatchdogPoller(BaseWorkerThread):
    """
    The basic AgentWatchdogPoller class
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
        self.actionLimit = self.config.AgentWatchdog.actionLimit
        self.timers = {}

        self.mainThread.parents = [ thread['pid'] for thread in processStatus(self.mainThread.native_id)]
        logging.info(f"{self.mainThread.name}: Initialized with parents {self.mainThread.parents}.")
        self.mainThread.enum = [ pid.native_id for pid in threading.enumerate() ]
        logging.info(f"{self.mainThread.name}: Initialized with the following threads: {self.mainThread.enum}.")
        self.mainThread.main = threading.main_thread().native_id
        logging.debug(f"{self.mainThread.name}: Initialized with main_thread {self.mainThread.main}.")

    def sigHandler(self, sigNum, currFrame, **kwargs):
        """
        A simple call back function to be used for resetting timers in the AgentWatchdog class
        It will be actually redirecting the received signal to all of the currently running timers/threads,
        and only the one to whom this signal was intended would react, by recognizing the senders pid
        """
        currThread = threading.currentThread()
        logging.info(f"{currThread.name}, pid: {currThread.native_id}, Asynchronous sigHandler for sigNum: {sigNum}")
        return True

    # def checkCompAlive(self):
    #     """
    #     # Iterate through all components and check if they have a healthy process tree:
    #     # Restart any of them found to have problems.
    #     # NOTE: Exclude AgentWatchdog and itself from this logic, because:
    #     #       * AgentWatchdog should never be touched since it monitors all others poling cycles
    #     #       * AgentWatchdog is going to spawn threads dynamically and will fail all checks
    #     #       * AgentWatchdog cannot restart itself
    #     """
    #     logging.info(f"Checking all components' threads:")
    #     for component in [comp for comp in self.config.listComponents_() if comp != 'AgentWatchdog']:
    #         if not isComponentAlive(self.config, component=component):
    #             try:
    #                 logging.warning(f"Restarting Unhealthy component: {component}")
    #                 forkRestart(self.config, componentsList=[component])
    #                 # rebuild the timer for this component upon restart
    #                 self.setupTimer(component)
    #             except Exception as ex:
    #                 logging.error(f"Failed to restart component: {component}. Full ERROR: {str(ex)}")
    #                 raise

    def restartUpdateAction(self, compName):
        """
        _restartUpdateAction_
        This is an action wrapper intended to act on both sides:
        * On the component side: executing forkRestart
        * On the AgentWatchdogPoller side: updating the respective timer's data
          reflecting the new component status
        :param *: Accepts all parameters valid for wmcoreDTolls.forkRestart
        """
        # TODO: * simplified  signature
        #       * reset action counter
        #       * call alert manager

        # Check the wrapped method signature
        # NOTE: If this is a wrapper accepting arbitrary set of arguments like:
        #       self.restartUpdateAction(self, *args, **kwArgs):
        #       when passed to the Timer class as a callback action, it will bypass the
        #       action signature check during the timer initialization. So it will need
        #       an the extra check here. Something like e.g.:
        #       try:
        #            actionSignature = inspect.signature(forkRestart)
        #            actionSignature.bind(*args, **kwArgs)
        #       except TypeError as ex:
        #            msg = f"The timer action wrapper method signature does not match the set of arguments provided. Error: {str(ex)}"
        #            raise TimerException(msg) from None
        #       The diff between those two checks (the one at the Timer class and this one)
        #       is only in the moment they are performed. This one here will be done
        #       during the timer's runtime when the action execution is attempted,
        #       while the former is performed during the timer's init time.

        # Execute fork Restart with the arguments provided to the wrapper
        forkRestart(config=self.config, componentsList=[compName])

        # Give the component some time to start:
        time.sleep(1)

        # Update the respective timer's data
        timer = self._findTimerByComp(compName)
        if timer:
            compPidTree = getComponentThreads(self.config, compName, quiet=True)
            if compPidTree:
                self.updateTimer(timer, compPidTree)

        # call alert manager
        

    def _findTimerByPid(self, pid):
        """
        _findTimerByPid_
        """
        correctTimer = None
        for timer in self.timers.values():
            # logging.debug(f"timer: {timer}")
            # logging.debug(f"timer.expPids: {timer.expPids}")
            if pid in timer.expPids:
                correctTimer = timer
                break
        return correctTimer

    def _findTimerByComp(self, compName):
        """
        _findTimerByComp
        """
        correctTimer = None
        for timer in self.timers.values():
            if timer.compName == compName:
                correctTimer = timer
                break
        return correctTimer

    def restartTimer(self, timer, compPidTree):
        """
        _restartTimer_
        """
        expPids = compPidTree['RunningThreads']
        expPids.append(compPidTree['Parent'])
        expPids.append(self.mainThread.native_id)
        # expPids.extend([thr.native_id for thr in threading.enumerate()])
        expPids = list(set(expPids))
        timer.restart(expPids=expPids)

    def updateTimer(self, timer, compPidTree):
        """
        _updateTimer_
        """
        expPids = compPidTree['RunningThreads']
        expPids.append(compPidTree['Parent'])
        expPids.append(self.mainThread.native_id)
        # expPids.extend([thr.native_id for thr in threading.enumerate()])
        expPids = list(set(expPids))
        timer.update(expPids=expPids)

    def setupTimer(self, compName):
        """
        We spawn a separate thread for every timer.
        :param compName: The component name this timer  is associated with.
        :return:         Nothing
        """
        currThread = threading.currentThread()
        logging.info(f"{self.mainThread.name}: setting up timer for component: {compName}.")

        # Here to walk the pidTree of the component and set all expected pids
        # which are to be allowed to reset the timer
        compPidTree = {}
        try:
            compPidTree = getComponentThreads(self.config, compName)
            logging.info(f"{self.mainThread.name}: Current Process tree for: {compName}: {compPidTree}")
        except Exception as ex:
            logging.error(f"Exception was thrown while rebuilding the the process tree for component: {compName}")
            logging.error(f"The full Error was : {str(ex)}")

        if not compPidTree:
            logging.error(f"Could not rebuild the the process tree for component: {compName}")
            logging.error(f"Giving up on timer creation for component: {compName}")
            return
        expPids = compPidTree['RunningThreads']
        expPids.append(compPidTree['Parent'])

        # Here to add the full set of possible origin pids due to the signal redirection
        # (current thread, main thread, parent thread, etc.)
        # NOTE: This will be needed in the case where all agent's components are not sending
        #       the reset signal directly to the timer's thread/process but rather to the
        #       main thread of AgentWatchdogPoller which is then to redirect this signal to the
        #       correct timer.
        expPids.append(self.mainThread.native_id)
        # expPids.extend([thr.native_id for thr in threading.enumerate()])
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
        compConfigSection = self.config.component_(compName)
        compPollIntervals = {attr: value
                             for attr,value in inspect.getmembers(compConfigSection)
                             if re.match(r"^.*[p,P]ollInterval$", attr)}

        # Take the shortest interval:
        shortestInterval = min(compPollIntervals)
        timerInterval = compPollIntervals[shortestInterval]
        logging.info(f"{self.mainThread.name}: selecting the shortest polling cycle in the component: {shortestInterval}:{timerInterval} sec.")

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

        # Create the action to be executed at the end of the timer.
        # NOTE: The format is a named tuple of the form:
        #       (callbackFunction, [args], {kwArgs})
        action = WatchdogAction(self.restartUpdateAction, [compName], {})
        logging.debug(f"{self.mainThread.name}: action function: {action}.")

        # Here to define the timer's path, where it will be permanently written on disk
        compDir = self.config.section_(compName).componentDir
        compDir = os.path.expandvars(compDir)
        timerPath = compDir + '/' + 'ComponentTimer'

        # Here to add the timer's thread to the timers list in the AgentWatchdog object
        self.timers[timerName] = Timer(name=timerName,
                                       compName=compName,
                                       expPids=expPids,
                                       action=action,
                                       actionLimit=self.actionLimit,
                                       path=timerPath,
                                       interval=timerInterval)

        # Finally start the timer:
        self.timers[timerName].start()

        # Here to preserve the timer on disk, such that its parameters can later be found by the
        # components itself, and it can be reset on time.
        # NOTE: This must happen upon timer's startup, because otherwise the timer attributes
        #       would be incomplete and later the component won't be able to reset it.
        self.timers[timerName].write()

        # NOTE: The so preserved timer would reflect only the state of the timer
        #       at init time, and won't be updated later until recreated. So any
        #       dynamic property like endTime, remTime may not reflect the real
        #       state of the timer as it is currently in memory.
        # DONE: To implement a mechanism for rewriting the timer's content/state
        #       upon receiving a signal.SIGCONT. This way the main thread can update
        #       the contents of its timers on a regular basis(e.g. with a rate depending
        #       on the AgentWatchDog polling cycle).


    @timeFunction
    def algorithm(self, parameters=None):
        """
        Spawn and register watchdog timers
        """
        currThread = threading.currentThread()

        startTime = time.time()
        endTime = startTime + self.pollInterval

        # Check all components' health:
        # DONE: To move it in a separate thread, not to mess up with the blocking calls
        #       for refreshing the timers data on disk bellow. And here, mess up means delaying,
        #       because the calls to wmcoreD.isComponentAlive have non zero runtime)
        #       The example code bellow does not allow to rebuild the newly restarted
        #       components timer, since the timer lives in the main thread not in the child
        # #compAliveThread = threading.Thread(target=self.checkCompAlive, name="ComponentsWatcher")
        # #compAliveThread.start()
        # self.checkCompAlive()

        logging.info(f"{self.mainThread.name}: Polling cycle started with current pid: {currThread.native_id}.")
        logging.debug(f"{self.mainThread.name}: Main pid from threading module: {threading.main_thread().native_id}, main pid at startup: {self.mainThread.native_id}, list of watched components: {list(self.timers.keys())}")
        logging.info(f"{self.mainThread.name}: Checking and Re-configuring previously expired timers for components with changed running state.")
        logging.debug(f"{self.mainThread.name}: All current threads: {[thr.native_id for thr in  threading.enumerate()]}.")

        # Refresh timers:
        for compName in self.watchedComponents:
            timer = self._findTimerByComp(compName)
            if timer:
                # Re-configuring any timers associated with components which changed execution or have been restarted:
                compPidTree = getComponentThreads(self.config, compName, quiet=True)
                if compPidTree and compPidTree['Parent'] not in timer.expPids:
                    if timer.is_alive():
                        logging.info(f"{self.mainThread.name}: Re-configuring timer: {timer}, whose associated component has changed state or have been restarted.")
                        self.updateTimer(timer, compPidTree)
                    else:
                        logging.info(f"{self.mainThread.name}: Restarting expired timer: {timer}, whose associated component has changed state or have been restarted.")
                        self.restartTimer(timer, compPidTree)
            else:
                # Retrying to create timers for components present in the watched list
                # but previously failed during initialization:
                logging.warning(f"Trying to recreate a previously failed timer for component: {compName}")
                self.setupTimer(compName)

        # Refresh all timers' data on disk every 1 second, or when a signal.SIGCONT
        # is received from a particular component.
        while _countdown(endTime):
            sigInfo = signal.sigtimedwait([self.expectedSignal], 1)
            if sigInfo:
                # Refreshing a particular timer data on disk depending on the sender's PID:
                logging.info(f"{self.mainThread.name} with main pid: {threading.main_thread().native_id}: Received signal: {pformat(sigInfo)}")

                # First, find the correct timer:
                correctTimer = self._findTimerByPid(sigInfo.si_pid)

                # Second, Refresh timer data:
                if correctTimer:
                    try:
                        logging.info(f"{self.mainThread.name}: Refreshing timer data on disk for: {correctTimer.name}")
                        logging.debug(f"{self.mainThread.name}: Timer expPids: {correctTimer.expPids}")
                        correctTimer.write()
                    except ProcessLookupError:
                        logging.warning(f"{self.mainThread.name}: Missing timer: {correctTimer.name}. It will be recreated on the next AgentWatchdogPoller cycle, but the current signal is lost and the timer data was NOT refreshed on disk.")
                else:
                    # Ignore signals from unknown origin:
                    logging.info(f"{self.mainThread.name}: The sender's pid: {sigInfo.si_pid} was not recognized. Ignoring the signal.")
                    continue
            else:
                logging.debug(f"{self.mainThread.name}: Refreshing all timers' data on disk.")
                for timer in self.timers.values():
                    timer.write()

        logging.info(f"{self.mainThread.name}: Reached the end of its polling cycle.")


        # NOTE: This whole logic bellow is for the case where we use signal redirection from the main AgentWatchdog thread
        #       to the timers. Currently we already took the path for communicating the timers' data through files
        #       so this mechanism is no longer needed and commented out for historical purposes. The logic
        #       is refurbished and reused (in the above paragraph) for the mechanism where the main thread is about to refresh all timers'
        #       data on disk upon receiving a signal.SIGCONT (instead of finding the correct timer and resetting it)

        # while True:
        #     sigInfo = signal.sigtimedwait([self.expectedSignal], _countdown(endTime))
        #     if sigInfo:
        #         # Resetting the correct timer:
        #         logging.info(f"{self.mainThread.name} with main pid: {threading.main_thread().native_id}: Received signal: {pformat(sigInfo)}")

        #         # First, find the correct timer:
        #         correctTimer = None
        #         for timer in self.timers.values():
        #             # logging.debug(f"timer: {timer}")
        #             # logging.debug(f"timer.expPids: {timer.expPids}")
        #             if sigInfo.si_pid in timer.expPids:
        #                 correctTimer = timer
        #                 break

        #         # Second, redirect the signal to the timer's thread:
        #         if correctTimer:
        #             # try to reset it:
        #             try:
        #                 logging.info(f"{self.mainThread.name}: Resetting timer: {correctTimer.name}")
        #                 logging.info(f"{self.mainThread.name}: Timer expPids: {correctTimer.expPids}")
        #                 logging.info(f"{self.mainThread.name}: sending signal from: {currThread.native_id}")
        #                 logging.info(f"{self.mainThread.name}: sending signal to: {correctTimer.native_id}")
        #                 os.kill(correctTimer.native_id, self.expectedSignal)
        #             except ProcessLookupError:
        #                 logging.warning(f"{self.mainThread.name}: Missing timer: {correctTimer.name}. It will be recreated on the next AgentWatchdogPoller cycle, but the current signal is lost and the component may be restarted soon.")
        #         else:
        #             # Ignore signals from unknown origin:
        #             logging.info(f"{self.mainThread.name}: The sender's pid: {sigInfo.si_pid} was not recognized. Ignoring the signal")
        #             continue
        #     else:
        #         logging.info(f"{self.mainThread.name}: Reached the end of polling cycle.")
        #         break


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
