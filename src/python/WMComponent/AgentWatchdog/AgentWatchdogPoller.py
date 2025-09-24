"""
A simple watchdog mechanism based on OS signal handling, implemented to help in the monitoring
of the health of all WMAgent components.

This AgentWatchdogPoller thread is to be watching the thread's of all other components
for run time overflow and will take care of proper restarts of the misbehaved ones.
While at the same time the AgentWatchdogScanner is to be endowed with a mechanism
to monitor the health of all other components' process trees and take the proper
(restart) actions in case of unhealthy component is found.


Here bellow follows the timeline of the interaction between a component's thread and a timer:

Timer duration = pollInt + runTimeEst*corrFactor + extraTimeout + random add on between 0-10%

          t --->

          ...---|--- One polling cycle -----|-One polling cycle-|------- One polling cycle ----------|---...
                |                           |                   |                                    |
Thread:   ...---o-------------------o-------o-----------o-------o----------------------------o-------o---...
                |--------var--------| const |----var----| const |-------------var------------| const |---...
                |       runtime     |pollInt|  runtime  |pollInt|      runtime               |pollInt|
                                    |                   |                                    |
                                    |(tResetEvent)      |                           ^        |(tRESTARTEvent)
                                    |                   |(tResetEvent)              |        |
Timer:          o------ timer ------|.........o         |                           |        o------ timer ----------------o
                                    V                   |                           |
                                    o------ timer ------|..........o                |(actionEvent)
                                                        V                           |
                                                        o--------- timer -----------o


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
from Utils.wmcoreDTools import getComponentThreads, _getConfigSubsections, restart, forkRestart, isComponentAlive, getThreadConfigSections
from WMComponent.AgentWatchdog.Timer import Timer, _countdown, WatchdogAction, TimerException
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.WMInit import connectToDB
from WMCore.Services.AlertManager.AlertManagerAPI import AlertManagerAPI


class AgentWatchdogPoller(BaseWorkerThread):
    """
    The basic AgentWatchdogPoller class
    """
    def  __init__(self, config):
        BaseWorkerThread.__init__(self)
        self.config = config
        self.mainThread = threading.currentThread()
        self.mainThread.setName('Cerberus')

        # Register the expected signal handler:
        self.expectedSignal = signal.SIGCONT
        signal.signal(self.expectedSignal, self.sigHandler)

        self.watchedPid = None
        self.watchdogTimeout = self.config.AgentWatchdog.watchdogTimeout
        self.pollInterval = self.config.AgentWatchdog.AgentWatchdogPoller.pollInterval
        self.watchedComponents = self.config.AgentWatchdog.watchedComponents
        self.runTimeCorrFactor = self.config.AgentWatchdog.runTimeCorrFactor
        self.actionLimit = self.config.AgentWatchdog.actionLimit
        self.timers = {}

        self.mainThread.parents = [ thread['pid'] for thread in processStatus(self.mainThread.native_id)]
        logging.info(f"Initialized with parents {self.mainThread.parents}.")
        self.mainThread.enum = [ pid.native_id for pid in threading.enumerate() ]
        logging.info(f"Initialized with the following threads: {self.mainThread.enum}.")
        self.mainThread.main = threading.main_thread().native_id
        logging.debug(f"Initialized with main_thread {self.mainThread.main}.")

        self.alertManagerUrl = self.config.Alert.alertManagerUrl
        self.alertDestinationMap = self.config.Alert.alertDestinationMap
        self.alertManager = AlertManagerAPI(self.alertManagerUrl)
        logging.info(f"Setting up an alertManager instance for AgentWatchdogPoller and redirecting alerts to: {self.alertManagerUrl}.")

    def sigHandler(self, sigNum, currFrame, **kwargs):
        """
        A simple call back function to be used for resetting timers in the AgentWatchdog class
        It will be actually redirecting the received signal to all of the currently running timers/threads,
        and only the one to whom this signal was intended would react, by recognizing the senders pid
        """
        currThread = threading.currentThread()
        logging.info(f"{currThread.name}, pid: {currThread.native_id}, Asynchronous sigHandler for sigNum: {sigNum}")
        return True

    def sendAlert(self, alertMessage, severity='low'):
        """
        A generic method for sending alerts from AgentWatchdogPoller
        :param alertMesasge: A string with the alert contents
        :param severity:     Default: 'low'
        NOTE: The rest of the alertAPI parameters come from the agent configuration at init time
        """
        currAgent = getattr(self.config.Agent, 'hostName')
        alertName = "AgentWatchdogPoller"
        summary = f"Alert from WMAgent {currAgent}"
        description = alertMessage
        service = f"AgentWatchdogPoller@{currAgent}"
        self.alertManager.sendAlert(alertName, severity, summary, description, service, tag=self.alertDestinationMap['alertAgentWatchdogPoller'])

    def alertAction(self, timerName):
        """
        A wrapper method to create an alert action for any of the WMAgent Watchdog timers
        :param timerName: The timer instance as linked at self.timers[<TimerName>]
        """
        timer = self.timers[timerName]

        # Call alert manager
        msg = f"alertAction from: {timer.name}: The component {timer.compName} has been running beyond its estimated runtime with a big margin."
        msg += f"This might be a signal for a badly performing component thread and might need attention."
        logging.warning(msg)
        self.sendAlert(msg)

    def restartUpdateAction(self, compName):
        """
        _restartUpdateAction_
        This is an action wrapper intended to act on both sides:
        * On the component side: executing forkRestart
        * On the AgentWatchdogPoller side: updating the respective timer's data
          reflecting the new component status
        :param *: Accepts all parameters valid for wmcoreDTolls.forkRestart
        """
        # DONE: * simplified  signature
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
        timer = self._findTimersByComp(compName)
        if timer:
            compPidTree = getComponentThreads(self.config, compName, quiet=True)
            if compPidTree:
                self.updateTimer(timer, compPidTree)

        # Call alert manager
        msg = f"A restartUpdateAction has been performed by {timer.name}."
        logging.warning(msg)
        self.sendAlert(msg)

    def _findTimersByPid(self, pid):
        """
        _findTimersByPid_
        """
        correctTimers = []
        for timer in self.timers.values():
            # logging.debug(f"timer: {timer}")
            # logging.debug(f"timer.expPids: {timer.expPids}")
            if pid in timer.expPids:
                correctTimers.append(timer)
                # NOTE: Once we move to one timer per thread (meaning many timers per compName),
                #       we should not break here, but rather return the full list of timers
                #       found to be associated with this component.
        return correctTimers

    def _findTimersByComp(self, compName):
        """
        _findTimersByComp
        """
        correctTimers = []
        for timer in self.timers.values():
            if timer.compName == compName:
                correctTimers.append(timer)
                # NOTE: Once we move to one timer per thread (meaning many timers per compName),
                #       we should not break here, but rather return the full list of timers
                #       found to be associated with this component.
        return correctTimers

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

    def setupTimer(self, compName, threadConfigSection, timerName=None):
        """
        Crates a timer from a given thread configuration section and associates it with
        the respective component.
        :param compName:            The component name.
        :param threadConfigSection: The configuration section of the thread for which this timer is about to be created
        :param timerName:           The threadName for this timer. If missing, it will be named after
                                    the threadConfigSection itself. (Default: None)
        :return:                    A Watchdog.Timer instance
        """
        timerName = timerName or threadConfigSection.getInternalName()

        logging.info(f"Creating timer: {timerName}")

        # Here to walk the pidTree of the component and set all expected pids
        # which are to be allowed to reset the timer
        compPidTree = {}
        try:
            compPidTree = getComponentThreads(self.config, compName)
            logging.info(f"Current Process tree for: {compName}: {compPidTree}")
        except Exception as ex:
            logging.error(f"Exception was thrown while rebuilding the the process tree for component: {compName}")
            logging.error(f"The full Error was : {str(ex)}")

        if not compPidTree:
            logging.error(f"Could not rebuild the the process tree for component: {compName}")
            logging.error(f"Giving up on timer creation for component: {compName}")
            return None

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
        # NOTE: We estimate the timer's interval by:
        #       * Taking the initial runtime estimate from the configuration SubSection for the component's thread/process
        #       * Multiply it with the common correction factor defined for all threads at the AgentWatchodog configurartion
        #       * Merge it with the pollInterval from the respective config SubSection or if none is found there
        #         with the pollInterval for the pid with the longest polling cycle from the component's config Section itself
        #       * Merge it with the watchdog timeout, in order to implement some static
        #         hysteresis in the watchdog logic. In the future instead of the shortest polling cycle
        #         (which does not reflect how long a component has run, but rather for how long we wait
        #         between component runs), we should use an estimator based on the runtime distribution
        #         of the slowest thread in the component.
        #       * Add some random factor between 1-10% on top of it, in order to avoid periodical
        #         overlaps between the timer's interval and the component's polling cycle,
        #         which would cause oscillations (the component being periodically rebooted due
        #         to lost signals caused by the intervals overlaps explained above)

        # Find the pre-configured runtimeEstimate
        threadRunTimeEst = getattr(threadConfigSection, 'runTimeEst', None)
        # Give up. No runtime estimate can be found for this thread
        if not threadRunTimeEst:
            logging.error(f"Failed to estimate the pollInterval for the timer: {timerName}. This one will be skipped")
            return None

        # Apply the safety margin:
        threadRunTimeEst *= self.runTimeCorrFactor

        # Find the correct poll interval for this thread
        # TODO: To find the compConfigSection this threadConfigSection to
        threadPollInterval = getattr(threadConfigSection, 'pollInterval', None)
        if not threadPollInterval:
            # Take the longest interval:
            compPollIntervals = {attr: value
                                 for attr,value in inspect.getmembers(compConfigSection)
                                 if re.match(r"^.*[p,P]ollInterval$", attr)}
            try:
                maxPollIntervalName = max(compPollIntervals)
                threadPollInterval = compPollIntervals[maxPollIntervalName]
                logging.info(f"Selecting the longest polling interval in the component: {maxPollIntervalName}:{threadPollInterval} sec.")
            except (ValueError, KeyError) as ex:
                threadPollInterval = None
                logging.error(f"No pollIntervals defined for component: {compName}")

        # Give up after the final attempt to estimate the pooll interval
        if not threadPollInterval:
            logging.error(f"Failed to estimate the pollInterval for the timer: {timerName}. This one will be scipped")
            return None

        # Set the timerInterval as the sum of the previous two
        timerInterval = threadRunTimeEst + threadPollInterval

        # Now lets add some disturbance to the force:
        timerInterval += self.watchdogTimeout
        timerInterval *= random.uniform(1.01, 1.1)

        # Create the action to be executed at the end of the timer.
        # NOTE: The format is a named tuple of the form:
        #       (callbackFunction, [args], {kwArgs})
        # action = WatchdogAction(self.restartUpdateAction, [compName], {})
        action = WatchdogAction(self.alertAction, [timerName], {})
        logging.debug(f"action function: {action}.")

        # Here to define the timer's path, where it will be permanently written on disk
        compDir = self.config.section_(compName).componentDir
        compDir = os.path.expandvars(compDir)
        timerPath = f"{compDir}/Timer-{timerName}"

        try:
            timer = Timer(name=timerName,
                             compName=compName,
                             expPids=expPids,
                             action=action,
                             actionLimit=self.actionLimit,
                             path=timerPath,
                             interval=timerInterval)
            # Preserve the creator's PID inside the timer
            # NOTE: This is a MUST, because otherwise if the timer expires the component
            #       won't know to whom to send the signal for properly restarting it
            timer.creator_id = threading.current_thread().native_id

        except Exception as ex:
            logging.error(f"Failed to create timer: {timerName} for component {compName} due to ERROR: {str(ex)}")
            return None

        self.timers[timerName] = timer
        return timer

    def setupCompTimers(self, compName):
        """
        Creates a timer per thread/process for all component's threads/processes.
        We spawn a separate thread for every timer.
        :param compName: The component name.
        :return:         Nothing
        """
        currThread = threading.currentThread()
        logging.info(f"setting up timers for component: {compName}.")

        for threadName, threadConfigSection in getThreadConfigSections(self.config, compName).items():

            # Here to setup the individual timer and add it to the timers list in the AgentWatchdog object

            # Adding an exception only for WorkQueueManagerReqMgrPoller, since its activation at the component
            # depends on an additional config parameter marking if the queue is Local or Global:
            if threadName == 'WorkQueueManagerReqMgrPoller' and self.config.WorkQueueManager.level == 'LocalQueue':
                logging.warning(f"Skip creation of timer: {threadName} for a Local WorkQueue.")
                continue

            # NOTE: Currently we are assigning the timerName to the actual threadName instead of compName
            #       in contrast to what it was when we were creating a single timer per component
            if not self.setupTimer(compName, threadConfigSection, timerName=threadName):
                continue

            # Finally start the timers:
            self.timers[threadName].start()

            # Here to preserve the timer on disk, such that its parameters can later be found by the
            # components itself, and it can be reset on time.
            # NOTE: This must happen upon timer's startup, because otherwise the timer attributes
            #       would be incomplete and later the component won't be able to reset it.
            self.timers[threadName].write()

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

        logging.debug(f"Polling cycle started with current pid: {currThread.native_id}.")
        logging.debug(f"Main pid from threading module: {threading.main_thread().native_id}, main pid at startup: {self.mainThread.native_id}, list of watched components: {list(self.timers.keys())}")
        logging.debug(f"Checking and Re-configuring previously expired timers for components with changed running state.")
        logging.debug(f"All current threads: {[thr.native_id for thr in  threading.enumerate()]}.")

        # Refresh timers:
        for compName in self.watchedComponents:
            timers = self._findTimersByComp(compName)

            # Retrying to create timers for components present in the watched list
            # but previously failed during initialization:
            logging.debug(f"Trying to recreate all previously failed timers for component: {compName}")
            configuredThreads = getThreadConfigSections(self.config, compName)
            for threadName, threadConfigSection in configuredThreads.items():
                # Adding the WorkQueueManagerReqMgrPoller exception for local WorkQueue yet again
                if threadName == 'WorkQueueManagerReqMgrPoller' and self.config.WorkQueueManager.level == 'LocalQueue':
                    continue
                if threadName not in [timer.name for timer in timers]:
                    if self.setupTimer(compName, threadConfigSection, timerName=threadName):
                        self.timers[threadName].start()
                        self.timers[threadName].write()

            # Re-configuring any timers associated with components which changed execution or have been restarted:
            for timer in timers:
                compPidTree = getComponentThreads(self.config, compName, quiet=True)
                if compPidTree and compPidTree['Parent'] not in timer.expPids:
                    if timer.is_alive():
                        logging.info(f"Re-configuring timer: {timer}, whose associated component has changed state or have been restarted.")
                        self.updateTimer(timer, compPidTree)
                    else:
                        logging.info(f"Restarting expired timer: {timer}, whose associated component has changed state or have been restarted.")
                        self.restartTimer(timer, compPidTree)

        # Refresh all timers' data on disk every 1 second. And restart expired timers
        # if a signal at the main thread has been received.
        # NOTE: When a signal.SIGCONT is received at the AgentWatchdogPooller's thread from a
        #       particular component's thread it means an overflowing thread has tried to resets its timer
        #       but failed because the timer has already expired. In such case we should consider the thread
        #       good to be watched again, since it did manage to complete its polling cycle and has made
        #       an attempt to reset the timer. In this situation we are about to find the timer per PID
        #       and just restart it.
        while _countdown(endTime):
            sigInfo = signal.sigtimedwait([self.expectedSignal], 1)
            if sigInfo:
                # Restarting a particular timer depending on the sender's PID:
                logging.info(f"{self.mainThread.name} with main pid: {threading.main_thread().native_id}: Received signal: {pformat(sigInfo)}")

                # First, find the correct timers:
                # NOTE: The condition are:
                #       * The sender's PID has to belong to a recognizable component's timer - This check
                #         would return more than a single timer for a multi-threaded component
                #       * Out of all such found timers filter only those which has already expired
                # TODO: To narrow the set of expPids per component such that we can have one and only one
                #       PID which should be allowed to reset the timer - this would avoid false positives.
                #       But this would require to implement a call to `pthread_setname_np` method from the `pthread`
                #       C library through ctype in order to preserve the thread's name at the OS in the proc fiel system.
                #       and later fetched through `psutils`
                timers = [timer for timer in self._findTimersByPid(sigInfo.si_pid) if not timer.alive]

                if not timers:
                    # Ignore signals from unknown origin:
                    logging.warning(f"The sender's pid: {sigInfo.si_pid} was not recognized or NO expired timers could be associate with it.")
                    logging.warning(f"Ignoring the signal.")
                    continue

                # Second, Refresh timer data:
                for timer in timers:
                    try:
                        logging.info(f"Restarting timer: {timer.name}")
                        self.restartTimer(timer, getComponentThreads(self.config, timer.compName, quiet=True))
                    except Exception:
                        logging.warning(f"Could not restart {timer.name}. The current signal is lost.")

            else:
                logging.debug(f"Refreshing all timers' data on disk.")
                for timer in self.timers.values():
                    timer.write()

        logging.debug(f"Reached the end of its polling cycle.")


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
        #                 logging.info(f"Resetting timer: {correctTimer.name}")
        #                 logging.info(f"Timer expPids: {correctTimer.expPids}")
        #                 logging.info(f"sending signal from: {currThread.native_id}")
        #                 logging.info(f"sending signal to: {correctTimer.native_id}")
        #                 os.kill(correctTimer.native_id, self.expectedSignal)
        #             except ProcessLookupError:
        #                 logging.warning(f"Missing timer: {correctTimer.name}. It will be recreated on the next AgentWatchdogPoller cycle, but the current signal is lost and the component may be restarted soon.")
        #         else:
        #             # Ignore signals from unknown origin:
        #             logging.info(f"The sender's pid: {sigInfo.si_pid} was not recognized. Ignoring the signal")
        #             continue
        #     else:
        #         logging.info(f"Reached the end of polling cycle.")
        #         break


    def setup(self, parameters=None):
        """
        __setup__

        Setup the name of the component to be watched
        """
        # Wait one full cycle before starting the whole component to allow all others to initialize properly
        logging.info("Waiting for one cycle to let others initialize properly ... ")
        # time.sleep(self.pollInterval)
        time.sleep(10)

        logging.info("Setting up all timers ... ")

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
        self.setupCompTimers(compName)
