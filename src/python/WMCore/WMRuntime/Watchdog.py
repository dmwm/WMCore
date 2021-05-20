#!/usr/bin/env python

"""
_Watchdog_

This cleverly named object is the thread that handles the monitoring of individual jobs
"""
from __future__ import division
from __future__ import print_function
from future.utils import viewitems

import logging
import os
import os.path
import threading
import traceback

from PSetTweaks.WMTweak import resizeResources
from WMCore.WMException import WMException
from WMCore.WMFactory import WMFactory


class WatchdogException(WMException):
    """
    _StepFactortyException_

    It's like an exception class that does nothing

    """
    pass


class Watchdog(threading.Thread):
    """
    Watchdog

    It's like a watchdog!  It watches things, and then it barks at them, curls up, and goes to sleep
    Or possibly monitors them and records what they do.  It's a bit of a crapshoot at this point.
    """

    def __init__(self, logPath=None, config=None):
        threading.Thread.__init__(self)
        self.doMonitoring = True
        self._Finished = threading.Event()
        self._EndOfJob = threading.Event()
        self._NewTask = threading.Event()
        self._JobKilled = threading.Event()
        self._RunUpdate = threading.Event()
        self._Interval = 120.0
        self._Monitors = []

        # Right now we join this, because we don't know
        # Where we'll be when we need this.
        self.logPath = os.path.join(os.getcwd(), logPath)

        self.factory = WMFactory(self.__class__.__name__,
                                 "WMCore.WMRuntime.Monitors")

    def setupMonitors(self, task, wmbsJob):
        logging.info("In Watchdog.setupMonitors")
        if not hasattr(task.data, 'watchdog'):
            msg = "Could not find watchdog in spec"
            logging.error(msg)
            # I don't think this is necessarily fatal
            return
        if not hasattr(task.data.watchdog, 'monitors'):
            msg = "Watchdog has no monitors"
            logging.error(msg)
            # Probably not fatal either
            return
        if hasattr(task.data.watchdog, 'interval'):
            # Set the interval off the config
            self.setInterval(task.data.watchdog.interval)
        for monitor in task.data.watchdog.monitors:
            msg = "Initializing monitor %s" % monitor
            logging.info(msg)
            mon = self.loadMonitor(monitor)
            args = {}
            if hasattr(task.data.watchdog, monitor):
                # This should be a config section
                monitorArgs = getattr(task.data.watchdog, monitor)
                args = monitorArgs.dictionary_()
            if monitor == 'PerformanceMonitor' and args:
                # Apply tweaks to PerformanceMonitor only.
                # Scale resources according to the HTCondor runtime environment.
                origCores = 1
                for stepName in task.listAllStepNames():
                    sh = task.getStepHelper(stepName)
                    origCores = max(origCores, sh.getNumberOfCores())
                resources = {'cores': origCores}
                origMaxPSS = args.get('maxPSS', args.get('maxRSS'))
                if origMaxPSS:
                    resources['memory'] = origMaxPSS
                # Actually parses the HTCondor runtime
                resizeResources(resources)
                # We decided to only touch Watchdog settings if the number of cores changed.
                # (even if this means the watchdog memory is wrong for a slot this size).
                changedCores = origCores != resources['cores']
                # If we did base maxPSS off the memory in the HTCondor slot, subtract a bit
                # off the top so watchdog triggers before HTCondor does.
                # Add the new number of cores to the args such that PerformanceMonitor can see it
                args['cores'] = resources['cores']
                if changedCores:
                    if origMaxPSS:
                        args['maxPSS'] = resources['memory'] - 50

                logging.info("Watchdog modified: %s. Final settings:", changedCores)
                for k, v in viewitems(args):
                    logging.info("  %s: %r", k, v)
            # Actually initialize the monitor variables
            mon.initMonitor(task=task, job=wmbsJob,
                            logPath=self.logPath, args=args)
            self._Monitors.append(mon)

        return

    def loadMonitor(self, monitorName):
        try:
            return self.factory.loadObject(monitorName)
        except WMException:
            msg = "WatchdogFactory Unable to load Object: %s" % monitorName
            logging.error(msg)
            raise WatchdogException(msg)
        except Exception as ex:
            msg = "Error creating object %s in WatchdogFactory:\n" % monitorName
            msg += str(ex)
            logging.error(msg)
            raise WatchdogException(msg)

    def setInterval(self, interval):
        """
        Set the monitor interval
        """
        logging.info("Set Watchdog interval to %s", interval)
        self._Interval = interval

    def disableMonitoring(self):
        """
        _disableMonitoring_

        Turn off active monitoring (periodicUpdate calls are disabled,
        event driven monitoring still occurs
        """
        self.doMonitoring = False

    def shutdown(self):
        """
        Shutdown the monitor.
        """
        logging.info("MonitorState: Shutdown called")
        self._Finished.set()
        return

    #  //=========notify Methods called by the ExecutionManager====
    # //
    # //  Start notification from the exe thread, this starts the
    #  //periodic updates of the monitor thread
    # //
    # //
    def notifyJobStart(self, task):
        """
        Start the job.
        """
        logging.info("MonitorThread: JobStarted")
        if self.doMonitoring:
            self.setDaemon(1)
            self.start()
        for monitor in self._Monitors:
            try:
                monitor.jobStart(task)
            except Exception as ex:
                msg = "Error in notifyJobStart for monitor class %s in Watchdog:\n" % monitor.__class__
                msg += str(ex)
                msg += str(traceback.format_exc())
                logging.error(msg)
                raise WatchdogException(msg)
        return

    #  //
    # // notify Monitors of new task start up
    # //
    def notifyStepStart(self, step):
        """
        notify Monitors of new task start up.
        """
        self._RunUpdate.set()
        for monitor in self._Monitors:
            try:
                monitor.stepStart(step)
            except Exception as ex:
                msg = "Error in notifyTaskStart for monitor class %s in Watchdog:\n" % monitor.__class__
                msg += str(ex)
                msg += str(traceback.format_exc())
                logging.error(msg)
                raise WatchdogException(msg)
        return

    #  //
    # // notify Monitors of task completion
    # //
    def notifyStepEnd(self, step, exitCode=0, stepReport=None):
        """
        notify Monitors of task completion.
        """
        self._RunUpdate.clear()
        for monitor in self._Monitors:
            try:
                monitor.stepEnd(step=step, stepReport=stepReport)
            except Exception as ex:
                msg = "Error in notifyTaskEnd for monitor class %s in Watchdog:\n" % monitor.__class__
                msg += str(ex)
                msg += str(traceback.format_exc())
                logging.error(msg)
                raise WatchdogException(msg)
        # print "Task Ended: %s with Exit Code:%s" % (task, exitCode)
        # self._MonMgr.taskEnd(task, exitCode)
        return

    #  //
    # // notify monitors of Job Completion, stops the periodic
    # //  updating
    def notifyJobEnd(self, task):
        """
        notify monitors of Job Completion, stops the periodic
        updating.
        """
        logging.info("MonitorThread: JobEnded")
        for monitor in self._Monitors:
            try:
                monitor.jobEnd(task)
            except Exception as ex:
                msg = "Error in notifyJobEnd for monitor class %s in Watchdog:\n" % monitor.__class__
                msg += str(ex)
                msg += str(traceback.format_exc())
                raise WatchdogException(msg)
        # self._MonMgr.jobEnd()
        self.shutdown()
        return

    #  //
    # //  Interrupt Notifiers
    # //   Job has been killed
    def notifyKillJob(self):
        """
        Interrupt Notifiers, Job has been killed.
        """
        logging.info("MonitorThread: JobKilled")
        for monitor in self._Monitors:
            try:
                monitor.jobKilled()
            except Exception as ex:
                msg = "Error in notifyKillJob for monitor class %s in Watchdog:\n" % monitor.__class__
                msg += str(ex)
                msg += str(traceback.format_exc())
                logging.error(msg)
                raise WatchdogException(msg)
        # self._MonMgr.jobKilled()
        self.shutdown()

    #  //
    # //  Task has been killed
    # //
    def notifyKillStep(self, step=None):
        """
        Task has been killed.
        """
        logging.info("MonitorThread: TaskKilled")
        for monitor in self._Monitors:
            try:
                monitor.stepKilled(step)
            except Exception as ex:
                msg = "Error in notifyKillTask for monitor class %s in Watchdog:\n" % monitor.__class__
                msg += str(ex)
                msg += str(traceback.format_exc())
                logging.error(msg)
                raise WatchdogException(msg)
                # self._MonMgr.taskKilled()

    # //
    # // Override Thread.run() to do the periodic update
    # //  of the MonitorState object and dispatch it to the monitors
    def run(self):
        """
        Override Thread.run() to do the periodic update
        of the MonitorState object and dispatch it to the monitors
        """
        while True:
            #  //
            # // shutdown signal
            # //
            if self._Finished.isSet():
                return

            # //
            # // Update State information only during a running task
            # //
            if self._RunUpdate.isSet():
                for monitor in self._Monitors:
                    try:
                        monitor.periodicUpdate()
                    except Exception as ex:
                        msg = "Error in periodicUpdate for monitor class %s in Watchdog:\n" % monitor.__class__
                        msg += str(ex)
                        msg += str(traceback.format_exc())
                        msg += "This is a CRITICAL error because this kills the monitoring.\n"
                        msg += "Terminate thread and retry.\n"
                        logging.error(msg)
                        # raise WatchdogException(msg)
                        # This one needs to be killed by itself
                        # since it's run by thread
                        os.abort()
                        # self._MonMgr.periodicUpdate()

            # time.sleep(self._Interval)
            self._Finished.wait(self._Interval)

    # //
    # // Load Monitor Objects based on Cfg settings passed
    # //  from Executor
    def initMonitorFwk(self, monitorCfg, updatorCfg):
        """
        _initMonitorFwk_

        Initialise the MonitorMgr object when this method is
        called from the Execution thread
        Load Monitor Objects based on Cfg settings passed from Executor.
        """
        # self._MonMgr.monitorConfig = monitorCfg
        # self._MonMgr.updatorConfig = updatorCfg
        # self._MonMgr.loadMonitors()
        # self._MonMgr.loadUpdators()
        return
