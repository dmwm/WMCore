#!/usr/bin/env python

"""
_Watchdog_

This cleverly named object is the thread that handles the monitoring of individual jobs
"""




import threading
import logging
import traceback

from WMCore.WMFactory   import WMFactory
from WMCore.WMException import WMException

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



    def __init__(self, config = None):
        threading.Thread.__init__(self)
        self.doMonitoring = True
        self._Finished    = threading.Event()
        self._EndOfJob    = threading.Event()
        self._NewTask     = threading.Event()
        self._JobKilled   = threading.Event()
        self._RunUpdate   = threading.Event()
        self._Interval    = 120.0
        self._Monitors    = []


        self.factory    = WMFactory(self.__class__.__name__, "WMCore.WMRuntime.Monitors")


    def setupMonitors(self, task, wmbsJob):
        logging.info("In Watchdog.setupMonitors")
        if not hasattr(task.data, 'watchdog'):
            msg = "Could not find watchdog in spec"
            logging.error(msg)
            #I don't think this is necessarily fatal
            return
        if not hasattr(task.data.watchdog, 'monitors'):
            msg = "Watchdog has no monitors"
            logging.error(msg)
            #Probably not fatal either
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
                #This should be a config section
                monitorArgs = getattr(task.data.watchdog, monitor)
                args = monitorArgs.dictionary_()
            mon.initMonitor(task = task, job = wmbsJob, args = args)
            self._Monitors.append(mon)

        return


    def loadMonitor(self, monitorName):
        try:
            return self.factory.loadObject(monitorName)
        except WMException, wmEx:
            msg = "WatchdogFactory Unable to load Object: %s" % monitorName
            raise WatchdogException(msg)
        except Exception, ex:
            msg = "Error creating object %s in WatchdogFactory:\n" % monitorName
            msg += str(ex)
            raise WatchdogException(msg)


    def setInterval(self, interval):
        """
        Set the monitor interval
        """
        logging.info("Set Watchdog interval to %s" % interval)
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
    #//  Start notification from the exe thread, this starts the
    #  //periodic updates of the monitor thread
    # //
    #//
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
            except Exception, ex:
                msg = "Error in notifyJobStart for monitor class %s in Watchdog:\n" % monitor.__class__
                msg += str(ex)
                msg += str(traceback.format_exc())
                raise WatchdogException(msg)
        return
    #  //
    # // notify Monitors of new task start up
    #//
    def notifyStepStart(self, step):
        """
        notify Monitors of new task start up.
        """
        self._RunUpdate.set()
        for monitor in self._Monitors:
            try:
                monitor.stepStart(step)
            except Exception, ex:
                msg = "Error in notifyTaskStart for monitor class %s in Watchdog:\n" % monitor.__class__
                msg += str(ex)
                msg += str(traceback.format_exc())
                raise WatchdogException(msg)
        return

    #  //
    # // notify Monitors of task completion
    #//
    def notifyStepEnd(self, step, exitCode = 0, stepReport = None):
        """
        notify Monitors of task completion.
        """
        self._RunUpdate.clear()
        for monitor in self._Monitors:
            try:
                monitor.stepEnd(step = step, stepReport = stepReport)
            except Exception, ex:
                msg = "Error in notifyTaskEnd for monitor class %s in Watchdog:\n" % monitor.__class__
                msg += str(ex)
                msg += str(traceback.format_exc())
                raise WatchdogException(msg)
        #print "Task Ended: %s with Exit Code:%s" % (task, exitCode)
        #self._MonMgr.taskEnd(task, exitCode)
        return

    #  //
    # // notify monitors of Job Completion, stops the periodic
    #//  updating
    def notifyJobEnd(self, task):
        """
        notify monitors of Job Completion, stops the periodic
        updating.
        """
        logging.info("MonitorThread: JobEnded")
        for monitor in self._Monitors:
            try:
                monitor.jobEnd(task)
            except Exception, ex:
                msg = "Error in notifyJobEnd for monitor class %s in Watchdog:\n" % monitor.__class__
                msg += str(ex)
                msg += str(traceback.format_exc())
                raise WatchdogException(msg)
        #self._MonMgr.jobEnd()
        self.shutdown()
        return
    #  //
    # //  Interrupt Notifiers
    #//   Job has been killed
    def notifyKillJob(self):
        """
        Interrupt Notifiers, Job has been killed.
        """
        logging.info("MonitorThread: JobKilled")
        for monitor in self._Monitors:
            try:
                monitor.jobKilled()
            except Exception, ex:
                msg = "Error in notifyKillJob for monitor class %s in Watchdog:\n" % monitor.__class__
                msg += str(ex)
                msg += str(traceback.format_exc())
                raise WatchdogException(msg)
        #self._MonMgr.jobKilled()
        self.shutdown()
    #  //
    # //  Task has been killed
    #//
    def notifyKillStep(self, step = None):
        """
        Task has been killed.
        """
        logging.info("MonitorThread: TaskKilled")
        for monitor in self._Monitors:
            try:
                monitor.stepKilled(step)
            except Exception, ex:
                msg = "Error in notifyKillTask for monitor class %s in Watchdog:\n" % monitor.__class__
                msg += str(ex)
                msg += str(traceback.format_exc())
                raise WatchdogException(msg)
        #self._MonMgr.taskKilled()
        
    #  //
    # // Override Thread.run() to do the periodic update
    #//  of the MonitorState object and dispatch it to the monitors
    def run(self):
        """
        Override Thread.run() to do the periodic update
        of the MonitorState object and dispatch it to the monitors
        """
        while 1:
            #  //
            # // shutdown signal
            #//
            if self._Finished.isSet():
                return

            #  //
            # // Update State information only during a running task
            #//
            if self._RunUpdate.isSet():
                for monitor in self._Monitors:
                    try:
                        monitor.periodicUpdate()
                    except Exception, ex:
                        msg = "Error in periodicUpdate for monitor class %s in Watchdog:\n" % monitor.__class__
                        msg += str(ex)
                        msg += str(traceback.format_exc())
                        raise WatchdogException(msg)
                #self._MonMgr.periodicUpdate()

            #time.sleep(self._Interval)
            self._Finished.wait(self._Interval)
           

    #  //
    # // Load Monitor Objects based on Cfg settings passed
    #//  from Executor
    def initMonitorFwk(self, monitorCfg, updatorCfg):
        """
        _initMonitorFwk_

        Initialise the MonitorMgr object when this method is
        called from the Execution thread
        Load Monitor Objects based on Cfg settings passed from Executor.
        """
        #self._MonMgr.monitorConfig = monitorCfg
        #self._MonMgr.updatorConfig = updatorCfg
        #self._MonMgr.loadMonitors()
        #self._MonMgr.loadUpdators()
        return
