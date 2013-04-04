#!/usr/bin/env python

"""
__PerformanceMonitor_

Monitor object which checks the job to ensure it is working inside
the agreed limits of virtual memory and wallclock time, and terminate it
if it exceeds them.
"""

import os
import signal
import os.path
import logging
import traceback
import time

import WMCore.Algorithms.SubprocessAlgos as subprocessAlgos
import WMCore.FwkJobReport.Report        as Report

from WMCore.WMRuntime.Monitors.DashboardMonitor import getStepPID
from WMCore.WMRuntime.Monitors.WMRuntimeMonitor import WMRuntimeMonitor
from WMCore.WMSpec.Steps.Executor               import getStepSpace
from WMCore.WMSpec.WMStep                       import WMStepHelper
from WMCore.WMException                         import WMException

getStepName = lambda step: WMStepHelper(step).name()

def average(numbers):
    """
    Quick averaging function

    """
    return float(sum(numbers)) / len(numbers)

class PerformanceMonitorException(WMException):
    """
    _PerformanceMonitorException_

    Just a performance monitor error...nothing to see here
    """
    pass

class PerformanceMonitor(WMRuntimeMonitor):
    """
    _PerformanceMonitor_

    Monitors the performance by pinging ps and
    recording data regarding the current step
    """

    def __init__(self):
        """
        Actual variable initialization
        in initMonitor
        """

        self.pid              = None
        self.uid              = os.getuid()
        self.monitorBase      = "ps -p %i -o pid,ppid,rss,vsize,pcpu,pmem,cmd -ww | grep %i"
        self.monitorCommand   = None
        self.currentStepSpace = None
        self.currentStepName  = None

        self.rss   = []
        self.vsize = []
        self.pcpu  = []
        self.pmem  = []

        self.maxRSS      = None
        self.maxVSize    = None
        self.softTimeout = None
        self.hardTimeout = None
        self.logPath     = None
        self.startTime   = None

        self.watchStepTypes = []

        self.disableStep = False

        WMRuntimeMonitor.__init__(self)

        return

    def initMonitor(self, task, job, logPath, args = {}):
        """
        _initMonitor_

        Puts together the information needed for the monitoring
        to actually find everything.
        """

        # Set the steps we want to watch
        self.watchStepTypes = args.get('WatchStepTypes', ['CMSSW', 'PerfTest'])

        self.maxRSS      = args.get('maxRSS', None)
        self.maxVSize    = args.get('maxVSize', None)
        self.softTimeout = args.get('softTimeout', None)
        self.hardTimeout = args.get('hardTimeout', None)

        self.logPath = os.path.join(logPath)

        return

    def jobStart(self, task):
        """
        _jobStart_

        Acknowledge that the job has started and initialize the time
        """
        self.startTime = time.time()

        return

    def stepStart(self, step):
        """
        _stepStart_

        Assure that the monitor is pointing at the right step
        """

        self.stepHelper = WMStepHelper(step)
        self.currentStepName  = getStepName(step)
        self.currentStepSpace = None

        if not self.stepHelper.stepType() in self.watchStepTypes:
            self.disableStep = True
            logging.debug("PerformanceMonitor ignoring step of type %s" % self.stepHelper.stepType())
            return
        else:
            logging.debug("Beginning PeformanceMonitor step Initialization")
            self.disableStep = False

        return


    def stepEnd(self, step, stepReport):
        """
        _stepEnd_

        Package the information and send it off
        """

        if not self.disableStep:
            # No information to correlate
            return

        self.currentStepName  = None
        self.currentStepSpace = None

        return


    def periodicUpdate(self):
        """
        Run on the defined intervals.

        """
        killProc = False
        killHard = False
        reason = ''
        errorCodeLookup = {'RSS' : 50660,
                           'VSZ' : 50661,
                           'Wallclock time' : 50664,
                           '' : 99999}

        if self.disableStep:
            # Then we aren't doing CPU monitoring
            # on this step
            return

        if self.currentStepName == None:
            # We're between steps
            return

        if self.currentStepSpace == None:
            # Then build the step space
            self.currentStepSpace = getStepSpace(self.stepHelper.name())

        stepPID = getStepPID(self.currentStepSpace, self.currentStepName)

        if stepPID == None:
            # Then we have no step PID, we can do nothing
            return

        # Now we run the monitor command and collate the data
        cmd = self.monitorBase % (stepPID, stepPID)
        stdout, stderr, retcode = subprocessAlgos.runCommand(cmd)

        output = stdout.split()
        if not len(output) > 7:
            # Then something went wrong in getting the ps data
            msg =  "Error when grabbing output from process ps\n"
            msg += "output = %s\n" % output
            msg += "command = %s\n" % self.monitorCommand
            logging.error(msg)
            return
        rss   = float(output[2])
        vsize = float(output[3])
        logging.info("Retrieved following performance figures:")
        logging.info("RSS: %s;  VSize: %s; PCPU: %s; PMEM: %s" % (output[2], output[3],
                                                                  output[4], output[5]))

        msg = 'Error in CMSSW step %s\n' % self.currentStepName
        if self.maxRSS != None and rss >= self.maxRSS:
            msg += "Job has exceeded maxRSS: %s\n" % self.maxRSS
            msg += "Job has RSS: %s\n" % rss
            killProc = True
            reason = 'RSS'
        if self.maxVSize != None and vsize >= self.maxVSize:
            msg += "Job has exceeded maxVSize: %s\n" % self.maxVSize
            msg += "Job has VSize: %s\n" % vsize
            killProc = True
            reason = 'VSZ'

        #Let's check the running time
        currentTime = time.time()

        if self.hardTimeout != None and self.softTimeout != None:
            if (currentTime - self.startTime) > self.softTimeout:
                killProc = True
                reason = 'Wallclock time'
                msg += "Job has been running for more than: %s\n" % str(self.softTimeout)
                msg += "Job has been running for: %s\n" % str(currentTime - self.startTime)
            if (currentTime - self.startTime) > self.hardTimeout:
                killHard = True
                msg += "Job exceeded soft timeout"

        if killProc:
            logging.error(msg)
            report  = Report.Report()
            # Find the global report
            logPath = os.path.join(self.currentStepSpace.location,
                                   '../../../',
                                   os.path.basename(self.logPath))
            try:
                if os.path.isfile(logPath):
                    # We should be able to find existant job report.
                    # If not, we're in trouble
                    logging.debug("Found pre-existant error report in PerformanceMonitor termination.")
                    report.load(logPath)
                # Create a new step that won't be overridden by an exiting CMSSW
                if not report.retrieveStep(step = "PerformanceError"):
                    report.addStep(reportname = "PerformanceError")
                report.addError(stepName = "PerformanceError", exitCode = errorCodeLookup[reason],
                                errorType = "PerformanceKill", errorDetails = msg)
                report.save(logPath)
            except Exception, ex:
                # Basically, we can't write a log report and we're hosed
                # Kill anyway, and hope the logging file gets written out
                msg2 =  "Exception while writing out jobReport.\n"
                msg2 += "Aborting job anyway: unlikely you'll get any error report.\n"
                msg2 += str(ex)
                msg2 += str(traceback.format_exc()) + '\n'
                logging.error(msg2)

            try:
                if not killHard:
                    logging.error("Attempting to kill step using SIGUSR2")
                    os.kill(stepPID, signal.SIGUSR2)
                else:
                    logging.error("Attempting to kill step using SIGTERM")
                    os.kill(stepPID, signal.SIGTERM)
            except Exception:
                logging.error("Attempting to kill step using SIGTERM")
                os.kill(stepPID, signal.SIGTERM)

        return
