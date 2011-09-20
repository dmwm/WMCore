#!/usr/bin/env python

import os
import signal
import os.path
import logging
import traceback

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
        #self.grabCommand      = "ps -u %i -o pid,ppid,rss,vsize,pcpu,pmem,cmd |grep %s |grep -v grep"
        self.monitorBase      = "ps -p %i -o pid,ppid,rss,vsize,pcpu,pmem,cmd -ww |grep %i"
        self.monitorCommand   = None
        self.currentStepSpace = None
        self.currentStepName  = None

        self.rss   = []
        self.vsize = []
        self.pcpu  = []
        self.pmem  = []

        self.maxRSS   = None
        self.maxVSize = None
        self.logPath  = None

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

        self.maxRSS   = args.get('maxRSS', None)
        self.maxVSize = args.get('maxVSize', None)

        self.logPath = os.path.join(logPath)

        return

    def stepStart(self, step):
        """
        _stepStart_

        Assure that the monitor is pointing at the right step
        """

        stepHelper = WMStepHelper(step)
        self.currentStepName  = getStepName(step)
        self.currentStepSpace = None

        if not stepHelper.stepType() in self.watchStepTypes:
            self.disableStep = True
            logging.debug("PerformanceMonitor ignoring step of type %s" % stepHelper.stepType())
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

        if self.disableStep:
            # Then we aren't doing CPU monitoring
            # on this step
            return

        if self.currentStepName == None:
            # We're between steps
            return

        if self.currentStepSpace == None:
            # Then build the step space
            self.currentStepSpace = getStepSpace(stepHelper.name())        

        stepPID = getStepPID(self.currentStepSpace, self.currentStepName)

        if stepPID == None:
            # Then we have no step PID, we can do nothing
            return

        # Now we run the monitor command and collate the data
        cmd = self.monitorBase % (stepPID, stepPID)
        stdout, stderr = subprocessAlgos.runCommand(cmd)

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
        if self.maxVSize != None and vsize >= self.maxVSize:
            msg += "Job has exceeded maxVSize: %s\n" % self.maxVSize
            msg += "Job has VSize: %s\n" % vsize
            killProc = True


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
                    logging.debug("Found pre-existant error report in DashboardMonitor termination.")
                    report.load(logPath)
                # Create a new step that won't be overridden by an exiting CMSSW
                if not report.retrieveStep(step = "PerformanceError"):
                    report.addStep(reportname = "PerformanceError")
                report.addError(stepName = "PerformanceError", exitCode = 99900,
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
                logging.error("Attempting to kill job using SIGUSR2")
                os.kill(stepPID, signal.SIGUSR2)
            except Exception:
                os.kill(stepPID, signal.SIGTERM)

        return


