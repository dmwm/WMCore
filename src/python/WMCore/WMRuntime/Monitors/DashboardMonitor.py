#!/usr/bin/env python

"""
_TestMonitor_

This is the test class for monitors
"""




import threading
import time
import logging
import os
import os.path
import signal
import subprocess

from WMCore.WMRuntime.Monitors.WMRuntimeMonitor import WMRuntimeMonitor
from WMCore.WMSpec.Steps.Executor               import getStepSpace
from WMCore.WMSpec.WMStep                       import WMStepHelper
from WMCore.Algorithms.SubprocessAlgos          import *

# Get the Dashboard information class
from WMCore.WMRuntime.DashboardInterface        import DashboardInfo

getStepName = lambda step: WMStepHelper(step).name()

def getStepPID(stepSpace, stepName):
    """
    _getStepPID_
    
    Find the PID for a step given its stepSpace from the file
    """
    currDir = stepSpace.location
    pidFile = os.path.join(currDir, 'process_id')
    if not os.path.isfile(pidFile):
        msg = "Could not find process ID file for step %s" % stepName
        logging.error(msg)
        return
    
    filehandle=open(pidFile,'r')
    output=filehandle.read()
    
    try:
        stepPID=int(output)
    except ValueError:
        msg = "Couldn't find a number"
        logging.error(msg)
        return None

    return stepPID


def searchForEvent(file):
    """
    _searchForEvent_
    
    Searches for the last event output into the CMSSW output file
    """

    MatchRunEvent = re.compile("Run: [0-9]+ Event: [0-9]+$")

    #I'm just grabbing the last twenty lines for the hell of it
    tailNLinesFromFile(file, 20)

    lastMatch = None
    for line in lines:
        if MatchRunEvent.search(line.strip()):
            matches = MatchRunEvent.findall(line.strip())
            lastMatch = matches[-1]

    if lastMatch != None:
        #  //
        # // Extract and update last run/event number
        #//
        try:
            runInfo, lastEvent = lastMatch.split("Event:", 1)
            lastRun =  int(runInfo.split("Run:", 1)[1])
            lastEvent = int(lastEvent)
            return (lastRun, lastEvent)
        except Exception:
            return (None, None)

    return (None, None)









class DashboardMonitor(WMRuntimeMonitor):

    def __init__(self):
        self.startTime        = None
        self.currentStep      = None
        self.currentStepName  = None
        self.currentStepSpace = None
        self.softTimeOut      = None
        self.hardTimeOut      = None
        self.killFlag         = False
        self.cmsswFile        = None
        self.task             = None
        self.job              = None
        self.dashboardInfo    = None
        WMRuntimeMonitor.__init__(self)


    def initMonitor(self, task, job, args = {}):
        """
        Handles the monitor initiation

        """
        print "In TestMonitor.initMonitor"

        self.task = task
        self.job  = job

        self.softTimeOut = args.get('softTimeOut', None)
        self.hardTimeOut = args.get('hardTimeOut', None)

        destHost = args.get('destinationHost', None)
        destPort = args.get('destinationPort', None)

        self.dashboardInfo = DashboardInfo(task = task, job = job)

        if destHost and destPort:
            self.dashboardInfo.addDestination(host = destHost,
                                              port = destPort)


    def jobStart(self, task):
        """
        Job start notifier.
        """

        self.dashboardInfo.jobStart()

        return


    def jobEnd(self, task):
        """
        Job End notification

        """

        self.dashboardInfo.jobEnd()

        return

    def stepStart(self, step):
        """
        Step start notification

        """
        self.currentStep      = step
        self.currentStepName  = getStepName(step)
        #self.currentStepSpace = getStepSpace(self.currentStepName)
        self.startTime        = time.time()
        self.dashboardInfo.stepStart(step = step)

        return

    def stepEnd(self, step, stepReport):
        """
        Step end notification

        """
        self.currentStep      = None
        self.currentStepName  = None
        #self.currentStepSpace = None
        self.dashboardInfo.stepEnd(step = step,
                                   stepReport = stepReport)


    def stepKilled(self, step):
        """
        Step killed notification

        """

        self.currentStep     = None
        self.currentStepName = None
        self.dashboardInfo.stepKilled(step = step)


    def jobKilled(self, task):
        """
        Killed job notification

        """

        self.dashboardInfo.jobKilled()

        return


    def periodicUpdate(self):
        """
        Run on the defined intervals.

        """
        
        if not self.currentStep:
            #We're probably between steps
            return

        self.dashboardInfo.periodicUpdate()


        #Check for events
        if self.cmsswFile:
            run, event = searchForEvent(file)
            if run and event:
                #Then we actually found something, otherwise do nothing
                #Right now I don't know what to do
                pass

        #Do timeout
        if not self.softTimeOut:
            return


        if time.time() - self.startTime > self.softTimeOut:
            #Then we have to kill the process

            #First, get the PID
            stepPID = getStepPID(self.currentStepSpace, self.currentStepName)
        
            #Now kill it!
            msg = ""
            msg += "Start Time: %s\n" % self.startTime
            msg += "Time Now: %s\n" % time.time()
            msg += "Timeout: %s\n" % self.softTimeOut
            msg += "Killing Job...\n"
            msg += "Process ID is: %s\n" % stepPID
            if time.time() - self.startTime < self.hardTimeOut or not self.killFlag:
                msg += "WARNING: Soft Kill Timeout has Expired:"
                logging.error(msg)
                os.kill(stepPID, signal.SIGUSR2)
                self.killFlag = True
            elif self.killFlag:
                msg += "WARNING: Hard Kill Timeout has Expired:"
                logging.error(msg)
                os.kill(stepPID, signal.SIGTERM)
                killedpid, stat = os.waitpid(pid, os.WNOHANG)
                if killedpid == 0:
                    os.kill(stepPID, signal.SIGKill)
                    killedpid, stat = os.waitpid(pid, os.WNOHANG)
                    if killedpid == 0:
                        logging.error("Can't kill job.  Out of options.  Waiting for system reboot.")
                        #Panic!  It's unkillable!
                        pass


        return
        
