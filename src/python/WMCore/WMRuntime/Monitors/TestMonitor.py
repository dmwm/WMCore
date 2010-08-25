#!/usr/bin/env python

"""
_TestMonitor_

This is the test class for monitors
"""

__version__ = "$Revision: 1.2 $"
__revision__ = "$Id: TestMonitor.py,v 1.2 2009/12/21 17:18:40 mnorman Exp $"

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









class TestMonitor(WMRuntimeMonitor):

    def __init__(self):
        self.startTime        = None
        self.currentStep      = None
        self.currentStepName  = None
        self.currentStepSpace = None
        self.softTimeOut      = None
        self.hardTimeOut      = None
        self.killFlag         = False
        self.cmsswFile        = None
        WMRuntimeMonitor.__init__(self)


    def initMonitor(self, task, job, args = {}):
        """
        Handles the monitor initiation

        """
        print "In TestMonitor.initMonitor"

        self.softTimeOut = args.get('softTimeOut', None)
        self.hardTimeOut = args.get('hardTimeOut', None)


    def jobStart(self, task):
        """
        Job start notifier.
        """
        print "Yeehaw!  I started a job"

        return


    def jobEnd(self, task):
        """
        Job End notification

        """

        print "Job ended"

        return

    def stepStart(self, step):
        """
        Step start notification

        """
        self.currentStep      = step
        self.currentStepName  = getStepName(step)
        self.currentStepSpace = getStepSpace(self.currentStepName)
        self.startTime        = time.time()
        print "Step started"

        return

    def stepEnd(self, step):
        """
        Step end notification

        """
        self.currentStep      = None
        self.currentStepName  = None
        self.currentStepSpace = None
        print "Step ended"


    def periodicUpdate(self):
        """
        Run on the defined intervals.

        """
        
        if not self.currentStep or not self.currentStepSpace:
            #We're probably between steps
            return


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
            if time.time() - self.startTime < self.hardTimeOut or not self.killFlag:
                os.kill(stepPID, signal.SIGUSR2)
                msg += "WARNING: Soft Kill Timeout has Expired:"
                self.killFlag = True
            elif self.killFlag:
                msg += "WARNING: Hard Kill Timeout has Expired:"
                os.kill(stepPID, signal.SIGTERM)
                killedpid, stat = os.waitpid(pid, os.WNOHANG)
                if killedpid == 0:
                    os.kill(stepPID, signal.SIGKill)
                    killedpid, stat = os.waitpid(pid, os.WNOHANG)
                    if killedpid == 0:
                        logging.error("Can't kill job.  Out of options.  Waiting for system reboot.")
                        #Panic!  It's unkillable!
                        pass

            msg += "Start Time: %s\n" % self.startTime
            msg += "Time Now: %s\n" % time.time()
            msg += "Timeout: %s\n" % self.softTimeOut
            msg += "Killing Job...\n"
            msg += "Process ID is: %s\n" % stepPID
            logging.error(msg)
        
        
