#!/usr/bin/env python

"""
_TestMonitor_

This is the test class for monitors
"""
from __future__ import print_function

import os.path
import time

from WMCore.Algorithms.SubprocessAlgos import *
from WMCore.WMRuntime.Monitors.WMRuntimeMonitor import WMRuntimeMonitor
from WMCore.WMSpec.Steps.Executor import getStepSpace
from WMCore.WMSpec.WMStep import WMStepHelper

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

    with open(pidFile, 'r') as filehandle:
        output = filehandle.read()

    try:
        stepPID = int(output)
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

    # I'm just grabbing the last twenty lines for the hell of it
    lines = tailNLinesFromFile(file, 20)

    lastMatch = None
    for line in lines:
        if MatchRunEvent.search(line.strip()):
            matches = MatchRunEvent.findall(line.strip())
            lastMatch = matches[-1]

    if lastMatch != None:
        #  //
        # // Extract and update last run/event number
        # //
        try:
            runInfo, lastEvent = lastMatch.split("Event:", 1)
            lastRun = int(runInfo.split("Run:", 1)[1])
            lastEvent = int(lastEvent)
            return (lastRun, lastEvent)
        except Exception:
            return (None, None)

    return (None, None)


class TestMonitor(WMRuntimeMonitor):
    def __init__(self):
        self.startTime = None
        self.currentStep = None
        self.currentStepName = None
        self.currentStepSpace = None
        self.softTimeOut = None
        self.hardTimeOut = None
        self.killFlag = False
        self.cmsswFile = None
        WMRuntimeMonitor.__init__(self)

    def initMonitor(self, task, job, logPath, args={}):
        """
        Handles the monitor initiation

        """
        print("In TestMonitor.initMonitor")

        self.softTimeOut = args.get('softTimeOut', None)
        self.hardTimeOut = args.get('hardTimeOut', None)

    def jobStart(self, task):
        """
        Job start notifier.
        """
        print("Yeehaw!  I started a job")

        return

    def jobEnd(self, task):
        """
        Job End notification

        """

        print("Job ended")

        return

    def stepStart(self, step):
        """
        Step start notification

        """
        self.currentStep = step
        self.currentStepName = getStepName(step)
        self.currentStepSpace = getStepSpace(self.currentStepName)
        self.startTime = time.time()
        print("Step started")

        return

    def stepEnd(self, step, stepReport):
        """
        Step end notification

        """
        self.currentStep = None
        self.currentStepName = None
        self.currentStepSpace = None
        print("Step ended")

    def periodicUpdate(self):
        """
        Run on the defined intervals.

        """

        if not self.currentStep or not self.currentStepSpace:
            # We're probably between steps
            return

        # Check for events
        if self.cmsswFile:
            run, event = searchForEvent(file)
            if run and event:
                # Then we actually found something, otherwise do nothing
                # Right now I don't know what to do
                pass

        # Do timeout
        if not self.softTimeOut:
            return

        if time.time() - self.startTime > self.softTimeOut:
            # Then we have to kill the process

            # First, get the PID
            stepPID = getStepPID(self.currentStepSpace, self.currentStepName)

            # Now kill it!
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
                killedpid, stat = os.waitpid(stepPID, os.WNOHANG)
                if killedpid == 0:
                    os.kill(stepPID, signal.SIGKill)
                    killedpid, stat = os.waitpid(stepPID, os.WNOHANG)
                    if killedpid == 0:
                        logging.error("Can't kill job.  Out of options.  Waiting for system reboot.")
                        # Panic!  It's unkillable!
                        pass


                        # logging.error(msg)
