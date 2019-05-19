#!/usr/bin/env python

"""
_DashboardMonitor_

This class monitors the job progress and reports to the dashboard
"""

import time
import logging
import os
import os.path
import traceback

from WMCore.WMRuntime.Monitors.WMRuntimeMonitor import WMRuntimeMonitor
from WMCore.WMSpec.WMStep                       import WMStepHelper
from WMCore.WMRuntime.DashboardInterface        import DashboardInfo

getStepName = lambda step: WMStepHelper(step).name()

def getStepPID(stepSpace, stepName):
    """
    _getStepPID_

    Find the PID for a step given its stepSpace from the file
    """
    currDir = stepSpace.location
    pidFile = os.path.join(currDir, 'process.id')
    if not os.path.isfile(pidFile):
        msg = "Could not find process ID file for step %s" % stepName
        logging.error(msg)
        return None

    with open(pidFile,'r') as filehandle:
        output = filehandle.read()

    try:
        stepPID = int(output)
    except ValueError:
        msg = "Couldn't find a number"
        logging.error(msg)
        return None

    return stepPID

class DashboardMonitor(WMRuntimeMonitor):
    """
    _DashboardMonitor_

    Run in the background and pass information to
    the DashboardInterface instance.

    If the job exceeds timeouts, kill the job
    """

    def __init__(self):
        self.startTime        = None
        self.currentStep      = None
        self.currentStepName  = None
        self.currentStepSpace = None
        self.task             = None
        self.job              = None
        self.dashboardInfo    = None
        WMRuntimeMonitor.__init__(self)


    def initMonitor(self, task, job, logPath, args = {}):
        """
        Handles the monitor initiation

        """
        logging.info("In DashboardMonitor.initMonitor")

        self.task    = task
        self.job     = job

        destHost = args.get('destinationHost', None)
        destPort = args.get('destinationPort', None)
        dashboardUrl = '%s:%s' % (destHost, str(destPort))
        cores = args.get('cores', 0)

        self.dashboardInfo = DashboardInfo(task, job, dashboardUrl=dashboardUrl,
                                           overrideCores=cores)

    def jobStart(self, task):
        """
        Job start notifier.
        """
        try:
            self.dashboardInfo.jobStart()
        except Exception as ex:
            logging.error(str(ex))
            logging.error(str(traceback.format_exc()))

        return


    def jobEnd(self, task):
        """
        Job End notification

        """
        try:
            self.dashboardInfo.jobEnd()
        except Exception as ex:
            logging.error(str(ex))
            logging.error(str(traceback.format_exc()))

        return

    def stepStart(self, step):
        """
        Step start notification

        """
        self.currentStep      = step
        self.currentStepName  = getStepName(step)
        self.currentStepSpace = None
        self.startTime        = time.time()
        try:
            self.dashboardInfo.stepStart(step = step)
        except Exception as ex:
            logging.error(str(ex))
            logging.error(str(traceback.format_exc()))
        return

    def stepEnd(self, step, stepReport):
        """
        Step end notification

        """
        self.currentStep      = None
        self.currentStepName  = None
        self.currentStepSpace = None
        try:
            self.dashboardInfo.stepEnd(step = step,
                                   stepReport = stepReport)
        except Exception as ex:
            logging.error(str(ex))
            logging.error(str(traceback.format_exc()))
        return


    def stepKilled(self, step):
        """
        Step killed notification

        """

        self.currentStep     = None
        self.currentStepName = None
        try:
            self.dashboardInfo.stepKilled(step = step)
        except Exception as ex:
            logging.error(str(ex))
            logging.error(str(traceback.format_exc()))
        return

    def jobKilled(self, task):
        """
        Killed job notification

        """
        try:
            self.dashboardInfo.jobKilled()
        except Exception as ex:
            logging.error(str(ex))
            logging.error(str(traceback.format_exc()))
        return


    def periodicUpdate(self):
        """
        Run on the defined intervals. Tell the dashboard info to run the
        periodic update

        """

        try:
            self.dashboardInfo.periodicUpdate()
        except Exception as ex:
            logging.error(str(ex))
            logging.error(str(traceback.format_exc()))
        return
