#!/usr/bin/env python

"""
_WMRuntimeMonitor_

This is the base class for monitors
"""




from builtins import object
import threading
import os

from WMCore.WMException                         import WMException
from WMCore.WMSpec.Steps.Executor               import getStepSpace
from WMCore.WMSpec.WMStep                       import WMStepHelper

getStepName = lambda step: WMStepHelper(step).name()

class WMRuntimeMonitorException(WMException):
    """
    _StepFactortyException_

    It's like an exception class that does nothing

    """
    pass

class WMRuntimeMonitor(object):


    def __init__(self):
        self.currentStep      = None
        self.currentStepName  = None
        self.currentStepSpace = None
        self.job              = None
        self.task             = None
        return



    def initMonitor(self, task, job, logPath, args = {}):
        return


    def shutdown(self):
        """
        Shutdown method, will be called before object is deleted
        at end of job.
        """
        pass


    def periodicUpdate(self):
        """
        Periodic update.
        """
        pass


    def jobStart(self, task):
        """
        Job start notifier.
        """
        pass


    def stepStart(self, step):
        """
        Tasked started notifier.
        """
        self.currentStep      = step
        self.currentStepName  = getStepName(step)
        self.currentStepSpace = getStepSpace(self.currentStepName)
        return


    def stepEnd(self, step, stepReport):
        """
        Tasked ended notifier.
        """
        self.currentStep      = None
        self.currentStepName  = None
        self.currentStepSpace = None
        return



    def jobEnd(self, task):
        """
        Job ended notifier.
        """
        pass

    def jobKilled(self):
        """
        Job killed notifier.
        """
        pass

    def stepKilled(self):
        """
        Task killed notifier.
        """
        pass

    def killCMSRun(self):
        """
        Kills all cmsRun processes owned by user

        """

        uid = os.getuid()
