import os
import pickle
import logging
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMComponent.RetryManager.Modifier.BaseModifier import BaseModifier
from WMCore.FwkJobReport.Report import Report

class DefaultModifier(BaseModifier):
    """
    Modifier of jobs with exit codes that dont have a specific modifier
    """
    def __init__(self, newMemory = 0):
        BaseModifier.__init__(self)
        
    def isReady(self, job):
        return True