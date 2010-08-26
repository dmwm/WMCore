#!/usr/bin/env python
"""
_LogCollect_

Template for a LogCollect Step

Mostly borrowed from StageOut since they share a similar function

"""

__revision__ = "$Id: LogCollect.py,v 1.1 2010/05/05 21:06:06 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMSpec.Steps.Template import Template
from WMCore.WMSpec.Steps.Template import CoreHelper
from WMCore.WMSpec.ConfigSectionTree import nodeName



class LogCollectStepHelper(CoreHelper):
    """
    _StageOutStepHelper_

    Add API calls and helper methods to the basic WMStepHelper to specialise
    for StageOut tasks

    This is very similar to StageOut since they have essentially the same function

    """
    
    def disableRetries(self):
        """
            handy for testing, without the 10 minute retry loop
        """
        self.data.retryCount = 1
        self.data.retryDelay = 0

    def addOutputDestination(self, lfn):
        """
        Adds an out location to put a tarball of all logs
        """


class LogCollect(Template):
    """
    _LogCollect_

    Tools for creating a template LogCollect Step

    """

    def install(self, step):
        stepname = nodeName(step)
        step.stepType = "LogCollect"
        step.section_("logs")
        step.logcount = 0
        step.retryCount = 3
        step.retryDelay = 300


    def helper(self, step):
        """
        _helper_

        Wrap the WMStep provided in the CMSSW helper class that
        includes the ability to add and manipulate the details
        of a CMSSW workflow step

        """
        return LogCollectStepHelper(step)
