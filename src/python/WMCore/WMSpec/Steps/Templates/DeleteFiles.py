#!/usr/bin/env python
"""
_DeleteFiles_

Template for a DeleteFiles Step

Mostly borrowed from StageOut since they share a similar function

"""




from WMCore.WMSpec.Steps.Template import Template
from WMCore.WMSpec.Steps.Template import CoreHelper
from WMCore.WMSpec.ConfigSectionTree import nodeName



class DeleteFilesStepHelper(CoreHelper):
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


class DeleteFiles(Template):
    """
    _DeleteFiles_

    Tools for creating a template DeleteFiles Step

    """

    def install(self, step):
        stepname = nodeName(step)
        step.stepType = "DeleteFiles"
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
        return DeleteFilesStepHelper(step)
