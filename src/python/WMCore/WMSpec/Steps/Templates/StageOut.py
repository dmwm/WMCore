#!/usr/bin/env python
"""
_StageOut_

Template for a StageOut Step

"""

from WMCore.WMSpec.Steps.Template import Template
from WMCore.WMSpec.Steps.Template import CoreHelper
from WMCore.WMSpec.ConfigSectionTree import nodeName

class StageOutStepHelper(CoreHelper):
    """
    _StageOutStepHelper_

    Add API calls and helper methods to the basic WMStepHelper to specialise
    for StageOut tasks

    """
    def addFile(self, infile, outfile):
        """
            Enqueues(sp?) a file to the StageOut step
        """
        target = self.data.files.section_("file%i" % self.data.filecount)
        target.input  = infile
        target.output = outfile
        self.data.filecount += 1 
    
    def disableRetries(self):
        """
            handy for testing, without the 10 minute retry loop
        """
        self.data.retryCount = 1
        self.data.retryDelay = 0


class StageOut(Template):
    """
    _StageOut_

    Tools for creating a template StageOut Step

    """

    def install(self, step):
        stepname = nodeName(step)
        step.stepType = "StageOut"
        step.section_("files")
        step.filecount = 0
        step.retryCount = 3
        step.retryDelay = 300


    def helper(self, step):
        """
        _helper_

        Wrap the WMStep provided in the CMSSW helper class that
        includes the ability to add and manipulate the details
        of a CMSSW workflow step

        """
        return StageOutStepHelper(step)



