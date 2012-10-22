#!/usr/bin/env python
"""
_LogArchive_

Template for a LogArchive Step

Mostly borrowed from StageOut since they share a similar function

"""




from WMCore.WMSpec.Steps.Template import Template
from WMCore.WMSpec.Steps.Template import CoreHelper
from WMCore.WMSpec.ConfigSectionTree import nodeName



class LogArchiveStepHelper(CoreHelper):
    """
    _StageOutStepHelper_

    Add API calls and helper methods to the basic WMStepHelper to specialise
    for StageOut tasks

    This is very similar to StageOut since they have essentially the same function

    """
    def addLog(self, infile):
        """
            Enqueues(sp?) a file to the StageOut step
            infile must be a PFN
            outfile must be a LFN
        """
        target = self.data.logs.section_("log%i" % self.data.filecount)
        target.input  = infile
        self.data.filecount += 1

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


class LogArchive(Template):
    """
    _LogArchive_

    Tools for creating a template LogArchive Step

    """

    def install(self, step):
        stepname = nodeName(step)
        step.stepType = "LogArchive"
        step.section_("logs")
        step.logcount = 0
        step.retryCount = 3
        step.retryDelay = 300

        # Create output to put logs in
        step.section_("output")
        step.output.section_("modules")
        step.output.modules.section_("logArchive")


    def helper(self, step):
        """
        _helper_

        Wrap the WMStep provided in the CMSSW helper class that
        includes the ability to add and manipulate the details
        of a CMSSW workflow step

        """
        return LogArchiveStepHelper(step)
