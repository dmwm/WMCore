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
    def addFile(self, infile, outfile, stageOut = True):
        """
            Enqueues(sp?) a file to the StageOut step
            infile must be a LOCAL FILENAME. No file:/// in front
            outfile must be a LFN which will be mapped to the remote
        """
        target = self.data.files.section_("file%i" % self.data.filecount)
        target.input         = infile
        target.output        = outfile
        target.stageOut      = stageOut
        self.data.filecount += 1

    def disableRetries(self):
        """
            handy for testing, without the 10 minute retry loop
        """
        self.data.retryCount = 1
        self.data.retryDelay = 0

    def disableStraightToMerge(self):
        """
        _disableStraightToMerge_

        Disable straight to merge for this step.
        """
        if hasattr(self.data.output, "minMergeSize"):
            delattr(self.data.output, "minMergeSize")

        return

    def disableStraightToMergeForOutputModules(self, outputModules):
        """
        _disableStraightToMergeForOutputModules_

        Disable straight to merge only for these output modules.
        """
        self.data.output.forceUnmergedOutputs = outputModules

        return

    def setMinMergeSize(self, minMergeSize, maxMergeEvents):
        """
        _setMinMergeSize_

        Set the mininum size for promoting a file to merged status.
        """
        self.data.output.minMergeSize = minMergeSize
        self.data.output.maxMergeEvents = maxMergeEvents
        return

    def minMergeSize(self):
        """
        _minMergeSize_

        Retrieve the minimum size for promoting a file to merged status.  If
        straight to merge is disabled -1 will be returned.
        """
        return getattr(self.data.output, "minMergeSize", -1)

class StageOut(Template):
    """
    _StageOut_

    Tools for creating a template StageOut Step

    """

    def install(self, step):
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
