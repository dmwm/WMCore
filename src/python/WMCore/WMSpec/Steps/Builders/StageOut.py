#!/usr/bin/env python
"""
_StageOut_

Builder implementation for StageOut

"""
from WMCore.WMSpec.Steps.Builder import Builder
from WMCore.WMSpec.ConfigSectionTree import nodeName





class StageOut(Builder):
    """
    _StageOut_

    Build a working area for a StageOut step

    """

    def build(self, step, workingDir, **args):
        """
        _build_

        implement build interface for StageOut Step

        """
        stepName = nodeName(step)
        stepWorkingArea = "%s/%s" % (workingDir, stepName)
        self.installWorkingArea(step, stepWorkingArea)
        print "Builders.StageOut.build called on %s" % stepName






