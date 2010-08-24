#!/usr/bin/env python
"""
_CMSSW_

Builder implementation for CMSSW

"""
from WMCore.WMSpec.Steps.Builder import Builder
from WMCore.WMSpec.ConfigSectionTree import nodeName





class CMSSW(Builder):
    """
    _CMSSW_

    Build a working area for a CMSSW step

    """

    def build(self, step, workingDir, **args):
        """
        _build_

        implement build interface for CMSSW Step

        """
        stepName = nodeName(step)
        stepWorkingArea = "%s/%s" % (workingDir, stepName)
        self.installWorkingArea(step, stepWorkingArea)
        print "Builders.CMSSW.build called on %s" % stepName






