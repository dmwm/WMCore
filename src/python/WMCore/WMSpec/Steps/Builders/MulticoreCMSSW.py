#!/usr/bin/env python
"""
_CMSSW_

Builder implementation for CMSSW

"""
from WMCore.WMSpec.Steps.Builder import Builder
from WMCore.WMSpec.ConfigSectionTree import nodeName





class MulticoreCMSSW(Builder):
    """
    _MulticoreCMSSW_

    Build a working area for a MulticoreCMSSW step
    (Exactly same as normal CMSSW step but doing everything in parallel to start with)
    """

    def build(self, step, workingDir, **args):
        """
        _build_

        implement build interface for CMSSW Step

        """
        stepName = nodeName(step)
        stepWorkingArea = "%s/%s" % (workingDir, stepName)
        self.installWorkingArea(step, stepWorkingArea)
        print "Builders.MulticoreCMSSW.build called on %s" % stepName
