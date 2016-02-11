#!/usr/bin/env python
"""
_AlcaHarvest_

Builder implementation for AlcaHarvest

"""
from __future__ import print_function
from WMCore.WMSpec.Steps.Builder import Builder
from WMCore.WMSpec.ConfigSectionTree import nodeName


class AlcaHarvest(Builder):
    """
    _AlcaHarvest_

    Build a working area for a AlcaHarvest step

    """

    def build(self, step, workingDir, **args):
        """
        _build_

        implement build interface for DQMUpload Step

        """
        stepName = nodeName(step)
        stepWorkingArea = "%s/%s" % (workingDir, stepName)
        self.installWorkingArea(step, stepWorkingArea)
        print("Builders.AlcaHarvest.build called on %s" % stepName)
