#!/usr/bin/env python
"""
_CMSSW_

Builder implementation for CMSSW

"""
from __future__ import print_function

import logging

from WMCore.WMSpec.ConfigSectionTree import nodeName
from WMCore.WMSpec.Steps.Builder import Builder


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
        logging.info("Builders.CMSSW.build called on %s", stepName)
