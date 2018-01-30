#!/usr/bin/env python
"""
_StageOut_

Builder implementation for StageOut

"""
from __future__ import print_function

import logging

from WMCore.WMSpec.ConfigSectionTree import nodeName
from WMCore.WMSpec.Steps.Builder import Builder


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
        logging.info("Builders.StageOut.build called on %s", stepName)
