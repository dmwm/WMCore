#!/usr/bin/env python
"""
_LogCollect_

Builder implementation for LogCollect

"""
from __future__ import print_function

import logging

from WMCore.WMSpec.ConfigSectionTree import nodeName
from WMCore.WMSpec.Steps.Builder import Builder


class LogCollect(Builder):
    """
    _LogCollect_

    Build a working area for a LogCollect step

    """

    def build(self, step, workingDir, **args):
        """
        _build_

        implement build interface for LogCollect Step

        """
        stepName = nodeName(step)
        stepWorkingArea = "%s/%s" % (workingDir, stepName)
        self.installWorkingArea(step, stepWorkingArea)
        logging.info("Builders.LogCollect.build called on %s", stepName)
