#!/usr/bin/env python
"""
_LogArchive_

Builder implementation for LogArchive

"""
from __future__ import print_function

import logging

from WMCore.WMSpec.ConfigSectionTree import nodeName
from WMCore.WMSpec.Steps.Builder import Builder


class LogArchive(Builder):
    """
    _LogArchive_

    Build a working area for a LogArchive step

    """

    def build(self, step, workingDir, **args):
        """
        _build_

        implement build interface for StageOut Step

        """
        stepName = nodeName(step)
        stepWorkingArea = "%s/%s" % (workingDir, stepName)
        self.installWorkingArea(step, stepWorkingArea)
        logging.info("Builders.LogArchive.build called on %s", stepName)
