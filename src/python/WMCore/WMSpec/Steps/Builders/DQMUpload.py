#!/usr/bin/env python
"""
_DQMUpload_

Builder implementation for DQMUpload

"""
from __future__ import print_function

import logging

from WMCore.WMSpec.ConfigSectionTree import nodeName
from WMCore.WMSpec.Steps.Builder import Builder


class DQMUpload(Builder):
    """
    _DQMUpload_

    Build a working area for a DQMUpload step

    """

    def build(self, step, workingDir, **args):
        """
        _build_

        implement build interface for DQMUpload Step

        """
        stepName = nodeName(step)
        stepWorkingArea = "%s/%s" % (workingDir, stepName)
        self.installWorkingArea(step, stepWorkingArea)
        logging.info("Builders.DQMUpload.build called on %s", stepName)
