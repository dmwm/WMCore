#!/usr/bin/env python
"""
_DeleteFiles_

Builder implementation for DeleteFiles

"""
from __future__ import print_function
from WMCore.WMSpec.Steps.Builder import Builder
from WMCore.WMSpec.ConfigSectionTree import nodeName





class DeleteFiles(Builder):
    """
    _DeleteFiles_

    Build a working area for a DeleteFiles step

    """

    def build(self, step, workingDir, **args):
        """
        _build_

        implement build interface for DeleteFiles Step

        """
        stepName = nodeName(step)
        stepWorkingArea = "%s/%s" % (workingDir, stepName)
        self.installWorkingArea(step, stepWorkingArea)
        print("Builders.DeleteFiles.build called on %s" % stepName)
