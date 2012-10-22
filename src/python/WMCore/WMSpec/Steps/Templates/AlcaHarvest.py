#!/usr/bin/env python
"""
_AlcaHarvest_

Template for a AlcaHarvest Step

"""

from WMCore.WMSpec.Steps.Template import Template
from WMCore.WMSpec.Steps.Template import CoreHelper
from WMCore.WMSpec.ConfigSectionTree import nodeName


class AlcaHarvestStepHelper(CoreHelper):
    """
    _AlcaHarvestStepHelper_

    Add API calls and helper methods to the basic WMStepHelper to specialise
    for AlcaHarvest tasks

    """
    def setRunNumber(self, runNumber):
        """
        Sets the run number, used to create a
        run specific subdir for the output
        """
        self.data.condition.runNumber = runNumber

    def setConditionOutputLabel(self, outLabel):
        """
        Sets the output label for the fake
        output model containing the sqlite files
        """
        self.data.condition.outLabel = outLabel

    def setConditionDir(self, dir):
        """
        Sets the directory for condition file copy,
        assumed to be a POSIX accessible path (AFS)
        """
        self.data.condition.dir = dir


class AlcaHarvest(Template):
    """
    _AlcaHarvest_

    Tools for creating a template AlcaHarvest Step

    """

    def install(self, step):
        stepname = nodeName(step)
        step.stepType = "AlcaHarvest"
        step.section_("condition")
        step.condition.runNumber = None
        step.condition.outLabel = None
        step.condition.dir = None

    def helper(self, step):
        """
        _helper_

        Wrap the WMStep provided in the CMSSW helper class that
        includes the ability to add and manipulate the details
        of a CMSSW workflow step

        """
        return AlcaHarvestStepHelper(step)
