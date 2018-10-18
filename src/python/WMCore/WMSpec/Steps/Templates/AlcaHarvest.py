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

    def setConditionLFNBase(self, lfnbase):
        """
        Sets the LFNBase for condition file copy
        (within the EOSCMS CERN EOS instance)
        """
        self.data.condition.lfnbase = lfnbase

    def setLuminosityURL(self, url):
        """
        Sets the ROOT URL for luminosity file copy
        """
        self.data.luminosity.url = url

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
        step.condition.lfnbase = None
        step.section_("luminosity")
        step.luminosity.url = None

    def helper(self, step):
        """
        _helper_

        Wrap the WMStep provided in the CMSSW helper class that
        includes the ability to add and manipulate the details
        of a CMSSW workflow step

        """
        return AlcaHarvestStepHelper(step)
