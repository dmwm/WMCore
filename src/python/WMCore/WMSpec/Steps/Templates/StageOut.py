#!/usr/bin/env python
"""
_StageOut_

Template for a StageOut Step

"""

from WMCore.WMSpec.Steps.Template import Template
from WMCore.WMSpec.Steps.Template import CoreHelper
from WMCore.WMSpec.ConfigSectionTree import nodeName

class StageOutStepHelper(CoreHelper):
    """
    _StageOutStepHelper_

    Add API calls and helper methods to the basic WMStepHelper to specialise
    for StageOut tasks

    """





class StageOut(Template):
    """
    _StageOut_

    Tools for creating a template StageOut Step

    """

    def install(self, step):
        stepname = nodeName(step)
        step.stepType = "StageOut"



    def helper(self, step):
        """
        _helper_

        Wrap the WMStep provided in the CMSSW helper class that
        includes the ability to add and manipulate the details
        of a CMSSW workflow step

        """
        return StageOutStepHelper(step)



