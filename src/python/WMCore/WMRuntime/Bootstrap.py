#!/usr/bin/env python
"""
_TaskSpace_

Frontend module for setting up TaskSpace & StepSpace areas within a job.

"""

from WMCore.WMRuntime import TaskSpace
from WMCore.WMRuntime import StepSpace


def establishTaskSpace(**args):
    """
    _establishTaskSpace_

    Bootstrap method for the execution dir for a WMTask

    """
    return TaskSpace.TaskSpace(**args)

def establishStepSpace(**args):
    """
    _establishStepSpace_

    Bootstrap method for the execution dir of a WMStep within a WMTask

    """
    return StepSpace.StepSpace(**args)
