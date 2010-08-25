#!/usr/bin/env python
"""
_TaskSpace_

Frontend module for setting up TaskSpace & StepSpace areas within a job.

"""

from WMCore.WMRuntime import TaskSpace
from WMCore.WMRuntime import StepSpace


def establishTaskSpace():
    """
    _establishTaskSpace_

    Bootstrap method for the execution dir for a WMTask

    """
    return TaskSpace()

def establishStepSpace():
    """
    _establishStepSpace_

    Bootstrap method for the execution dir of a WMStep within a WMTask

    """
    return StepSpace()
