#!/usr/bin/env python
"""
_StepSpace_

Frontend module for setting up StepSpace areas within a job.

"""

class StepSpace:
    """
    _StepSpace_

    Working area utils for a Step during execution
    TaskSpace reference is set when StepSpace is retrieved from top level
    task space instance

    """
    def __init__(self, **args):
        self.taskSpace = None
        pass


