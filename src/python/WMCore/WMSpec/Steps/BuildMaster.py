#!/usr/bin/env python
"""
_BuildMaster_

Overseer object that traverses a task and invokes the type based builder
for each step

"""
__author__ = "evansde"
__revision__ = "$Id: BuildMaster.py,v 1.3 2009/05/08 16:22:35 evansde Exp $"
__version__ = "$Revision: 1.3 $"


import WMCore.WMSpec.Steps.StepFactory as StepFactory

class BuildMaster:
    """
    _BuildMaster_

    """
    def __init__(self, workingDir):
        self.workDir = workingDir


    def __call__(self, task):
        """
        _operator(task)_

        Invoke the builder on the task provided

        TODO: Build top level directory
        TODO: Exception handling

        """
        #
        #if not os.path.exists(self.workingDir):
        #   os.makedirs(self
        for step in task.steps().nodeIterator():
            stepType = step.stepType
            builder = StepFactory.getStepBuilder(stepType)
            builder.build(step, self.workDir)

