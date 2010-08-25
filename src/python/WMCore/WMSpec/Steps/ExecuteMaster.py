#!/usr/bin/env python
"""
_ExecuteMaster_

Overseer object that traverses a task and invokes the type based executor
for each step

"""
__author__ = "evansde"
__revision__ = "$Id: ExecuteMaster.py,v 1.4 2009/10/19 20:32:03 evansde Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMSpec.WMStep import WMStepHelper
import WMCore.WMSpec.Steps.StepFactory as StepFactory
import inspect, os

class ExecuteMaster:
    """
    _ExecuteMaster_

    Traverse the given task and invoke the execute framework
    If an emulator is provided, then invoke the appropriate emulator
    instead of the executor

    """
    def __init__(self, emulator = None):
        self.emulator = emulator



    def __call__(self, task, wmbsJob):
        """
        _operator(task)_

        Load and run executors for all steps in Task, if an emulator is
        available for that step, use it instead.

        """
        for step in task.steps().nodeIterator():
            helper = WMStepHelper(step)
            stepType = helper.stepType()
            stepName = helper.name()
            emu = None
            if self.emulator != None:
                emu = self.emulator.getEmulator(stepName)
            if emu != None:
                emu.emulate(step)
            else:
                executor = StepFactory.getStepExecutor(stepType)
                self.doExecution(executor, step, wmbsJob)
        return

    def doExecution(self, executor, step, job):
        """
        _doExecution_

        Invoke the Executor for the step provided

        TODO: Fork subprocess
        TODO: Add Monitoring thread & setup
        TODO: Exception Handling
        TODO: pre/post outcome can change the next execution task, need to
              ensure that this happens

        """
        self.toStepDirectory(step)
        preOutcome = executor.pre(step)
        if preOutcome != None:
            print "Pre Executor Task Change: %s" % preOutcome
            print "TODO: Implement Me!!!"
            self.toTaskDirectory()
        executor.execute(step, job)

        postOutcome = executor.post(step)
        if postOutcome != None:
            print "Pre Executor Task Change: %s" % preOutcome
            print "TODO: Implement Me!!!"
            self.toTaskDirectory()
        self.toTaskDirectory()

    def toStepDirectory(self, step):
        """
        _toStepDirectory_

        Switch current working directory to the step location
        within WMTaskSpace

        """
        stepName = WMStepHelper(step).name()
        from WMTaskSpace import taskSpace
        stepSpace = taskSpace.stepSpace(stepName)

        os.chdir(stepSpace.location)



    def toTaskDirectory(self):
        """
        _toTaskDirectory_

        Switch to current working directory to the task location
        within WMTaskSpace

        """
        from WMTaskSpace import taskSpace
        os.chdir(taskSpace.location)
        return




