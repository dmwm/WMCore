#!/usr/bin/env python
"""
_TaskEmulator_

Top level emulator controller

"""

from builtins import object
from WMCore.WMSpec.WMStep import WMStepHelper
import WMCore.WMSpec.Steps.StepFactory as StepFactory


class TaskEmulator(object):
    """
    _TaskEmulator_

    Top Level Emulator that contains a map of all the steps to step emulators
    and be able to run them

    Instantiate with the task to be emulated

    """
    def __init__(self, task):
        self.task = task
        self.emulators = {}

    def getEmulator(self, stepName):
        """
        _getEmulator_

        Retrieve the Emulator for the step name provided, returns None if
        there is no emulator

        """
        return self.emulators.get(stepName, None)


    def addEmulator(self, nodeName, emulatorName):
        """
        _addEmulator_

        Add an Emulator for the node provided, emulatorName is the name
        of the emulator class to be loaded by the Emulator factory

        TODO: Exception handling

        """
        emuInstance = StepFactory.getStepEmulator(emulatorName)
        self.emulators[nodeName] = emuInstance
        return

    def emulateAll(self):
        """
        _emulateAll_

        Traverse all Steps and load up the default Emulator based on
        type.

        """
        for step in self.task.steps().nodeIterator():
            helper = WMStepHelper(step)
            stepType = helper.stepType()
            stepName = helper.name()
            self.addEmulator(stepName, stepType)



    def __call__(self, step):
        """
        _operator(step)_

        Invoke the emulator for the given step

        """
