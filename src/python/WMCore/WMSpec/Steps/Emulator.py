#!/usr/bin/env python2.5
"""
_Emulator_

Define the Interface for a Step Emulator


"""


class Emulator:
    """
    _Emulator_

    Base class for an Emulator implementation for a given step

    """
    def initialise(self, executorInstance):
        """
        _initialise_

        Post ctor initialisation, shortcut to some of the standard variables provided by the executor

        """
        self.executor = executorInstance
        self.job = self.executor.job
        self.report = self.executor.report
        self.task = self.executor.task
        self.step = self.executor.step
        self.stepSpace = self.executor.stepSpace
        self.stepName = self.executor.stepName


    def pre(self):
        """
        _pre_

        Override pre step to emulate
        """
        return None


    def execute(self):
        """
        _emulate_

        Emulate the response to the step provided

        """
        msg = "WMStep.Steps.Emulator.emulate not implemented for "
        msg += "class %s" % self.__class__.__name__
        raise NotImplementedError(msg)

    def post(self):
        """
        _post_

        Override to emulate post execution step

        """
        return None
