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
    def emulate(self, step):
        """
        _emulate_

        Emulate the response to the step provided

        """
        msg = "WMStep.Steps.Emulator.emulate not implemented for "
        msg += "class %s" % self.__class__.__name__
        raise NotImplementedError, msg







