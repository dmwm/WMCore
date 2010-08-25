#!/usr/bin/env python
"""
_StageOut_

Basic Emulator for StageOut Step

"""

from WMCore.WMSpec.Steps.Emulator import Emulator

class StageOut(Emulator):
    """
    _StageOut_

    Emulate the execution of a StageOut Step

    """
    def emulate(self, step):
        """
        _emulate_


        """
        step.section_("execution")
        step.execution.exitStatus = 0
        step.section_("emulation")
        step.emulation.emulatedBy = str(self.__class__.__name__)

        print "Emulating StageOut Step"
