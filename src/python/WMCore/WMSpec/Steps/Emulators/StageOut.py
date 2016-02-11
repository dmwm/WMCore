#!/usr/bin/env python
"""
_StageOut_

Basic Emulator for StageOut Step

"""
from __future__ import print_function

from WMCore.WMSpec.Steps.Emulator import Emulator

class StageOut(Emulator):
    """
    _StageOut_

    Emulate the execution of a StageOut Step

    """
    def pre(self):
        """
        _pre_

        Emulate the stage out pre step

        """
        return None

    def execute(self):
        """
        _execute_

        Emulate StageOut execution

        """
        self.step.section_("execution")
        self.step.execution.exitStatus = 0
        self.step.section_("emulation")
        self.step.emulation.emulatedBy = str(self.__class__.__name__)

        print("Emulating StageOut Step")


    def post(self):
        """
        _post_

        Emulate post stage out

        """
        return None
