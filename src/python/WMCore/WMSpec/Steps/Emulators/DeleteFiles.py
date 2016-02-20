#!/usr/bin/env python
"""
_DeleteFiles_

Basic Emulator for DeleteFiles Step

"""
from __future__ import print_function

from WMCore.WMSpec.Steps.Emulator import Emulator

class DeleteFiles(Emulator):
    """
    _DeleteFiles_

    Emulate the execution of a DeleteFiles Step

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

        Emulate DeleteFiles execution

        """
        self.step.section_("execution")
        self.step.execution.exitStatus = 0
        self.step.section_("emulation")
        self.step.emulation.emulatedBy = str(self.__class__.__name__)

        print("Emulating DeleteFiles Step")


    def post(self):
        """
        _post_

        Emulate post stage out

        """
        return None
