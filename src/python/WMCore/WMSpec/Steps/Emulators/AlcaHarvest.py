#!/usr/bin/env python
"""
_AlcaHarvest_

Basic Emulator for AlcaHarvest Step

"""
from __future__ import print_function

from WMCore.WMSpec.Steps.Emulator import Emulator

class AlcaHarvest(Emulator):
    """
    _AlcaHarvest_

    Emulate the execution of a AlcaHarvest Step

    """
    def pre(self):
        """
        _pre_

        Emulate pre AlcaHarvest

        """
        return None

    def execute(self):
        """
        _execute_

        Emulate AlcaHarvest execution

        """
        self.step.section_("execution")
        self.step.execution.exitStatus = 0
        self.step.section_("emulation")
        self.step.emulation.emulatedBy = str(self.__class__.__name__)

        print("Emulating AlcaHarvest Step")


    def post(self):
        """
        _post_

        Emulate post AlcaHarvest

        """
        return None
