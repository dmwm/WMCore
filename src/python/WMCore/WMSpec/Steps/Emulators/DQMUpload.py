#!/usr/bin/env python
"""
_DQMUpload_

Basic Emulator for DQMUpload Step

"""
from __future__ import print_function

from WMCore.WMSpec.Steps.Emulator import Emulator

class DQMUpload(Emulator):
    """
    _DQMUpload_

    Emulate the execution of a DQMUpload Step

    """
    def pre(self):
        """
        _pre_

        Emulate the dqm upload pre step

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

        print("Emulating DQMUpload Step")


    def post(self):
        """
        _post_

        Emulate post dqm upload

        """
        return None
