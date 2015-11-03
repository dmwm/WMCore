#!/usr/bin/env python
"""
_LogCollect_

Basic Emulator for LogCollect Step

"""
from __future__ import print_function

import os
import os.path
import re
import shutil

from WMCore.WMSpec.Steps.Emulator import Emulator

class LogCollect(Emulator):
    """
    _LogCollect_

    Emulate the execution of a LogCollect Step

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

        Emulate LogCollect execution

        """
        self.step.section_("execution")
        self.step.execution.exitStatus = 0
        self.step.section_("emulation")
        self.step.emulation.emulatedBy = str(self.__class__.__name__)

        print("Emulating LogCollect Step")

        output = getattr(self.report.data, self.stepName)
        output.outputPFN = 'ThisIsAPFN'
        output.PNN       = 'ThisIsAPNN'
        output.LFN       = 'ThisIsALFN'

        print("Have done emulation")
        print(self.report.data)

        return



    def post(self):
        """
        _post_

        Emulate post stage out

        """
        return None
