#!/usr/bin/env python
"""
_LogArchive_

Basic Emulator for LogArchive Step

"""

from WMCore.WMSpec.Steps.Emulator import Emulator

class LogArchive(Emulator):
    """
    _LogArchive_

    Emulate the execution of a LogArchive Step

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

        Emulate LogArchive execution

        """
        self.step.section_("execution")
        self.step.execution.exitStatus = 0
        self.step.section_("emulation")
        self.step.emulation.emulatedBy = str(self.__class__.__name__)

        print "Emulating LogArchive Step"


    def post(self):
        """
        _post_

        Emulate post stage out

        """
        return None
