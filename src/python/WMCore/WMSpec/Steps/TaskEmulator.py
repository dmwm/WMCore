#!/usr/bin/env python
"""
_TaskEmulator_

Top level emulator controller

"""


class TaskEmulator:
    """
    _TaskEmulator_

    Top Level Emulator that contains a map of all the steps to step emulators
    and be able to run them

    Instantiate with the task to be emulated

    """
    def __init__(self, task):
        self.emulators = {}



    def __call__(self):
        """
        _operator()_

        Invoke the emulator

        """



