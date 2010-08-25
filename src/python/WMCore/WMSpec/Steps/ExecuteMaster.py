#!/usr/bin/env python
"""
_ExecuteMaster_

Overseer object that traverses a task and invokes the type based executor
for each step

"""
__author__ = "evansde"
__revision__ = "$Id: ExecuteMaster.py,v 1.2 2009/05/08 13:12:06 evansde Exp $"
__version__ = "$Revision: 1.2 $"


class ExecuteMaster:
    """
    _ExecuteMaster_

    Traverse the given task and invoke the execute framework
    If an emulator is provided, then invoke the appropriate emulator
    instead of the executor

    """
    def __init__(self, emulator = None):
        self.emulator = emulator

