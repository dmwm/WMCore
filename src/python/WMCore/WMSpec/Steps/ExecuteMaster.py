#!/usr/bin/env python
"""
_ExecuteMaster_

Overseer object that traverses a task and invokes the type based executor
for each step

"""
__author__ = "evansde"
__revision__ = "$Id: ExecuteMaster.py,v 1.1 2009/05/07 17:53:18 evansde Exp $"
__version__ = "$Revision: 1.1 $"


class ExecuteMaster:
    """
    _ExecuteMaster_

    Traverse the given task and invoke the execute framework
    If an emulator is provided, then invoke the appropriate emulator
    instead of the executor

    """
    def __init__(self, emulator = None):
        self.emulator = emulator
