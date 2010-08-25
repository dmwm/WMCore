#!/usr/bin/env python
"""
_BuildMaster_

Overseer object that traverses a task and invokes the type based builder
for each step

"""
__author__ = "evansde"
__revision__ = "$Id: BuildMaster.py,v 1.1 2009/05/07 17:53:18 evansde Exp $"
__version__ = "$Revision: 1.1 $"



class BuildMaster:
    """
    _BuildMaster_

    """
    def __init__(self, workingDir):
        self.workDir = workingDir


    def __call__(self, task):
        """
        _operator(task)_

        Invoke the builder on the task provided

        """

        pass
