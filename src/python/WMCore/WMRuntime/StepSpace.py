#!/usr/bin/env python
"""
_StepSpace_

Frontend module for setting up StepSpace areas within a job.

"""
from builtins import object
import os
import inspect
from WMCore.WMRuntime.Sandbox import Sandbox

class StepSpace(object):
    """
    _StepSpace_

    Working area utils for a Step during execution
    TaskSpace reference is set when StepSpace is retrieved from top level
    task space instance

    """
    def __init__(self, **args):
        self.taskSpace = None
        self.args = args
        self.stepName = args.get("StepName", None)
        self.initmodule = inspect.getsourcefile(args.get("Locator", None))
        self.location  = os.path.dirname(self.initmodule)
        self.sandbox = Sandbox(args["TaskName"], self.stepName)

    def sandboxFiles(self):
        """
        _sandboxFiles_

        Get details of sandbox files for this step from the
        WMSandbox

        """
        return self.sandbox.listFiles()


    def getFromSandbox(self, filename, target = None):
        """
        _getFromSandbox_

        Copy a file from the Sandbox area to the Step runtime area.
        Will have same basename as in sandbox unless a target name
        is provided

        """
        sbox = self.sandbox.getFile(filename)
        if target == None:
            target = filename
        destination = os.path.join(self.location, target)
        os.system("/bin/cp %s %s" % (sbox, destination))
        return


    def getWMTask(self):
        """
        _getWMTask_

        Get the WMTask from the TaskSpace

        """
        return self.taskSpace.getWMTask()

    def getWMStep(self):
        """
        _getWMStep_

        Get the WMStep for this Step

        """
        task = self.taskSpace.getWMTask()
        return task.getStep(self.stepName)
