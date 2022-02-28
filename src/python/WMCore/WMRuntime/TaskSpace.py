#!/usr/bin/env python
"""
_TaskSpace_


Runtime utils for a Task


"""

from builtins import object
import os
import sys
import inspect
import pickle

from WMCore.WMSpec.WMWorkload import WMWorkloadHelper


def preloadWorkload(x):
    """
    _preloadWorkload_

    Method decorator to ensure that accessors to the workload are
    called only when the workload has been loaded

    Use only for decorating no-arg getters

    """
    def wrapper(self):
        self.getWMWorkload()
        return x(self)
    return wrapper

class TaskSpace(object):
    """
    _TaskSpace_

    Util container for runtime operations within a Task & its
    constituent steps

    """
    def __init__(self, **args):
        self.taskName = args['TaskName']
        self.location = os.path.dirname(inspect.getsourcefile(args['Locator']))
        self.task = None
        self.workload = None







    def getWMWorkload(self):
        """
        _getWMTask_

        Get the WMTask for this space

        TODO: Refactor to getWMWorkload method

        """
        if self.workload != None:
            return self.workload
        try:
            import WMSandbox
        except ImportError as ex:
            msg = "Error importing WMSandbox module"
            msg += str(ex)
            raise RuntimeError(msg)

        wmsandboxLoc = inspect.getsourcefile(WMSandbox)
        workloadPcl = wmsandboxLoc.replace("__init__.py","WMWorkload.pkl")

        with open(workloadPcl, 'rb') as handle:
            wmWorkload = pickle.load(handle)
        self.workload = WMWorkloadHelper(wmWorkload)
        return

    @preloadWorkload
    def workloadName(self):
        return self.workload.name()

    @preloadWorkload
    def getWMTask(self):
        """
        _getWMTask_

        Get the WMTask instance from the workload

        """
        return self.workload.getTaskByPath(self.taskName)


    def stepSpaces(self):
        """
        _stepSpaces_

        List step spaces available in this

        """
        result = []
        for item in os.listdir(self.location):
            location = os.path.join(self.location, item)
            if not os.path.isdir(location): continue
            initModule = os.path.join(location, "__init__.py")
            if not os.path.exists(initModule): continue
            #TODO: Test that it is really a StepSpace not some random py module
            result.append(item)
        return result

    def stepSpace(self, stepName):
        """
        _stepSpace_

        Load in the step Space with the name provided.
        Returns the step space instance from the directory

        """
        modName = "WMTaskSpace.%s" % stepName
        if modName in sys.modules:
            space = sys.modules[modName]
        else:
            try:
                #space = __import__(modName, globals(), locals(), ['stepSpace'], -1)
                space = __import__(modName, globals(), locals(), ['stepSpace'])

            except ImportError as ex:
                # TODO: Dedicated exception class
                msg = "Unable to import StepSpace from %s:\n" % modName
                msg += str(ex)
                raise RuntimeError(msg)


        stepSpace = getattr(space, "stepSpace", None)
        if stepSpace == None:
            # TODO: Dedicated Exception class
            msg = "No stepSpace Attribute in module %s" % modName
            raise RuntimeError(msg)

        setattr(stepSpace, "taskSpace", self)

        return stepSpace
