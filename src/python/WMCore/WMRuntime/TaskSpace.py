#!/usr/bin/env python
"""
_TaskSpace_


Runtime utils for a Task


"""

import os
import sys
import inspect


class TaskSpace:
    """
    _TaskSpace_

    Util container for runtime operations within a Task & its
    constituent steps

    """
    def __init__(self, **args):
        self.taskName = args['TaskName']
        self.location = os.path.dirname(inspect.getsourcefile(args['Locator']))




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
        if modName in sys.modules.keys():
            space = sys.modules[modName]
        else:
            try:
                space = __import__(modName, globals(), locals(), ['stepSpace'], -1)

            except ImportError, ex:
                # TODO: Dedicated exception class
                msg = "Unable to import StepSpace from %s" % modName
                raise RuntimeError, msg


        stepSpace = getattr(space, "stepSpace", None)
        if stepSpace == None:
            # TODO: Dedicated Exception class
            msg = "No stepSpace Attribute in module %s" % modName
            raise RuntimeError, msg

        setattr(stepSpace, "taskSpace", self)

        return stepSpace


