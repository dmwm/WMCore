#!/usr/bin/env python
"""
_ScriptInterface_


API definition for RuntimeScripts that will be called in the job area.


"""
from builtins import object


class ScriptInterface(object):
    """
    _ScriptInterface_


    """
    def __init__(self):
        self.stepSpace = None

    def __call__(self):
        """
        _operator_

        Override this to perform the required tasks and return an appropriate
        exit code

        """
        return 0
