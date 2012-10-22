#!/usr/bin/env python
"""
_StageOutError_

General Exception class for JC modules

"""




import inspect
import sys

from WMCore.WMException import WMException

ErrorDefinitions = {
    60311 : "GeneralStageOutFailure",
    60315 : "StageOutInitError",
    }


class StageOutError(WMException):
    """
    _StageOutError_

    Exception class which works out details of where
    it was raised.

    """
    def __init__(self, message, **data):
        WMException.__init__(self, message, **data)

        self.data.setdefault("ErrorCode", 60311)
        self.data.setdefault("ErrorType", ErrorDefinitions[60311])

class StageOutFailure(StageOutError):
    """
    _StageOutFailure_

    Standard Error class to indicate a stage out failure

    """
    def __init__(self, message, **data):
        StageOutError.__init__(self, message, **data)
        self.data.setdefault("ErrorCode", 60311)
        self.data.setdefault("ErrorType", ErrorDefinitions[60311])

        return

class StageOutInitError(StageOutError):
    """
    _StageOutInitError_

    Error when initialising for stage out including reading
    site conf, TFC etc

    """
    def __init__(self, message, **data):
        StageOutError.__init__(self, message, **data)
        self.data["ErrorCode"] =  60315
        self.data["ErrorType"] = ErrorDefinitions[60315]

class StageOutInvalidPath(StageOutError):
    """
    Exception class indicating a directory does not exist
    """
    def __init__(self, messgae = '', **data):
        StageOutError.__init__(self, messgae, **data)
