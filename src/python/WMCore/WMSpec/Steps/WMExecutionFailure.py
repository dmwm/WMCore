#!/usr/bin/env python
"""
_WMExecutionFailure_

Error class to generate when an executor implementation encounters a problem

"""



from WMCore.WMException import WMException


class WMExecutionFailure(WMException):


    def __init__(self, code, name, detail):
        WMException.__init__(self, detail, code)
        self.code = code
        self.name = name
        self.detail = detail
