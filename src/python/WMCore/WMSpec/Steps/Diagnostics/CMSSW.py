#!/usr/bin/env python
"""
_CMSSW_

Diagnostic implementation for a CMSSW job


"""


from WMCore.WMSpec.Steps.Diagnostic import Diagnostic, DiagnosticHandler

class ExeNotFoundHandler(DiagnosticHandler):

    def __call__(self, errCode, executor, **args):
        msg = "Executable Not Found"
        print msg


class CMSSW(Diagnostic):

    def __init__(self):
        Diagnostic.__init__(self)
        self.handlers[127] = ExeNotFoundHandler()
