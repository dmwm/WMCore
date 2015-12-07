#!/usr/bin/env python
"""
_Generic_

Generic Diagnostic implementation for step types with lazy authors


"""


from WMCore.WMSpec.Steps.Diagnostic import Diagnostic, DiagnosticHandler



class Generic(Diagnostic):

    def __init__(self):
        Diagnostic.__init__(self)
