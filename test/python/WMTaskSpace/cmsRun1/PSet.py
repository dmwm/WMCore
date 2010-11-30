#!/usr/bin/env python
"""
_Pset_

Bogus CMSSW PSet for testing runtime code.
"""

import pickle

class Container():
    """
    _Container_

    Empty class that we can use like a PSet.
    """
    pass

class Process():
    """
    _Process_

    Process class that has one output module.
    """
    outputRECORECO = Container()
    source = Container()
    services = {}

    def outputModules_(self):
        """
        _outputModules_

        Return a list of output modules.
        """
        return ["outputRECORECO"]

    def add_(self, service):
        """
        _add__

        Add a service to our services dict.
        """
        self.services[service.serviceName] = service

    def dumpPython(self):
        """
        _dumpPython_

        Pickle this object so that we can examine it later.
        """
        return pickle.dumps(self)

process = Process()
