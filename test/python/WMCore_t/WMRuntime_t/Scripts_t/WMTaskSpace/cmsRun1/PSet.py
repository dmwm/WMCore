#!/usr/bin/env python
"""
_Pset_

Bogus CMSSW PSet for testing runtime code.
"""

import pickle
import FWCore.ParameterSet.Config as cms

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
    producers = {}
    filters = {}

    def __init__(self):
        """
        __init__

        """
        mixing1 = cms.PSet(input = cms.PSet(fileNames = cms.untracked.vstring()))
        mixing1.setType("MixingModule")
        mixing2 = cms.PSet(input = cms.PSet(fileNames = cms.untracked.vstring()))
        mixing2.setType("MixingModule")
        notmixing1 = cms.PSet(input = cms.PSet(fileNames = cms.untracked.vstring()))
        notmixing1.setType("NotAMixingModule")
        notmixing2 = cms.PSet(input = cms.PSet(fileNames = cms.untracked.vstring()))
        notmixing2.setType("NotAMixingModule")
        
        self.producers["mix1"] = mixing1
        self.producers["notmix1"] = notmixing1
        self.filters["mix2"] = mixing2
        self.filters["notmix2"] = notmixing2

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
