#!/usr/bin/env python
"""
_Pset_

Bogus CMSSW PSet for testing runtime code.
"""

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
        mixing2 = cms.PSet(secsource = cms.PSet(fileNames = cms.untracked.vstring()))
        mixing2.setType("MixingModule")
        datamixing1 = cms.PSet(secsource = cms.PSet(fileNames = cms.untracked.vstring()))
        datamixing1.setType("DataMixingModule")
        datamixing2 = cms.PSet(input = cms.PSet(fileNames = cms.untracked.vstring()))
        datamixing2.setType("DataMixingModule")

        self.producers["mix1"] = mixing1
        self.producers["datamix1"] = datamixing1
        self.filters["mix2"] = mixing2
        self.filters["datamix2"] = datamixing2

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
        setattr(self, service.serviceName, service)
        return

process = Process()
