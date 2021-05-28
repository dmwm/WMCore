#!/usr/bin/env python
"""
_Pset_

Bogus CMSSW PSet for testing runtime code.
"""
import FWCore.ParameterSet.Config as cms

process = cms.Process('Test')

# import of standard configurations for Services and mixing modules
process.load('Configuration.StandardSequences.Services_cff')
process.load('SimGeneral.MixingModule.mixNoPU_cfi')
process.load('Configuration.StandardSequences.FrontierConditions_GlobalTag_cff')

# Input source
process.source = cms.Source("EmptySource")
process.source.fileNames = cms.untracked.vstring()
process.source.secondaryFileNames = cms.untracked.vstring()
process.source.firstLuminosityBlock = cms.untracked.uint32(1)

process.maxEvents = cms.untracked.PSet(
    input = cms.untracked.int32(10),
    output = cms.optional.untracked.allowed(cms.int32,cms.PSet)
)

process.options = cms.untracked.PSet()
process.options.numberOfThreads = 8
process.options.numberOfStreams = 2
