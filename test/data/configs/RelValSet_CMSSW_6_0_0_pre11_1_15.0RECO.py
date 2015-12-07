# Auto generated configuration file
# using:
# Revision: 1.392
# Source: /local/reps/CMSSW/CMSSW/Configuration/PyReleaseValidation/python/ConfigBuilder.py,v
# with command line options: step3 --datatier GEN-SIM-RECO,DQM --conditions auto:startup -s RAW2DIGI,L1Reco,RECO,VALIDATION,DQM --eventcontent RECOSIM,DQM --io RECO.io --python RECO.py -n 100 --no_exec --filein file:step2.root --fileout file:step3.root
# modified to reduce outside dependencies - this probably doesn't do anything useful anymore
# hopefully its still useful as a test

import FWCore.ParameterSet.Config as cms

process = cms.Process('RECO')

# import of standard configurations
#process.load('Configuration.StandardSequences.Services_cff')
#process.load('SimGeneral.HepPDTESSource.pythiapdt_cfi')
#process.load('FWCore.MessageService.MessageLogger_cfi')
#process.load('Configuration.EventContent.EventContent_cff')
#process.load('SimGeneral.MixingModule.mixNoPU_cfi')
#process.load('Configuration.StandardSequences.GeometryRecoDB_cff')
#process.load('Configuration.StandardSequences.MagneticField_38T_cff')
#process.load('Configuration.StandardSequences.RawToDigi_cff')
#process.load('Configuration.StandardSequences.L1Reco_cff')
#process.load('Configuration.StandardSequences.Reconstruction_cff')
#process.load('Configuration.StandardSequences.Validation_cff')
#process.load('DQMOffline.Configuration.DQMOfflineMC_cff')
#process.load('Configuration.StandardSequences.EndOfProcess_cff')
#process.load('Configuration.StandardSequences.FrontierConditions_GlobalTag_cff')

process.maxEvents = cms.untracked.PSet(
    input = cms.untracked.int32(100)
)

# Input source
process.source = cms.Source("PoolSource",
    secondaryFileNames = cms.untracked.vstring(),
    fileNames = cms.untracked.vstring('file:step2.root')
)

process.options = cms.untracked.PSet(

)

# Production Info
process.configurationMetadata = cms.untracked.PSet(
    version = cms.untracked.string('$Revision: 1.392 $'),
    annotation = cms.untracked.string('step3 nevts:100'),
    name = cms.untracked.string('PyReleaseValidation')
)

# Output definition

process.RECOSIMoutput = cms.OutputModule("PoolOutputModule",
    splitLevel = cms.untracked.int32(0),
    eventAutoFlushCompressedSize = cms.untracked.int32(5242880),
#    outputCommands = process.RECOSIMEventContent.outputCommands,
    fileName = cms.untracked.string('file:step3.root'),
    dataset = cms.untracked.PSet(
        filterName = cms.untracked.string(''),
        dataTier = cms.untracked.string('GEN-SIM-RECO')
    )
)

process.DQMoutput = cms.OutputModule("PoolOutputModule",
    splitLevel = cms.untracked.int32(0),
#    outputCommands = process.DQMEventContent.outputCommands,
    fileName = cms.untracked.string('file:step3_inDQM.root'),
    dataset = cms.untracked.PSet(
        filterName = cms.untracked.string(''),
        dataTier = cms.untracked.string('DQM')
    )
)

# Additional output definition

# Other statements
#process.mix.playback = True
#process.mix.digitizers = cms.PSet()
for a in process.aliases: delattr(process, a)
#process.RandomNumberGeneratorService.restoreStateLabel=cms.untracked.string("randomEngineStateProducer")
#process.GlobalTag.globaltag = 'START60_V4::All'

# Path and EndPath definitions
process.raw2digi_step = cms.Path() #cms.Path(process.RawToDigi)
process.L1Reco_step = cms.Path() #cms.Path(process.L1Reco)
process.reconstruction_step = cms.Path() #cms.Path(process.reconstruction)
process.prevalidation_step = cms.Path() #cms.Path(process.prevalidation)
process.dqmoffline_step = cms.Path() #cms.Path(process.DQMOffline)
process.validation_step = cms.EndPath() #cms.EndPath(process.validation)
process.endjob_step = cms.EndPath() #cms.EndPath(process.endOfProcess)
process.RECOSIMoutput_step = cms.EndPath() #cms.EndPath(process.RECOSIMoutput)
process.DQMoutput_step = cms.EndPath() #cms.EndPath(process.DQMoutput)

# Schedule definition
process.schedule = cms.Schedule(process.raw2digi_step,process.L1Reco_step,process.reconstruction_step,process.prevalidation_step,process.validation_step,process.dqmoffline_step,process.endjob_step,process.RECOSIMoutput_step,process.DQMoutput_step)
