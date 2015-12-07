# Auto generated configuration file
# using:
# Revision: 1.392
# Source: /local/reps/CMSSW/CMSSW/Configuration/PyReleaseValidation/python/ConfigBuilder.py,v
# with command line options: step2 --datatier GEN-SIM-DIGI-RAW-HLTDEBUG --conditions auto:startup -s DIGI,L1,DIGI2RAW,HLT,RAW2DIGI,L1Reco --eventcontent FEVTDEBUGHLT --io DIGI.io --python DIGI.py -n 100 --no_exec --filein filelist:step1_dbsquery.log --fileout file:step2.root
# modified to reduce outside dependencies - this probably doesn't do anything useful anymore
# hopefully its still useful as a test

import FWCore.ParameterSet.Config as cms

process = cms.Process('HLT')

# import of standard configurations
#process.load('Configuration.StandardSequences.Services_cff')
#process.load('SimGeneral.HepPDTESSource.pythiapdt_cfi')
#process.load('FWCore.MessageService.MessageLogger_cfi')
#process.load('Configuration.EventContent.EventContent_cff')
#process.load('SimGeneral.MixingModule.mixNoPU_cfi')
#process.load('Configuration.StandardSequences.GeometryRecoDB_cff')
#process.load('Configuration.StandardSequences.MagneticField_38T_cff')
#process.load('Configuration.StandardSequences.Digi_cff')
#process.load('Configuration.StandardSequences.SimL1Emulator_cff')
#process.load('Configuration.StandardSequences.DigiToRaw_cff')
#process.load('HLTrigger.Configuration.HLT_GRun_cff')
#process.load('Configuration.StandardSequences.RawToDigi_cff')
#process.load('Configuration.StandardSequences.L1Reco_cff')
#process.load('Configuration.StandardSequences.EndOfProcess_cff')
#process.load('Configuration.StandardSequences.FrontierConditions_GlobalTag_cff')

process.maxEvents = cms.untracked.PSet(
    input = cms.untracked.int32(100)
)

# Input source
process.source = cms.Source("PoolSource",
    secondaryFileNames = cms.untracked.vstring(),
    fileNames = cms.untracked.vstring('/store/relval/CMSSW_6_0_0_pre3/RelValSingleElectronPt10/GEN-SIM/START60_V0-v1/0124/B64692A8-9C8E-E111-80E4-0018F3D0967E.root')
)

process.options = cms.untracked.PSet(

)

# Production Info
process.configurationMetadata = cms.untracked.PSet(
    version = cms.untracked.string('$Revision: 1.392 $'),
    annotation = cms.untracked.string('step2 nevts:100'),
    name = cms.untracked.string('PyReleaseValidation')
)

# Output definition

process.FEVTDEBUGHLToutput = cms.OutputModule("PoolOutputModule",
    splitLevel = cms.untracked.int32(0),
    eventAutoFlushCompressedSize = cms.untracked.int32(5242880),
#    outputCommands = process.FEVTDEBUGHLTEventContent.outputCommands,
    fileName = cms.untracked.string('file:step2.root'),
    dataset = cms.untracked.PSet(
        filterName = cms.untracked.string(''),
        dataTier = cms.untracked.string('GEN-SIM-DIGI-RAW-HLTDEBUG')
    )
)

# Additional output definition

# Other statements
#process.GlobalTag.globaltag = 'START60_V4::All'

# Path and EndPath definitions
process.digitisation_step = cms.Path() #cms.Path(process.pdigi)
process.L1simulation_step = cms.Path() #cms.Path(process.SimL1Emulator)
process.digi2raw_step = cms.Path() #cms.Path(process.DigiToRaw)
process.raw2digi_step = cms.Path() #cms.Path(process.RawToDigi)
process.L1Reco_step = cms.Path() #cms.Path(process.L1Reco)
process.endjob_step = cms.Path() #cms.EndPath(process.endOfProcess)
process.FEVTDEBUGHLToutput_step = cms.EndPath() #cms.EndPath(process.FEVTDEBUGHLToutput)

# Schedule definition
process.schedule = cms.Schedule(process.digitisation_step,process.L1simulation_step,process.digi2raw_step)
#process.schedule.extend(process.HLTSchedule)
process.schedule.extend([process.raw2digi_step,process.L1Reco_step,process.endjob_step,process.FEVTDEBUGHLToutput_step])

# customisation of the process.

# Automatic addition of the customisation function from HLTrigger.Configuration.customizeHLTforMC
#from HLTrigger.Configuration.customizeHLTforMC import customizeHLTforMC

#call to customisation function customizeHLTforMC imported from HLTrigger.Configuration.customizeHLTforMC
#process = customizeHLTforMC(process)

# End of customisation functions
