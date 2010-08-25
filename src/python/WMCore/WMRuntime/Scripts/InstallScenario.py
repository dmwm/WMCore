#!/usr/bin/env python
"""
_InstallScenario_

Runtime script that installs the scenario based configuration PSet into the job

"""

import logging
import sys
import traceback

import FWCore.ParameterSet.Config as cms

from WMCore.WMRuntime.ScriptInterface import ScriptInterface

from PSetTweaks.WMTweak import makeTweak, applyTweak
from PSetTweaks.WMTweak import makeOutputTweak, makeJobTweak, makeTaskTweak

applyPromptReco = lambda s, a: s.promptReco(a['globalTag'], a['writeTiers'])
applyAlcaSkim = lambda s, a: s.alcaSkim(a['skims'])
applySkimming = lambda s, a: s.skimming(a['skims'])

class InstallScenario(ScriptInterface):
    funcMap = {
        "promptReco": applyPromptReco,
        "alcaSkim": applyAlcaSkim,
        "skimming": applySkimming
        }

    def fixupProcess(self, process):
        """
        _fixupProcess_

        Look over the process object and make sure that all of the attributes
        that we expect to exist actually exist.
        """
        # Make sure that for each output module the following parameters exist
        # in the PSet returned from the framework:
        #   fileName
        #   logicalFileName
        #   dataset.dataTier
        #   dataset.filterName
        for outMod in process.outputModules_():
            outModRef = getattr(process, outMod)
            if not hasattr(outModRef, "dataset"):
                outModRef.dataset = cms.untracked.PSet()
            if not hasattr(outModRef.dataset, "dataTier"):
                outModRef.dataset.dataTier = cms.untracked.string("")
            if not hasattr(outModRef.dataset, "filterName"):
                outModRef.dataset.filterName = cms.untracked.string("")
            if not hasattr(outModRef, "fileName"):
                outModRef.fileName = cms.untracked.string("")
            if not hasattr(outModRef, "logicalFileName"):
                outModRef.logicalFileName = cms.untracked.string("")

        # Make sure that the process object has the following attributes:
        #    GlobtalTag.globaltag
        #    source.firstEvent
        #    source.firstRun
        #    source.firstLuminosityBlock
        #    source.skipEvents
        #    maxEvents.input
        #    source.fileNames
        #    source.secondaryFileNames
        #    The AdaptorConfig service
        if not hasattr(process, "GlobalTag"):
            process.GlobalTag = cms.PSet(globalTag = cms.string(""))
        if not hasattr(process.GlobalTag, "globaltag"):
            process.GlobalTag.globaltag = cms.string("")
        if not hasattr(process.source, "firstEvent"):
            process.source.firstEvent = cms.untracked.uint32(0)
        if not hasattr(process.source, "firstRun"):
            process.source.firstRun = cms.untracked.uint32(0)
        if not hasattr(process.source, "firstLuminosityBlock"):
            process.source.firstLuminosityBlock = cms.untracked.uint32(0)            
        if not hasattr(process.source, "skipEvents"):
            process.source.skipEvents = cms.untracked.uint32(0)
        if not hasattr(process, "maxEvents"):
            process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32(-1))
        if not hasattr(process.maxEvents, "input"):
            process.maxEvents.input = cms.untracked.int32(-1)
        if not hasattr(process.source, "fileNames"):
            process.source.fileNames = cms.untracked.vstring()
        if not hasattr(process.source, "secondaryFileNames"):
            process.source.secondaryFileNames = cms.untracked.vstring()
        if not process.services.has_key("AdaptorConfig"):
            process.add_(cms.Service("AdaptorConfig"))

        return process

    def createProcess(self, scenario, funcName, funcArgs):
        """
        __createProcess__

        Load a process from the framework.
        """
        if funcName == "merge":
            try:
                from Configuration.DataProcessing.Merge import mergeProcess
                process = mergeProcess([])
            except Exception, ex:
                msg = "Filaed to create a merge process."
                print msg
                return None
        else:
            try:
                from Configuration.DataProcessing.GetScenario import getScenario            
                scenarioInst = getScenario(scenario)
                applicationFunc = self.funcMap[funcName]
                process = applicationFunc(scenarioInst,  funcArgs.dictionary_())            
            except Exception, ex:
                msg = "Failed to retrieve the Scenario named "
                msg += str(scenario)
                msg += "\nWith Error:"
                msg += str(ex)
                print msg
                return None

        return process

    def __call__(self):
        configSect = self.step.data.application.configuration
        scenario = getattr(configSect, "scenario", None)
        funcName = getattr(configSect, "function", None)
        funcArgs = getattr(configSect, "arguments", None)

        if scenario == None:
            msg = "No %s.application.configuration.scenario Provided" % (
                self.step.name(),)
            print msg
            return 50201

        if funcName == None:
            msg = "No %s.application.configuration.function Provided" % (
                self.step.name(),)
            print msg
            return 50201

        process = self.createProcess(scenario, funcName, funcArgs)
        if process == None:
            return 50202
        
        process = self.fixupProcess(process)
           
        # Apply task level tweaks
        taskTweak = makeTaskTweak(self.step.data)
        applyTweak(process, taskTweak)

        # Apply per job PSet Tweaks
        jobTweak = makeJobTweak(self.job)
        applyTweak(process, jobTweak)

        # Apply per output module PSet Tweaks
        cmsswStep = self.step.getTypeHelper()
        for om in cmsswStep.listOutputModules():
            mod = cmsswStep.getOutputModule(om)
            outTweak = makeOutputTweak(mod, self.job)
            applyTweak(process, outTweak)

        # revlimiter for testing
        if hasattr(process, "maxEvents"):
            if hasattr(process.maxEvents, "input"):
                process.maxEvents.input = 2

        process.services["AdaptorConfig"].cacheHint = cms.untracked.string("lazy-download")
        process.services["AdaptorConfig"].readHint = cms.untracked.string("auto-detect")
        process.source.cacheSize = cms.untracked.uint32(100000000)

        configFile = self.step.data.application.command.configuration
        workingDir = self.stepSpace.location
        handle = open("%s/%s" % (workingDir, configFile), 'w')
        handle.write(process.dumpPython())
        handle.close()
                     
        return 0
