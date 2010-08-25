#!/usr/bin/env python
"""
_SetupCMSSWPset_

"""

from WMCore.WMRuntime.ScriptInterface import ScriptInterface

from PSetTweaks.PSetTweak import PSetTweak
from PSetTweaks.WMTweak import makeTweak, applyTweak
from PSetTweaks.WMTweak import makeOutputTweak, makeJobTweak, makeTaskTweak

import FWCore.ParameterSet.Config as cms

applyPromptReco = lambda s, a: s.promptReco(a['globalTag'], a['writeTiers'])
applyAlcaSkim = lambda s, a: s.alcaSkim(a['skims'])
applySkimming = lambda s, a: s.skimming(a['skims'])

class SetupCMSSWPset(ScriptInterface):
    """
    _SetupCMSSWPset_

    """
    funcMap = {
        "promptReco": applyPromptReco,
        "alcaSkim": applyAlcaSkim,
        "skimming": applySkimming
        }
    
    def createProcess(self, scenario, funcName, funcArgs):
        """
        _createProcess_

        """
        if funcName == "merge":
            try:
                from Configuration.DataProcessing.Merge import mergeProcess
                self.process = mergeProcess([])
            except Exception, ex:
                msg = "Filaed to create a merge process."
                print msg
                return None
        else:
            try:
                from Configuration.DataProcessing.GetScenario import getScenario
                scenarioInst = getScenario(scenario)
                applicationFunc = self.funcMap[funcName]
                self.process = applicationFunc(scenarioInst,  funcArgs.dictionary_())
            except Exception, ex:
                msg = "Failed to retrieve the Scenario named "
                msg += str(scenario)
                msg += "\nWith Error:"
                msg += str(ex)
                print msg
                return None
            
        return

    def loadPSet(self):
        """
        _loadPSet_

        """
        psetModule = "WMTaskSpace.%s.PSet" % self.step.data._internal_name

        try:
            processMod = __import__(psetModule, globals(), locals(), ["process"], -1)
            self.process = processMod.process
        except ImportError, ex:
            msg = "Unable to import process from %s:\n" % psetModule
            msg += str(ex)
            print msg
            return 1

        return

    def fixupProcess(self):
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
        print dir(self.process)
        if hasattr(self.process, "outputModules"):
            outputModuleNames = self.process.outputModules.keys()
        else:
            outputModulesNames = self.process.outputModules_()            
        for outMod in outputModuleNames:
            outModRef = getattr(self.process, outMod)
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
        if not hasattr(self.process, "GlobalTag"):
            self.process.GlobalTag = cms.PSet(globalTag = cms.string(""))
        if not hasattr(self.process.GlobalTag, "globaltag"):
            self.process.GlobalTag.globaltag = cms.string("")
        if not hasattr(self.process.source, "firstEvent"):
            self.process.source.firstEvent = cms.untracked.uint32(0)
        if not hasattr(self.process.source, "firstRun"):
            self.process.source.firstRun = cms.untracked.uint32(0)
        if not hasattr(self.process.source, "firstLuminosityBlock"):
            self.process.source.firstLuminosityBlock = cms.untracked.uint32(0)            
        if not hasattr(self.process.source, "skipEvents"):
            self.process.source.skipEvents = cms.untracked.uint32(0)
        if not hasattr(self.process, "maxEvents"):
            self.process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32(-1))
        if not hasattr(self.process.maxEvents, "input"):
            self.process.maxEvents.input = cms.untracked.int32(-1)
        if not hasattr(self.process.source, "fileNames"):
            self.process.source.fileNames = cms.untracked.vstring()
        if not hasattr(self.process.source, "secondaryFileNames"):
            self.process.source.secondaryFileNames = cms.untracked.vstring()
        if not self.process.services.has_key("AdaptorConfig"):
            self.process.add_(cms.Service("AdaptorConfig"))

        return

    def applyTweak(self, setTweak):
        """
        _applyTweak_

        """
        tweak = PSetTweak()
        tweak.unpersist(psetTweak)
        applyTweak(self.process, tweak)
        return

    def __call__(self):
        """
        _call_

        """
        step = self.step.data
        self.process = None

        scenario = getattr(step.application.configuration, "scenario", None)
        if scenario != None:
            funcName = getattr(step.application.configuration, "function", None)
            funcArgs = getattr(step.application.configuration, "arguments", None)
            self.createProcess(scenario, funcName, funcArgs)
        else:
            self.loadPSet()

        self.fixupProcess()

        psetTweak = getattr(step.application.command, "psetTweak", None)
        if psetTweak != None:
            self.applyPSetTweak(psetTweak)

        # Apply task level tweaks
        taskTweak = makeTaskTweak(self.step.data)
        applyTweak(self.process, taskTweak)

        # Apply per job PSet Tweaks
        jobTweak = makeJobTweak(self.job)
        applyTweak(self.process, jobTweak)

        # Apply per output module PSet Tweaks
        cmsswStep = self.step.getTypeHelper()
        for om in cmsswStep.listOutputModules():
            mod = cmsswStep.getOutputModule(om)
            outTweak = makeOutputTweak(mod, self.job)
            applyTweak(self.process, outTweak)
            
        # revlimiter for testing
        self.process.maxEvents.input = 2

        self.process.services["AdaptorConfig"].cacheHint = cms.untracked.string("lazy-download")
        self.process.services["AdaptorConfig"].readHint = cms.untracked.string("auto-detect")
        self.process.source.cacheSize = cms.untracked.uint32(100000000)

        configFile = self.step.data.application.command.configuration
        workingDir = self.stepSpace.location
        handle = open("%s/%s" % (workingDir, configFile), 'w')
        handle.write(self.process.dumpPython())
        handle.close()
        
        return 0
