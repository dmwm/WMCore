#!/usr/bin/env python
"""
_InstallScenario_

Runtime script that installs the scenario based configuration PSet into the job

"""

import logging
import sys
import traceback

from WMCore.WMRuntime.ScriptInterface import ScriptInterface


from PSetTweaks.WMTweak import makeTweak, makeJobTweak
from PSetTweaks.WMTweak import makeOutputTweak, applyTweak

applyPromptReco = lambda s, a: s.promptReco(a['globalTag'], a['writeTiers'])
applyAlcaSkim = lambda s, a: s.alcaSkim(a['skims'])
applySkimming = lambda s, a: s.skimming(a['skims'])


class InstallScenario(ScriptInterface):


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

        funcMap = {
            "promptReco": applyPromptReco,
            "alcaSkim": applyAlcaSkim,
            "skimming": applySkimming
            }

        if funcName == "merge":
            try:
                from Configuration.DataProcessing.Merge import mergeProcess
                process = mergeProcess([])
            except Exception, ex:
                msg = "Filaed to create a merge process."
                print msg
                return 50202
        else:
            try:
                from Configuration.DataProcessing.GetScenario import getScenario            
                scenarioInst = getScenario(scenario)
                applicationFunc = funcMap[funcName]
                process = applicationFunc(scenarioInst,  funcArgs.dictionary_())            
            except Exception, ex:
                msg = "Failed to retrieve the Scenario named "
                msg += str(scenario)
                msg += "\nWith Error:"
                msg += str(ex)
                print msg
                return 50202

        # apply task PSet Tweaks
        # TODO: Implement this


        # apply per job PSet Tweaks
        jobTweak = makeJobTweak(self.job)
        applyTweak(process, jobTweak)
        
        # output modules fixup for missing parameters
        import FWCore.ParameterSet.Config as cms
        for outMod in process.outputModules_():
            outModRef = getattr(process, outMod)
            if not hasattr(outModRef, "logicalFileName"):
                outModRef.logicalFileName = cms.untracked.string('')
        
        cmsswStep = self.step.getTypeHelper()
        for om in cmsswStep.listOutputModules():
            mod = cmsswStep.getOutputModule(om)
            outTweak = makeOutputTweak(mod, self.job)
            applyTweak(process, outTweak)

        if funcName == "alcaSkim":
            process.GlobalTag.globaltag = ""

        # revlimiter for testing
        if hasattr(process, "maxEvents"):
            if hasattr(process.maxEvents, "input"):
                process.maxEvents.input = 2

        if not process.services.has_key("AdaptorConfig"):
            process.add_(cms.Service("AdaptorConfig"))

        process.services["AdaptorConfig"].cacheHint = cms.untracked.string("lazy-download")
        process.services["AdaptorConfig"].readHint = cms.untracked.string("auto-detect")
        process.source.cacheSize = cms.untracked.uint32(100000000)

        configFile = self.step.data.application.command.configuration
        workingDir = self.stepSpace.location
        handle = open("%s/%s" % (workingDir, configFile), 'w')
        handle.write(process.dumpPython())
        handle.close()
                     

        return 0
