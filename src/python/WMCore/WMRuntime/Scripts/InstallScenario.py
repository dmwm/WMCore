#!/usr/bin/env python
"""
_InstallScenario_

Runtime script that installs the scenario based configuration PSet into the job

"""


from WMCore.WMRuntime.ScriptInterface import ScriptInterface



applyPromptReco = lambda s, a: s.promptReco(a['globalTag'], a['writeTiers'])

#def applyPromptReco(scenarioInst, **args):
#    """
#    _applyPromptReco_
#
#    Apply the promptReco call with the arguments provided
#
#    """
#    return scenarioInst.promptReco(args['globalTag'], args['writeTiers'])

def applySkimming(scenarioInst, **args):
    """
    _applySkimming_

    Apply the skimming call with the arguments provided

    """
    print "skimming(%s)" % args


class InstallScenario(ScriptInterface):


    def __call__(self):

        try:
            from Configuration.DataProcessing.GetScenario import getScenario
        except ImportError, ex:
            msg = "Failed to load Configuration.DataProcessing Modules"
            print msg
            return 50200
        

        
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
                
        try:
            scenarioInst = getScenario(scenario)
        except Exception, ex:
            msg = "Failed to retrieve the Scenario named "
            msg += str(scenario)
            msg += "\nWith Error:"
            msg += str(ex)
            print msg
            return 50202

        funcMap = {
            "promptReco": applyPromptReco,
            "skimming"  : applySkimming,
            }
        
        print "InstallScenario for %s: %s.%s" % (self.step.name(), scenario, funcName)

        applicationFunc = funcMap[funcName]
        try:
            process = applicationFunc(scenarioInst,  funcArgs.dictionary_())
        except Exception, ex:
            msg = "Failed to invoke %s.%s with args %s\n" % (scenario, funcName, funcArgs.dictionary_())
            msg += str(ex)
            print msg
            return 50203

        configFile = self.step.data.application.command.configuration
        workingDir = self.stepSpace.location
        handle = open("%s/%s" % (workingDir, configFile), 'w')
        handle.write(process.dumpPython())
        handle.close()
                     

        return 0
