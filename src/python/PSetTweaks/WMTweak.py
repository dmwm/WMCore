#!/usr/bin/env python
"""
_WMTweak_

Define extraction of a standard set of WM related PSet parameters
Note: This can be used within the CMSSW environment to act on a
process/config but does not depend on any CMSSW libraries. It needs to stay like this.

"""

from PSetTweaks.PSetTweak import PSetTweak
from PSetTweaks.PSetTweak import parameterIterator, psetIterator

# params to be extracted from an output module
_TweakOutputModules = [
    "fileName",
    "logicalFileName",
    "compressionLevel",
    "basketSize",
    "splitLevel",
    "overrideInputFileSplitLevels",
    "maxSize",
    "fastCloning",
    "sortBaskets",
    "dropMetaData",
    #"outputCommands", #this is just a huge pile of stuff which we probably shouldnt be setting anyways
    "SelectEvents.SelectEvents",
    "dataset.dataTier",
    "dataset.filterName",
    # TODO: support dataset.* here


    ]

_TweakParams = [
    # options
    "process.options.fileMode",
    "process.options.wantSummary",
    "process.options.allowUnscheduled",
    "process.options.makeTriggerResults",
    "process.options.Rethrow",
    "process.options.SkipEvent",
    "process.options.FailPath",
    "process.options.FailModule",
    "process.options.IgnoreCompletely",

    #config metadata
    "process.configurationMetadata.name",
    "process.configurationMetadata.version",
    "process.configurationMetadata.annotation",



    # source
    "process.source.maxEvents",
    "process.source.skipEvents",
    "process.source.firstEvent",
    "process.source.firstRun",
    "process.source.firstLuminosityBlock",
    "process.source.numberEventsInRun",
    "process.source.fileNames",
    "process.source.secondaryFileNames",
    "process.source.fileMatchMode",
    "process.source.overrideCatalog",
    "process.source.numberEventsInLuminosityBlock",
    "process.source.firstTime",
    "process.source.timeBetweenEvents",
    "process.source.eventCreationDelay",
    "process.source.needSecondaryFileNames",
    "process.source.parametersMustMatch",
    "process.source.branchesMustMatch",
    "process.source.setRunNumber",
    "process.source.skipBadFiles",
    "process.source.eventsToSkip",
    "process.source.lumisToSkip",
    "process.source.eventsToProcess",
    "process.source.lumisToProcess",
    "process.source.noEventSort",
    "process.source.duplicateCheckMode",
    "process.source.inputCommands",
    "process.source.dropDescendantsOfDroppedBranches",


    # maxevents
    "process.maxEvents.input",
    "process.maxEvents.output",
    # TODO: there are more settings stored as a VPSet which are a complete
    #       ballache to handle, suggest asking framework to change interface here

    # job report service
    # Everything has shifted to the default cff

    # message logger
    # Everything is in the default cff

    # random seeds
    "process.RandomNumberGeneratorService.*.initialSeed",

    ]


def hasParameter(pset, param, nopop = False):
    """
    _hasParameter_

    check that pset provided has the attribute chain
    specified.

    Eg if param is pset.attr1.attr2.attr3
    check for pset.attr1.attr2.attr3
    returns True if parameter exists, False if not

    """
    params = param.split(".")
    if not nopop:
        params.pop(0) # first param is the pset we have the reference to
    lastParam = pset
    for param in params:
        lastParam = getattr(lastParam, param, None)
        if lastParam == None:
            return False
    if lastParam != None:
        return True
    return False

def getParameter(pset, param, nopop = False):
    """
    _getParameter_

    Retrieve the specified parameter from the PSet Provided
    given the attribute chain

    returns None if not found

    """
    params = param.split(".")
    if not nopop:
        params.pop(0) # first param is the pset we have the reference to
    lastParam = pset
    for param in params:
        lastParam = getattr(lastParam, param, None)
        if lastParam == None:
            return None
    return lastParam.value()

def setParameter(process, param, value):
    """
    _setParameter_

    Set the value of the parameter to the given value.

    - process is the reference to the process

    - param is the name of the param as process.pset1.pset2...parameter

    - value is the value to set that paramter to

    """
    params = param.split('.')
    params.pop(0) # first is process object
    lastPSet = process
    for pset in params:
        lastPSet = getattr(lastPSet, pset, None)
        if lastPSet == None:
            msg = "Cannot find attribute named: %s\n" % pset
            msg += "Cannot set value: %s" % param
            raise RuntimeError, msg

    lastPSet.setValue(value)
    return




def expandParameter(process, param):
    """
    _expandParameter_

    If param contains a wildcard * then expand it to the list of
    matching parameters

    """
    params = param.split('.')
    params.pop(0)
    lastResults = {"process" : process}
    finalResults = {}
    for i  in range(0, len(params)):
        pset = params.pop(0)
        if pset == "*":
            newResults = {}
            for lastResultKey, lastResultVal in lastResults.items():
                for param in listParams(lastResultVal):
                    newResultKey = "%s.%s" % (lastResultKey, param)
                    newResultVal = getattr(lastResultVal, param)
                    if not hasattr(newResultVal, "parameters_"):
                        if len(params) == 0:
                            finalResults[newResultKey] = newResultVal
                        continue
                    newResults[newResultKey] = newResultVal
            lastResults = newResults


        else:
            newResults = {}
            for lastResultKey, lastResultVal in lastResults.items():
                newResultKey = "%s.%s" % (lastResultKey, pset)
                newResultVal = getattr(lastResultVal, pset, None)
                if not hasattr(newResultVal, "parameters_"):
                    finalResults[newResultKey] = newResultVal
                    continue
                newResults[newResultKey] = newResultVal

            lastResults = newResults



    return finalResults

listParams = lambda x: [ y for y in x.parameters_()  ]

class TweakMaker:
    """
    _TweakMaker_

    Object to generate a Tweak instance from a generic
    configuration by searching for a set of specific parameters
    within the process, all output modules and a set of parameters
    within the output modules

    """
    def __init__(self, processParams = _TweakParams,
                 outmodParams = _TweakOutputModules):

        self.processLevel = processParams
        self.outModLevel = outmodParams

    def __call__(self, process):
        tweak = PSetTweak()
        # handle process parameters
        processParams = []
        [ processParams.extend( expandParameter(process, param).keys())
          for param in self.processLevel]


        [ tweak.addParameter(param, getParameter(process, param))
          for param in processParams if hasParameter(process, param) ]

        # output modules
        tweak.addParameter('process.outputModules_', [])
        for outMod in process.outputModules_():
            tweak.getParameter('process.outputModules_').append(outMod)
            outModRef = getattr(process, outMod)
            for param in self.outModLevel:
                fullParam = "process.%s.%s" % (outMod, param)
                if hasParameter(outModRef, param, True):
                    tweak.addParameter(
                        fullParam,
                        getParameter(outModRef,
                                     param,
                                     True))


        return tweak

def makeTweak(process):
    """
    _makeTweak_

    Create a PSetTweak instance using the list of potential parameters
    defined above. If the process has those parameters, they get added
    to the tweak, if not, they are left out.

    """
    maker = TweakMaker()
    return maker(process)



def applyTweak(process, tweak):
    """
    _applyTweak_

    Add the changes contained in the tweak to the process to give
    a job specific process

    """
    for param, value in tweak:
        setParameter(process, param, value)









