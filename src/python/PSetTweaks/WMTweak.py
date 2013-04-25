#!/usr/bin/env python
"""
_WMTweak_

Define extraction of a standard set of WM related PSet parameters
Note: This can be used within the CMSSW environment to act on a
process/config but does not depend on any CMSSW libraries. It needs to stay like this.

"""
import logging
import pickle

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
    "process.GlobalTag.globaltag",

    ]

class WMTweakMaskError(StandardError):

    def __init__(self, mask = None, msg = "Cannot set process from job mask"):
        self.mask = mask
        self.message = msg

    def __str__(self):
        return "Error: %s \n Mask: %s" % (self.message, str(self.mask))

def lfnGroup(job):
    """
    _lfnGroup_

    Determine the lfnGroup from the job counter and the agent number
    provided in the job baggage, the job counter and agent number
    default both to 0. The result will be a 5-digit string.
    """
    modifier = str(job.get("agentNumber", 0))
    lfnGroup = modifier + str(job.get("counter", 0) / 1000).zfill(4)
    return lfnGroup

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
            print msg
            return

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



def applyTweak(process, tweak, fixup = None):
    """
    _applyTweak_

    Add the changes contained in the tweak to the process to give a job specific
    process.  The fixup parameters is a dictionary keyed by parameter name.  If
    the tweak contains a parameter in the dictionary the value in the dict will
    be calls and passed the process.

    This is useful for preparing the process before the value is applied (ie-
    making sure all the necessary PSets and configuration values exist).
    """
    for param, value in tweak:
        if fixup and fixup.has_key(param):
            fixup[param](process)

        setParameter(process, param, value)


childParameters = lambda p, x: [ i for i in  x._internal_settings if i not in x._internal_children]
childSections = lambda s : [ getattr(s, x) for x in s._internal_children ]


class ConfigSectionDecomposer:
    """
    _ConfigSectionDecomposer_

    Util to collapse a ConfigSection to a dict of . delimited param: values
    where the params contain the section structure.

    May turn out to be generally useful for ConfigSections

    """
    def __init__(self):
        self.configSects = []
        self.parameters = {}
        self.queue = []


    def __call__(self, configSect):
        """
        _operator(configSect)_

        recursively traverse all parameters in this and all child
        PSets

        """
        self.queue.append(configSect._internal_name)
        csectPath = ".".join(self.queue)
        self.configSects.append(csectPath)
        params = childParameters(csectPath, configSect)
        for par in params:
            paramName = ".".join([csectPath, par])
            paramVal = getattr(configSect, par)
            self.parameters[paramName] = paramVal


        map(self, childSections(configSect))
        self.queue.pop(-1)


def decomposeConfigSection(csect):
    """
    _decomposeConfigSection_

    Util to convert a config section into a . delimited dict of
    parameters mapped to values

    """
    decomposer = ConfigSectionDecomposer()
    decomposer(csect)

    return decomposer.parameters

def makeTaskTweak(stepSection):
    """
    _makeTaskTweak_

    Create a tweak for options in the task that apply to all jobs.
    """
    result = PSetTweak()

    # GlobalTag
    if hasattr(stepSection, "application"):
        if hasattr(stepSection.application, "configuration"):
            if hasattr(stepSection.application.configuration, "pickledarguments"):
                args = pickle.loads(stepSection.application.configuration.pickledarguments)
                if args.has_key('globalTag'):
                    result.addParameter("process.GlobalTag.globaltag", args['globalTag'])
                if args.has_key('globalTagTransaction'):
                    result.addParameter("process.GlobalTag.DBParameters.transactionId", args['globalTagTransaction'])

    return result

def makeJobTweak(job):
    """
    _makeJobTweak_

    Convert information from a WMBS Job object into a PSetTweak
    that can be used to modify a CMSSW process.
    """
    result = PSetTweak()
    baggage = job.getBaggage()

    # Check in the baggage if we are processing .lhe files
    lheInput = getattr(baggage, "lheInputFiles", False)

    # Input files and secondary input files.
    primaryFiles = []
    secondaryFiles = []
    for inputFile in job["input_files"]:
        if inputFile["lfn"].startswith("MCFakeFile"):
            # If there is a preset lumi in the mask, use it as the first
            # luminosity setting
            if job['mask'].get('FirstLumi', None) != None:
                result.addParameter("process.source.firstLuminosityBlock",
                                    job['mask']['FirstLumi'])
            else:
                #We don't have lumi information in the mask, raise an exception
                raise WMTweakMaskError(job['mask'],
                                       "No first lumi information provided")
            continue

        primaryFiles.append(inputFile["lfn"])
        for secondaryFile in inputFile["parents"]:
            secondaryFiles.append(secondaryFile["lfn"])

    if len(primaryFiles) > 0:
        result.addParameter("process.source.fileNames", primaryFiles)
        if len(secondaryFiles) > 0:
            result.addParameter("process.source.secondaryFileNames", secondaryFiles)
    elif not lheInput:
        #First event parameter should be set from whatever the mask says,
        #That should have the added protection of not going over 2^32 - 1
        #If there is nothing in the mask, then we fallback to the counter method
        if job['mask'].get('FirstEvent',None) != None:
            result.addParameter("process.source.firstEvent",
                                job['mask']['FirstEvent'])
        else:
            #No first event information in the mask, raise and error
            raise WMTweakMaskError(job['mask'],
                                   "No first event information provided in the mask")

    mask =  job['mask']

    # event limits
    maxEvents = mask.getMaxEvents()
    if maxEvents == None: maxEvents = -1
    result.addParameter("process.maxEvents.input", maxEvents)

    # We don't want to set skip events for MonteCarlo jobs which have
    # no input files.
    firstEvent = mask['FirstEvent']
    if firstEvent != None and firstEvent >= 0 and (len(primaryFiles) > 0 or lheInput):
        if lheInput:
            result.addParameter("process.source.skipEvents", firstEvent - 1)
        else:
            result.addParameter("process.source.skipEvents", firstEvent)

    firstRun = mask['FirstRun']
    if firstRun != None:
        result.addParameter("process.source.firstRun", firstRun)
    elif not len(primaryFiles):
        #Then we have a MC job, we need to set firstRun to 1
        logging.debug("MCFakeFile initiated without job FirstRun - using one.")
        result.addParameter("process.source.firstRun", 1)

    runs = mask.getRunAndLumis()
    lumisToProcess = []
    for run in runs.keys():
        lumiPairs = runs[run]
        for lumiPair in lumiPairs:
            if len(lumiPair) != 2:
                # Do nothing
                continue
            lumisToProcess.append("%s:%s-%s:%s" % (run, lumiPair[0], run, lumiPair[1]))

    if len(lumisToProcess) > 0:
        result.addParameter("process.source.lumisToProcess", lumisToProcess)

    # install any settings from the per job baggage
    procSection = getattr(baggage, "process", None)
    if procSection == None:
        return result

    baggageParams = decomposeConfigSection(procSection)
    for k,v in baggageParams.items():
        result.addParameter(k,v)


    return result



def makeOutputTweak(outMod, job):
    """
    _makeOutputTweak_

    Make a PSetTweak for the output module and job instance provided

    """
    result = PSetTweak()
    # output filenames
    modName = str(getattr(outMod, "_internal_name"))
    fileName = "%s.root" % modName

    result.addParameter("process.%s.fileName" % modName, fileName)

    lfnBase = str(getattr(outMod, "lfnBase", None))
    if lfnBase != None:
        lfn = "%s/%s/%s.root" % (lfnBase, lfnGroup(job), modName)
        result.addParameter("process.%s.logicalFileName" % modName, lfn)


    #TODO: Nice standard way to meddle with the other parameters in the
    #      output module based on the settings in the section

    return result
