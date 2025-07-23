#!/usr/bin/env python
"""
_WMTweak_

Define extraction of a standard set of WM related PSet parameters
Note: This can be used within the CMSSW environment to act on a
process/config but does not depend on any CMSSW libraries. It needs to stay like this.

"""
from __future__ import print_function, division

from builtins import map, range, str, object
from future.utils import viewitems, viewkeys

import logging
import os
import pickle
from Utils.PythonVersion import PY3
from Utils.Utilities import encodeUnicodeToBytesConditional
from PSetTweaks.PSetTweak import PSetTweak

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
    # "outputCommands", #this is just a huge pile of stuff which we probably shouldnt be setting anyways
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

    # config metadata
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


class WMTweakMaskError(Exception):
    def __init__(self, mask=None, msg="Cannot set process from job mask"):
        super(WMTweakMaskError, self).__init__()
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
    jobLfnGroup = modifier + str(job.get("counter", 0) // 1000).zfill(4)
    return jobLfnGroup


def hasParameter(pset, param, nopop=False):
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
        params.pop(0)  # first param is the pset we have the reference to
    lastParam = pset
    for param in params:
        lastParam = getattr(lastParam, param, None)
        if lastParam is None:
            return False
    if lastParam is not None:
        return True
    return False


def getParameter(pset, param, nopop=False):
    """
    _getParameter_

    Retrieve the specified parameter from the PSet Provided
    given the attribute chain

    returns None if not found

    """
    params = param.split(".")
    if not nopop:
        params.pop(0)  # first param is the pset we have the reference to
    lastParam = pset
    for param in params:
        lastParam = getattr(lastParam, param, None)
        if lastParam is None:
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
    params.pop(0)  # first is process object
    lastPSet = process
    for pset in params:
        lastPSet = getattr(lastPSet, pset, None)
        if lastPSet is None:
            msg = "Cannot find attribute named: %s\n" % pset
            msg += "Cannot set value: %s" % param
            logging.error(msg)
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
    lastResults = {"process": process}
    finalResults = {}
    for _ in range(0, len(params)):
        pset = params.pop(0)
        if pset == "*":
            newResults = {}
            for lastResultKey, lastResultVal in viewitems(lastResults):
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
            for lastResultKey, lastResultVal in viewitems(lastResults):
                newResultKey = "%s.%s" % (lastResultKey, pset)
                newResultVal = getattr(lastResultVal, pset, None)
                if not hasattr(newResultVal, "parameters_"):
                    finalResults[newResultKey] = newResultVal
                    continue
                newResults[newResultKey] = newResultVal

            lastResults = newResults

    return finalResults


listParams = lambda x: [y for y in x.parameters_()]


class TweakMaker(object):
    """
    _TweakMaker_

    Object to generate a Tweak instance from a generic
    configuration by searching for a set of specific parameters
    within the process, all output modules and a set of parameters
    within the output modules

    """

    def __init__(self, processParams=None, outmodParams=None):
        processParams = processParams or _TweakParams
        outmodParams = outmodParams or _TweakOutputModules
        self.processLevel = processParams
        self.outModLevel = outmodParams

    def __call__(self, process):
        tweak = PSetTweak()
        # handle process parameters
        processParams = []
        for param in self.processLevel:
            processParams.extend(viewkeys(expandParameter(process, param)))

        for param in processParams:
            if hasParameter(process, param):
                tweak.addParameter(param, getParameter(process, param))

        # output modules
        tweak.addParameter('process.outputModules_', [])
        for outMod in process.outputModules_():
            tweak.getParameter('process.outputModules_').append(outMod)
            outModRef = getattr(process, outMod)
            for param in self.outModLevel:
                fullParam = "process.%s.%s" % (outMod, param)
                if hasParameter(outModRef, param, True):
                    tweak.addParameter(fullParam, getParameter(outModRef, param, True))

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


def applyTweak(process, tweak, fixup=None):
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
        if isinstance(value, type(u'')) and hasattr(value, "encode"):
            logging.info("Found unicode parameter type for param: %s, with value: %s", param, value)
            value = value.encode("utf-8")
        if fixup and param in fixup:
            fixup[param](process)

        setParameter(process, param, value)


childParameters = lambda p, x: [i for i in x._internal_settings if i not in x._internal_children]
childSections = lambda s: [getattr(s, x) for x in s._internal_children]


class ConfigSectionDecomposer(object):
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

        list(map(self, childSections(configSect)))
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


def makeTaskTweak(stepSection, result):
    """
    _makeTaskTweak_

    Create a tweak for options in the task that apply to all jobs.
    """

    # GlobalTag
    if hasattr(stepSection, "application"):
        if hasattr(stepSection.application, "configuration"):
            if hasattr(stepSection.application.configuration, "pickledarguments"):
                pklArgs = encodeUnicodeToBytesConditional(stepSection.application.configuration.pickledarguments,
                                                          condition=PY3)
                args = pickle.loads(pklArgs)
                if 'globalTag' in args:
                    result.addParameter("process.GlobalTag.globaltag",  "customTypeCms.string('%s')" % args['globalTag'])

    return


def makeJobTweak(job, result):
    """
    _makeJobTweak_

    Convert information from a WMBS Job object into a PSetTweak
    that can be used to modify a CMSSW process.
    """
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
                logging.info("Setting 'firstLuminosityBlock' attr to: %s", job['mask']['FirstLumi'])
                result.addParameter("process.source.firstLuminosityBlock",
                                    "customTypeCms.untracked.uint32(%s)" % job['mask']['FirstLumi'])
            else:
                # We don't have lumi information in the mask, raise an exception
                raise WMTweakMaskError(job['mask'],
                                       "No first lumi information provided")
            continue

        primaryFiles.append(inputFile["lfn"])
        for secondaryFile in inputFile["parents"]:
            secondaryFiles.append(secondaryFile["lfn"])

    logging.info("Adding %d files to 'fileNames' attr", len(primaryFiles))
    logging.info("Adding %d files to 'secondaryFileNames' attr", len(secondaryFiles))
    if len(primaryFiles) > 0:
        result.addParameter("process.source.fileNames", "customTypeCms.untracked.vstring(%s)" % primaryFiles)
        if len(secondaryFiles) > 0:
            result.addParameter("process.source.secondaryFileNames", "customTypeCms.untracked.vstring(%s)" % secondaryFiles)
    elif not lheInput:
        # First event parameter should be set from whatever the mask says,
        # That should have the added protection of not going over 2^32 - 1
        # If there is nothing in the mask, then we fallback to the counter method
        if job['mask'].get('FirstEvent', None) != None:
            logging.info("Setting 'firstEvent' attr to: %s", job['mask']['FirstEvent'])
            result.addParameter("process.source.firstEvent", "customTypeCms.untracked.uint32(%s)" % job['mask']['FirstEvent'])
        else:
            # No first event information in the mask, raise and error
            raise WMTweakMaskError(job['mask'],
                                   "No first event information provided in the mask")

    mask = job['mask']

    # event limits
    maxEvents = mask.getMaxEvents()
    if maxEvents is None:
        maxEvents = -1
    logging.info("Setting 'maxEvents.input' attr to: %s", maxEvents)
    result.addParameter("process.maxEvents", "customTypeCms.untracked.PSet(input=cms.untracked.int32(%s))"% maxEvents)

    # We don't want to set skip events for MonteCarlo jobs which have
    # no input files.
    firstEvent = mask['FirstEvent']
    if firstEvent != None and firstEvent >= 0 and (len(primaryFiles) > 0 or lheInput):
        if lheInput:
            logging.info("Setting 'skipEvents' attr to: %s", firstEvent - 1)
            result.addParameter("process.source.skipEvents", "customTypeCms.untracked.uint32(%s)" % (firstEvent - 1))
        else:
            logging.info("Setting 'skipEvents' attr to: %s", firstEvent)
            result.addParameter("process.source.skipEvents", "customTypeCms.untracked.uint32(%s)" % firstEvent)

    firstRun = mask['FirstRun']
    if firstRun != None:
        result.addParameter("process.source.firstRun", "customTypeCms.untracked.uint32(%s)" % firstRun)
    elif not len(primaryFiles):
        # Then we have a MC job, we need to set firstRun to 1
        logging.debug("MCFakeFile initiated without job FirstRun - using one.")
        result.addParameter("process.source.firstRun", "customTypeCms.untracked.uint32(1)")

    runs = mask.getRunAndLumis()
    lumisToProcess = []
    for run in viewkeys(runs):
        lumiPairs = runs[run]
        for lumiPair in lumiPairs:
            if len(lumiPair) != 2:
                # Do nothing
                continue
            lumisToProcess.append("%s:%s-%s:%s" % (run, lumiPair[0], run, lumiPair[1]))

    if len(lumisToProcess) > 0:
        logging.info("Adding %d run/lumis mask to 'lumisToProcess' attr", len(lumisToProcess))
        result.addParameter("process.source.lumisToProcess", "customTypeCms.untracked.VLuminosityBlockRange(%s)" % lumisToProcess)

    # install any settings from the per job baggage
    procSection = getattr(baggage, "process", None)
    if procSection is None:
        return result

    baggageParams = decomposeConfigSection(procSection)
    for k, v in viewitems(baggageParams):
        if isinstance(v, str):
            v =  "customTypeCms.untracked.string(%s)" % v
        elif isinstance(v, int):
            v =  "customTypeCms.untracked.uint32(%s)" % v
        elif isinstance(v, list):
            v =  "customTypeCms.untracked.vstring(%s)" % v
        result.addParameter(k, v)

    return


def makeOutputTweak(outMod, job, result):
    """
    _makeOutputTweak_

    Make a PSetTweak for the output module and job instance provided

    """
    # output filenames
    modName = outMod.getInternalName()
    logging.info("modName = %s", modName)
    fileName = "%s.root" % modName

    result.addParameter("process.%s.fileName" % modName, fileName)

    lfnBase = str(getattr(outMod, "lfnBase", None))
    if lfnBase != None:
        lfn = "%s/%s/%s.root" % (lfnBase, lfnGroup(job), modName)
        result.addParameter("process.%s.logicalFileName" % modName, lfn)

    return


def readAdValues(attrs, adname, castInt=False):
    """
    A very simple parser for the ads available at runtime.  Returns
    a dictionary containing
    - attrs: A list of string keys to look for.
    - adname: Which ad to parse; "job" for the $_CONDOR_JOB_AD or
      "machine" for $_CONDOR_MACHINE_AD
    - castInt: Set to True to force the values to be integer literals.
      Otherwise, this will return the values as a string representation
      of the ClassAd expression.

    Note this is not a ClassAd parser - will not handle new-style ads
    or any expressions.

    Will return a dictionary containing the key/value pairs that were
    present in the ad and parseable.

    On error, returns an empty dictionary.
    """
    retval = {}
    adfile = None
    if adname == 'job':
        adfile = os.environ.get("_CONDOR_JOB_AD")
    elif adname == 'machine':
        adfile = os.environ.get("_CONDOR_MACHINE_AD")
    else:
        logging.warning("Invalid ad name requested for parsing: %s", adname)
        return retval
    if not adfile:
        logging.warning("%s adfile is not set in environment.", adname)
        return retval
    attrs = [i.lower() for i in attrs]

    try:
        with open(adfile) as fd:
            for line in fd:
                info = line.strip().split("=", 1)
                if len(info) != 2:
                    continue
                attr = info[0].strip().lower()
                if attr in attrs:
                    val = info[1].strip()
                    if castInt:
                        try:
                            retval[attr] = int(val)
                        except ValueError:
                            logging.warning("Error parsing %s's %s value: %s", adname, attr, val)
                    else:
                        retval[attr] = val
    except IOError:
        logging.exception("Error opening %s ad:", adname)
        return {}

    return retval


def resizeResources(resources):
    """
    _resizeResources_

    Look at the job runtime environment and determine whether we are allowed
    to resize the core count.  If so, change the resources dictionary passed
    to this function according to the information found in $_CONDOR_MACHINE_AD.
    The following keys are changed:
     - cores -> uses value of Cpus from the machine ad.
     - memory -> Memory

    This only works when running under HTCondor, $_CONDOR_MACHINE_AD exists,
    and WMCore_ResizeJob is true.
          - WMCore_ResizeJob is 'true'

    No return value - the resources directory is changed in-place.
    Should not throw an exception - on error, no change is made and a message
    is printed out.
    """
    if readAdValues(['wmcore_resizejob'], 'job').get('wmcore_resizejob', 'false').lower() != "true":
        logging.info("Not resizing job")
        return

    logging.info("Resizing job.  Initial resources: %s", resources)
    adValues = readAdValues(['memory', 'cpus'], 'machine', castInt=True)
    machineCpus = adValues.get('cpus', 0)
    machineMemory = adValues.get('memory', 0)
    if machineCpus > 0 and 'cores' in resources:
        resources['cores'] = machineCpus
    if machineMemory > 0 and 'memory' in resources:
        resources['memory'] = machineMemory
    logging.info("Resizing job.  Resulting resources: %s", resources)
