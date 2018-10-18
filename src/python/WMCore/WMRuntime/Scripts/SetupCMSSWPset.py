"""
_SetupCMSSWPset_

Create a CMSSW PSet suitable for running a WMAgent job.

"""
from __future__ import print_function

import json
import logging
import os
import pickle
import random
import socket

import FWCore.ParameterSet.Config as cms

from PSetTweaks.PSetTweak import PSetTweak
from PSetTweaks.WMTweak import applyTweak, makeJobTweak, makeOutputTweak, makeTaskTweak, resizeResources
from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig
from WMCore.Storage.TrivialFileCatalog import TrivialFileCatalog
from WMCore.WMRuntime.ScriptInterface import ScriptInterface


def fixupGlobalTag(process):
    """
    _fixupGlobalTag_

    Make sure that the process has a GlobalTag.globaltag string.

    Requires that the configuration already has a properly configured GlobalTag object.

    """
    if hasattr(process, "GlobalTag"):
        if not hasattr(process.GlobalTag, "globaltag"):
            process.GlobalTag.globaltag = cms.string("")
    return


def fixupGlobalTagTransaction(process):
    """
    _fixupGlobalTagTransaction_

    Make sure that the process has a GlobalTag.DBParameters.transactionId string.

    Requires that the configuration already has a properly configured GlobalTag object

    (used to customize conditions access for Tier0 express processing)

    """
    if hasattr(process, "GlobalTag"):
        if not hasattr(process.GlobalTag.DBParameters, "transactionId"):
            process.GlobalTag.DBParameters.transactionId = cms.untracked.string("")
    return


def fixupFirstRun(process):
    """
    _fixupFirstRun_

    Make sure that the process has a firstRun parameter.

    """
    if not hasattr(process.source, "firstRun"):
        process.source.firstRun = cms.untracked.uint32(0)
    return


def fixupLastRun(process):
    """
    _fixupLastRun_

    Make sure that the process has a lastRun parameter.

    """
    if not hasattr(process.source, "lastRun"):
        process.source.lastRun = cms.untracked.uint32(0)
    return


def fixupLumisToProcess(process):
    """
    _fixupLumisToProcess_

    Make sure that the process has a lumisToProcess parameter.

    """
    if not hasattr(process.source, "lumisToProcess"):
        process.source.lumisToProcess = cms.untracked.VLuminosityBlockRange()
    return


def fixupSkipEvents(process):
    """
    _fixupSkipEvents_

    Make sure that the process has a skip events parameter.

    """
    if not hasattr(process.source, "skipEvents"):
        process.source.skipEvents = cms.untracked.uint32(0)
    return


def fixupFirstEvent(process):
    """
    _fixupFirstEvent_

    Make sure that the process has a first event parameter.

    """
    if not hasattr(process.source, "firstEvent"):
        process.source.firstEvent = cms.untracked.uint32(0)
    return


def fixupMaxEvents(process):
    """
    _fixupMaxEvents_

    Make sure that the process has a max events parameter.

    """
    if not hasattr(process, "maxEvents"):
        process.maxEvents = cms.untracked.PSet(input=cms.untracked.int32(-1))
    if not hasattr(process.maxEvents, "input"):
        process.maxEvents.input = cms.untracked.int32(-1)
    return


def fixupFileNames(process):
    """
    _fixupFileNames_

    Make sure that the process has a fileNames parameter.

    """
    if not hasattr(process.source, "fileNames"):
        process.source.fileNames = cms.untracked.vstring()
    return


def fixupSecondaryFileNames(process):
    """
    _fixupSecondaryFileNames_

    Make sure that the process has a secondaryFileNames parameter.

    """
    if not hasattr(process.source, "secondaryFileNames"):
        process.source.secondaryFileNames = cms.untracked.vstring()
    return


def fixupFirstLumi(process):
    """
    _fixupFirstLumi

    Make sure that the process has firstLuminosityBlock parameter.
    """
    if not hasattr(process.source, "firstLuminosityBlock"):
        process.source.firstLuminosityBlock = cms.untracked.uint32(1)
    return


def isCMSSWSupported(thisCMSSW, supportedCMSSW):
    """
    _isCMSSWSupported_

    Function used to validate whether the CMSSW release to be used supports
    a feature not available in all releases.
    :param thisCMSSW: release to be used in this job
    :param allowedCMSSW: first (lower) release that started supporting this
    feature. Only the first 2 digits are supported.
    """
    if not thisCMSSW or not supportedCMSSW:
        logging.info("You must provide the CMSSW version being used by this job and a supported version")
        return False

    thisCMSSW = thisCMSSW.split('_', 3)
    supportedCMSSW = supportedCMSSW.split('_', 3)
    if thisCMSSW[1] > supportedCMSSW[1]:
        return True
    if thisCMSSW[1] == supportedCMSSW[1]:
        if thisCMSSW[2] >= supportedCMSSW[2]:
            return True

    return False


class SetupCMSSWPset(ScriptInterface):
    """
    _SetupCMSSWPset_

    """
    fixupDict = {"process.GlobalTag.globaltag": fixupGlobalTag,
                 "process.GlobalTag.DBParameters.transactionId": fixupGlobalTagTransaction,
                 "process.source.fileNames": fixupFileNames,
                 "process.source.secondaryFileNames": fixupSecondaryFileNames,
                 "process.maxEvents.input": fixupMaxEvents,
                 "process.source.skipEvents": fixupSkipEvents,
                 "process.source.firstEvent": fixupFirstEvent,
                 "process.source.firstRun": fixupFirstRun,
                 "process.source.lastRun": fixupLastRun,
                 "process.source.lumisToProcess": fixupLumisToProcess,
                 "process.source.firstLuminosityBlock": fixupFirstLumi}

    def __init__(self, crabPSet=False):
        ScriptInterface.__init__(self)
        self.crabPSet = crabPSet
        self.process = None

    def createProcess(self, scenario, funcName, funcArgs):
        """
        _createProcess_

        Create a Configuration.DataProcessing PSet.

        """
        if funcName == "merge":

            baggage = self.job.getBaggage()
            if getattr(baggage, "useErrorDataset", False):
                funcArgs['outputmod_label'] = "MergedError"

            try:
                from Configuration.DataProcessing.Merge import mergeProcess
                self.process = mergeProcess(**funcArgs)
            except Exception as ex:
                msg = "Failed to create a merge process."
                logging.exception(msg)
                raise ex
        elif funcName == "repack":
            try:
                from Configuration.DataProcessing.Repack import repackProcess
                self.process = repackProcess(**funcArgs)
            except Exception as ex:
                msg = "Failed to create a repack process."
                logging.exception(msg)
                raise ex
        else:
            try:
                from Configuration.DataProcessing.GetScenario import getScenario
                scenarioInst = getScenario(scenario)
            except Exception as ex:
                msg = "Failed to retrieve the Scenario named "
                msg += str(scenario)
                msg += "\nWith Error:"
                msg += str(ex)
                logging.error(msg)
                raise ex
            try:
                self.process = getattr(scenarioInst, funcName)(**funcArgs)
            except Exception as ex:
                msg = "Failed to load process from Scenario %s (%s)." % (scenario, scenarioInst)
                logging.error(msg)
                raise ex

        return

    def loadPSet(self):
        """
        _loadPSet_

        Load a PSet that was shipped with the job sandbox.

        """
        psetModule = "WMTaskSpace.%s.PSet" % self.step.data._internal_name

        try:
            processMod = __import__(psetModule, globals(), locals(), ["process"], -1)
            self.process = processMod.process
        except ImportError as ex:
            msg = "Unable to import process from %s:\n" % psetModule
            msg += str(ex)
            logging.error(msg)
            raise ex

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
        if hasattr(self.process, "outputModules"):
            outputModuleNames = self.process.outputModules.keys()
        else:
            outputModuleNames = self.process.outputModules_()
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
        return

    def applyTweak(self, psetTweak):
        """
        _applyTweak_

        Apply a tweak to the process.
        """
        tweak = PSetTweak()
        tweak.unpersist(psetTweak)
        applyTweak(self.process, tweak, self.fixupDict)
        return

    def handleSeeding(self):
        """
        _handleSeeding_

        Handle Random Seed settings for the job
        """
        baggage = self.job.getBaggage()
        seeding = getattr(baggage, "seeding", None)
        if seeding == "ReproducibleSeeding":
            randService = self.process.RandomNumberGeneratorService
            tweak = PSetTweak()
            for x in randService:
                parameter = "process.RandomNumberGeneratorService.%s.initialSeed" % x._internal_name
                tweak.addParameter(parameter, x.initialSeed)
            applyTweak(self.process, tweak, self.fixupDict)
        else:
            if hasattr(self.process, "RandomNumberGeneratorService"):
                from IOMC.RandomEngine.RandomServiceHelper import RandomNumberServiceHelper
                helper = RandomNumberServiceHelper(self.process.RandomNumberGeneratorService)
                helper.populate()
        return

    def handlePerformanceSettings(self):
        """
        _handlePerformanceSettings_

        Install the standard performance report services
        """
        # include the default performance report services
        if getattr(self.step.data.application.command, 'silentMemoryCheck', False):
            self.process.add_(cms.Service("SimpleMemoryCheck", jobReportOutputOnly=cms.untracked.bool(True)))
        else:
            self.process.add_(cms.Service("SimpleMemoryCheck"))

        self.process.add_(cms.Service("CPU"))
        self.process.add_(cms.Service("Timing"))
        self.process.Timing.summaryOnly = cms.untracked(cms.bool(True))

        return

    def handleChainedProcessing(self):
        """
        _handleChainedProcessing_

        In order to handle chained processing it's necessary to feed
        output of one step/task (nomenclature ambiguous) to another.
        This method creates particular mapping in a working Trivial
        File Catalog (TFC).
        """
        # first, create an instance of TrivialFileCatalog to override
        tfc = TrivialFileCatalog()
        # check the jobs input files
        inputFile = ("../%s/%s.root" % (self.step.data.input.inputStepName,
                                        self.step.data.input.inputOutputModule))
        tfc.addMapping("direct", inputFile, inputFile, mapping_type="lfn-to-pfn")
        tfc.addMapping("direct", inputFile, inputFile, mapping_type="pfn-to-lfn")

        fixupFileNames(self.process)
        fixupMaxEvents(self.process)
        self.process.source.fileNames.setValue([inputFile])
        self.process.maxEvents.input.setValue(-1)

        tfcName = "override_catalog.xml"
        tfcPath = os.path.join(os.getcwd(), tfcName)
        logging.info("Creating override TFC, contents below, saving into '%s'", tfcPath)
        tfcStr = tfc.getXML()
        logging.info(tfcStr)
        with open(tfcPath, 'w') as tfcFile:
            tfcFile.write(tfcStr)

        self.step.data.application.overrideCatalog = "trivialcatalog_file:" + tfcPath + "?protocol=direct"

        return

    def handlePileup(self):
        """
        _handlePileup_

        Handle pileup settings.
        """
        # find out local site SE name
        siteConfig = loadSiteLocalConfig()
        PhEDExNodeName = siteConfig.localStageOut["phedex-node"]
        logging.info("Running on site '%s', local PNN: '%s'", siteConfig.siteName, PhEDExNodeName)

        pileupDict = self._getPileupConfigFromJson()

        # 2011-02-03 according to the most recent version of instructions, we do
        # want to differentiate between "MixingModule" and "DataMixingModule"
        mixModules, dataMixModules = self._getPileupMixingModules()

        # 2011-02-03
        # on the contrary to the initial instructions (wave), there are
        # going to be only two types of pileup input datasets: "data" or "mc"
        # unlike all previous places where pileupType handled in a flexible
        # way as specified in the configuration passed by the user, here are
        # the two pileupTypes hardcoded: and we are going to add the "mc"
        # datasets to "MixingModule"s and only add the "data" datasets to the
        # "DataMixingModule"s

        # if the user in the configuration specifies different pileup types
        # than "data" or "mc", the following call will not modify anything
        self._processPileupMixingModules(pileupDict, PhEDExNodeName, dataMixModules, "data")
        self._processPileupMixingModules(pileupDict, PhEDExNodeName, mixModules, "mc")

        return

    def _processPileupMixingModules(self, pileupDict, PhEDExNodeName, modules, requestedPileupType):
        """
        Iterates over all modules and over all pileup configuration types.
        The only considered types are "data" and "mc" (input to this method).
        If other pileup types are specified by the user, the method doesn't
        modify anything.

        The method considers only files which are present on this local PNN.
        The job will use only those, unless it was told to trust the PU site
        location (trustPUSitelists=True), in this case ALL the blocks/files
        will be added to the PSet and files will be read via AAA.
        Dataset, divided into blocks, may not have all blocks present on a
        particular PNN. However, all files belonging into a block will be
        present when reported by DBS.

        The structure of the pileupDict: PileupFetcher._queryDbsAndGetPileupConfig

        2011-02-03:
        According to the current implementation of helper testing module
        WMCore_t/WMRuntime_t/Scripts_t/WMTaskSpace/cmsRun1/PSet.py
        each type of modules instances can have either "secsource"
        or "input" attribute, so need to probe both, one shall succeed.
        """
        baggage = self.job.getBaggage()

        for m in modules:
            for pileupType in self.step.data.pileup.listSections_():
                # there should be either "input" or "secsource" attributes
                # and both "MixingModule", "DataMixingModule" can have both
                inputTypeAttrib = getattr(m, "input", None) or getattr(m, "secsource", None)
                if not inputTypeAttrib:
                    continue
                inputTypeAttrib.fileNames = cms.untracked.vstring()
                if pileupType == requestedPileupType:
                    eventsAvailable = 0
                    useAAA = True if getattr(baggage, 'trustPUSitelists', False) else False
                    for blockName in sorted(pileupDict[pileupType].keys()):
                        blockDict = pileupDict[pileupType][blockName]
                        if PhEDExNodeName in blockDict["PhEDExNodeNames"] or useAAA:
                            eventsAvailable += int(blockDict.get('NumberOfEvents', 0))
                            for fileLFN in blockDict["FileList"]:
                                # vstring does not support unicode
                                inputTypeAttrib.fileNames.append(str(fileLFN['logical_file_name']))
                    if requestedPileupType == 'data':
                        if getattr(baggage, 'skipPileupEvents', None) is not None:
                            # For deterministic pileup, we want to shuffle the list the
                            # same for every job in the task and skip events
                            random.seed(self.job['task'])
                            logging.info("Skipping %d pileup events for deterministic data mixing",
                                         baggage.skipPileupEvents)
                            inputTypeAttrib.skipEvents = cms.untracked.uint32(
                                int(baggage.skipPileupEvents) % eventsAvailable)
                            inputTypeAttrib.sequential = cms.untracked.bool(True)
                    # Shuffle according to the seed above or randomly
                    random.shuffle(inputTypeAttrib.fileNames)
        return

    def _getPileupMixingModules(self):
        """
        Method returns two lists:
            1) list of mixing modules ("MixingModule")
            2) list of data mixing modules ("DataMixingModules")
        The first gets added only pileup files of type "mc", the
        second pileup files of type "data".

        """
        mixModules, dataMixModules = [], []
        prodsAndFilters = {}
        prodsAndFilters.update(self.process.producers)
        prodsAndFilters.update(self.process.filters)
        for key, value in prodsAndFilters.items():
            if value.type_() in ["MixingModule", "DataMixingModule"]:
                mixModules.append(value)
            if value.type_() == "DataMixingModule":
                dataMixModules.append(value)
        return mixModules, dataMixModules

    def _getPileupConfigFromJson(self):
        """
        There has been stored pileup configuration stored in a JSON file
        as a result of DBS querrying when running PileupFetcher,
        this method loads this configuration from sandbox and returns it
        as dictionary.

        The PileupFetcher was called by WorkQueue which creates job's sandbox
        and sandbox gets migrated to the worker node.

        """
        workingDir = self.stepSpace.location
        jsonPileupConfig = os.path.join(workingDir, "pileupconf.json")
        logging.info("Pileup JSON configuration file: '%s'", jsonPileupConfig)
        try:
            with open(jsonPileupConfig) as jdata:
                pileupDict = json.load(jdata)
        except IOError:
            m = "Could not read pileup JSON configuration file: '%s'" % jsonPileupConfig
            raise RuntimeError(m)
        return pileupDict

    def handleProducersNumberOfEvents(self):
        """
        _handleProducersNumberOfEvents_

        Some producer modules are initialized with a maximum number of events
        to be generated, usually based on the process.maxEvents.input attribute
        but after that is tweaked the producers number of events need to
        be fixed as well. This method takes care of that.
        """
        producers = {}
        producers.update(self.process.producers)
        for producer in producers:
            if hasattr(producers[producer], "nEvents"):
                producers[producer].nEvents = self.process.maxEvents.input.value()

    def handleDQMFileSaver(self):
        """
        _handleDQMFileSaver_

        Harvesting jobs have the dqmFileSaver EDAnalyzer that must
        be tweaked with the dataset name in order to store it
        properly in the DQMGUI, others tweaks can be added as well
        """
        if not hasattr(self.process, "dqmSaver"):
            return

        baggage = self.job.getBaggage()
        runIsComplete = getattr(baggage, "runIsComplete", False)
        multiRun = getattr(baggage, "multiRun", False)
        runLimits = getattr(baggage, "runLimits", "")

        self.process.dqmSaver.runIsComplete = cms.untracked.bool(runIsComplete)
        if multiRun and isCMSSWSupported(self.getCmsswVersion(), "CMSSW_8_0_X"):
            self.process.dqmSaver.forceRunNumber = cms.untracked.int32(999999)
        if hasattr(self.step.data.application.configuration, "pickledarguments"):
            args = pickle.loads(self.step.data.application.configuration.pickledarguments)
            datasetName = args.get('datasetName', None)
            if datasetName:
                if multiRun:
                    # then change the dataset name in order to get a different root file name
                    datasetName = datasetName.rsplit('/', 1)
                    datasetName[0] += runLimits
                    datasetName = "/".join(datasetName)
                self.process.dqmSaver.workflow = cms.untracked.string(datasetName)
        return

    def handleRepackSettings(self):
        """
        _handleRepackSettings_

        Repacking small events is super inefficient reading directly from EOS.
        """
        logging.info("Hardcoding read/cache strategies for repack")
        self.process.add_(
            cms.Service("SiteLocalConfigService",
                        overrideSourceCacheHintDir=cms.untracked.string("lazy-download")
                        )
        )

        return

    def handleSingleCoreOverride(self):
        """
        _handleSingleCoreOverride_

        Make sure job only uses one core and one stream in CMSSW
        """
        try:
            if int(self.step.data.application.multicore.numberOfCores) > 1:
                self.step.data.application.multicore.numberOfCores = 1
        except AttributeError:
            pass

        try:
            if int(self.step.data.application.multicore.eventStreams) > 0:
                self.step.data.application.multicore.eventStreams = 0
        except AttributeError:
            pass

        return

    def handleSpecialCERNMergeSettings(self, funcName):
        """
        _handleSpecialCERNMergeSettings_

        CERN has a 30ms latency between Meyrin and Wigner, which kills merge performance
        Enable lazy-download for fastCloning for all CMSSW_7_5 jobs (currently off)
        Enable lazy-download for all merge jobs
        """
        if self.getCmsswVersion().startswith("CMSSW_7_5") and False:
            logging.info("Using fastCloning/lazydownload")
            self.process.add_(cms.Service("SiteLocalConfigService",
                                          overrideSourceCloneCacheHintDir=cms.untracked.string("lazy-download")))
        elif funcName == "merge":
            logging.info("Using lazydownload")
            self.process.add_(cms.Service("SiteLocalConfigService",
                                          overrideSourceCacheHintDir=cms.untracked.string("lazy-download")))
        return

    def handleCondorStatusService(self):
        """
        _handleCondorStatusService_

        Enable CondorStatusService for CMSSW releases that support it.
        """
        if isCMSSWSupported(self.getCmsswVersion(), "CMSSW_7_6_X"):
            logging.info("Tag chirp updates from CMSSW with step %s", self.step.data._internal_name)
            self.process.add_(cms.Service("CondorStatusService",
                                          tag=cms.untracked.string("_%s_" % self.step.data._internal_name)))

        return

    def getCmsswVersion(self):
        """
        _getCmsswVersion_

        Return a string representing the CMSSW version to be used.
        """
        if not self.crabPSet:
            return self.step.data.application.setup.cmsswVersion
        else:
            # CRAB3 needs to use an environment var to get the version
            return os.environ.get("CMSSW_VERSION", "")

    def __call__(self):
        """
        _call_

        Examine the step configuration and construct a PSet from that.

        """
        self.process = None

        scenario = getattr(self.step.data.application.configuration, "scenario", None)
        if scenario is not None and scenario != "":
            funcName = getattr(self.step.data.application.configuration, "function", None)
            if getattr(self.step.data.application.configuration, "pickledarguments", None) is not None:
                funcArgs = pickle.loads(self.step.data.application.configuration.pickledarguments)
            else:
                funcArgs = {}
            try:
                self.createProcess(scenario, funcName, funcArgs)
            except Exception as ex:
                logging.exception("Error creating process for Config/DataProcessing:")
                raise ex

            if funcName == "repack":
                self.handleRepackSettings()

            if funcName in ["merge", "alcaHarvesting"]:
                self.handleSingleCoreOverride()

            if socket.getfqdn().endswith("cern.ch"):
                self.handleSpecialCERNMergeSettings(funcName)

        else:
            try:
                self.loadPSet()
            except Exception as ex:
                logging.exception("Error loading PSet:")
                raise ex

        # Check process.source exists
        if getattr(self.process, "source", None) is None:
            msg = "Error in CMSSW PSet: process is missing attribute 'source'"
            msg += " or process.source is defined with None value."
            logging.error(msg)
            raise RuntimeError(msg)

        self.handleCondorStatusService()

        self.fixupProcess()

        # In case of CRAB3, the number of threads in the PSet should not be overridden
        if not self.crabPSet:
            try:
                origCores = int(getattr(self.step.data.application.multicore, 'numberOfCores', 1))
                eventStreams = int(getattr(self.step.data.application.multicore, 'eventStreams', 0))
                resources = {'cores': origCores}
                resizeResources(resources)
                numCores = resources['cores']
                if numCores != origCores:
                    logging.info(
                        "Resizing a job with nStreams != nCores. Setting nStreams = nCores. This may end badly.")
                    eventStreams = 0
                options = getattr(self.process, "options", None)
                if options is None:
                    self.process.options = cms.untracked.PSet()
                    options = getattr(self.process, "options")
                options.numberOfThreads = cms.untracked.uint32(numCores)
                options.numberOfStreams = cms.untracked.uint32(eventStreams)
            except AttributeError as ex:
                logging.error("Failed to override numberOfThreads: %s", str(ex))

        psetTweak = getattr(self.step.data.application.command, "psetTweak", None)
        if psetTweak is not None:
            self.applyPSetTweak(psetTweak, self.fixupDict)

        # Apply task level tweaks
        taskTweak = makeTaskTweak(self.step.data)
        applyTweak(self.process, taskTweak, self.fixupDict)

        # Check if chained processing is enabled
        # If not - apply the per job tweaks
        # If so - create an override TFC (like done in PA) and then modify thePSet accordingly
        if hasattr(self.step.data.input, "chainedProcessing") and self.step.data.input.chainedProcessing:
            self.handleChainedProcessing()
        else:
            # Apply per job PSet Tweaks
            jobTweak = makeJobTweak(self.job)
            applyTweak(self.process, jobTweak, self.fixupDict)

        # check for pileup settings presence, pileup support implementation
        # and if enabled, process pileup configuration / settings
        if hasattr(self.step.data, "pileup"):
            self.handlePileup()

        # Apply per output module PSet Tweaks
        cmsswStep = self.step.getTypeHelper()
        for om in cmsswStep.listOutputModules():
            mod = cmsswStep.getOutputModule(om)
            outTweak = makeOutputTweak(mod, self.job)
            applyTweak(self.process, outTweak, self.fixupDict)

        # revlimiter for testing
        if getattr(self.step.data.application.command, "oneEventMode", False):
            self.process.maxEvents.input = 1

        # check for random seeds and the method of seeding which is in the job baggage
        self.handleSeeding()

        # make sure default parametersets for perf reports are installed
        self.handlePerformanceSettings()

        # check for event numbers in the producers
        self.handleProducersNumberOfEvents()

        # fixup the dqmFileSaver
        self.handleDQMFileSaver()

        # Check if we accept skipping bad files
        if hasattr(self.step.data.application.configuration, "skipBadFiles"):
            self.process.source.skipBadFiles = \
                cms.untracked.bool(self.step.data.application.configuration.skipBadFiles)

        # Apply events per lumi section if available
        if hasattr(self.step.data.application.configuration, "eventsPerLumi"):
            self.process.source.numberEventsInLuminosityBlock = \
                cms.untracked.uint32(self.step.data.application.configuration.eventsPerLumi)

        # limit run time if desired
        if hasattr(self.step.data.application.configuration, "maxSecondsUntilRampdown"):
            self.process.maxSecondsUntilRampdown = cms.untracked.PSet(
                input=cms.untracked.int32(self.step.data.application.configuration.maxSecondsUntilRampdown))

        # accept an overridden TFC from the step
        if hasattr(self.step.data.application, 'overrideCatalog'):
            logging.info("Found a TFC override: %s", self.step.data.application.overrideCatalog)
            self.process.source.overrideCatalog = \
                cms.untracked.string(self.step.data.application.overrideCatalog)

        configFile = self.step.data.application.command.configuration
        configPickle = getattr(self.step.data.application.command, "configurationPickle", "PSet.pkl")
        workingDir = self.stepSpace.location
        try:
            with open("%s/%s" % (workingDir, configPickle), 'wb') as pHandle:
                pickle.dump(self.process, pHandle)

            with open("%s/%s" % (workingDir, configFile), 'w') as handle:
                handle.write("import FWCore.ParameterSet.Config as cms\n")
                handle.write("import pickle\n")
                handle.write("with open('%s', 'rb') as handle:\n" % configPickle)
                handle.write("    process = pickle.load(handle)\n")
        except Exception as ex:
            logging.exception("Error writing out PSet:")
            raise ex

        return 0
