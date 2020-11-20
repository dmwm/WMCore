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
import re

import FWCore.ParameterSet.Config as cms

from PSetTweaks.PSetTweak import PSetTweak
from PSetTweaks.WMTweak import applyTweak, makeJobTweak, makeOutputTweak, makeTaskTweak, resizeResources
from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig
from WMCore.Storage.TrivialFileCatalog import TrivialFileCatalog
from WMCore.WMRuntime.ScriptInterface import ScriptInterface
from WMCore.WMRuntime.Tools.Scram import isCMSSWSupported, isEnforceGUIDInFileNameSupported


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
        self.jobBag = None
        self.logger = logging.getLogger()

    def createProcess(self, scenario, funcName, funcArgs):
        """
        _createProcess_

        Create a Configuration.DataProcessing PSet.

        """
        if funcName == "merge":

            if getattr(self.jobBag, "useErrorDataset", False):
                funcArgs['outputmod_label'] = "MergedError"

            try:
                from Configuration.DataProcessing.Merge import mergeProcess
                self.process = mergeProcess(**funcArgs)
            except Exception as ex:
                msg = "Failed to create a merge process."
                self.logger.exception(msg)
                raise ex
        elif funcName == "repack":
            try:
                from Configuration.DataProcessing.Repack import repackProcess
                self.process = repackProcess(**funcArgs)
            except Exception as ex:
                msg = "Failed to create a repack process."
                self.logger.exception(msg)
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
                self.logger.error(msg)
                raise ex
            try:
                self.process = getattr(scenarioInst, funcName)(**funcArgs)
            except Exception as ex:
                msg = "Failed to load process from Scenario %s (%s)." % (scenario, scenarioInst)
                self.logger.error(msg)
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
            self.logger.error(msg)
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
        seeding = getattr(self.jobBag, "seeding", None)
        self.logger.info("Job seeding set to: %s", seeding)
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
        self.logger.info("Handling chained processing job")
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
        self.logger.info("Creating override TFC and saving into '%s'", tfcPath)
        tfcStr = tfc.getXML()
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
        self.logger.info("Running on site '%s', local PNN: '%s'", siteConfig.siteName, PhEDExNodeName)

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

    def _processPileupMixingModules(self, pileupDict, PhEDExNodeName,
                                    modules, requestedPileupType):
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
        self.logger.info("Requested pileup type %s with %d mixing modules", requestedPileupType, len(modules))

        for m in modules:
            self.logger.info("Loaded module type: %s", m.type_())
            for pileupType in self.step.data.pileup.listSections_():
                # there should be either "input" or "secsource" attributes
                # and both "MixingModule", "DataMixingModule" can have both
                inputTypeAttrib = getattr(m, "input", None) or getattr(m, "secsource", None)
                self.logger.info("pileupType: %s with input attributes: %s", pileupType, bool(inputTypeAttrib))
                if not inputTypeAttrib:
                    continue
                inputTypeAttrib.fileNames = cms.untracked.vstring()
                if pileupType == requestedPileupType:
                    eventsAvailable = 0
                    useAAA = True if getattr(self.jobBag, 'trustPUSitelists', False) else False
                    self.logger.info("Pileup set to read data remotely: %s", useAAA)
                    for blockName in sorted(pileupDict[pileupType].keys()):
                        blockDict = pileupDict[pileupType][blockName]
                        if PhEDExNodeName in blockDict["PhEDExNodeNames"] or useAAA:
                            eventsAvailable += int(blockDict.get('NumberOfEvents', 0))
                            for fileLFN in blockDict["FileList"]:
                                # vstring does not support unicode
                                inputTypeAttrib.fileNames.append(str(fileLFN))
                    if requestedPileupType == 'data':
                        if getattr(self.jobBag, 'skipPileupEvents', None) is not None:
                            # For deterministic pileup, we want to shuffle the list the
                            # same for every job in the task and skip events
                            random.seed(self.job['task'])
                            self.logger.info("Skipping %d pileup events for deterministic data mixing",
                                             self.jobBag.skipPileupEvents)
                            inputTypeAttrib.skipEvents = cms.untracked.uint32(
                                int(self.jobBag.skipPileupEvents) % eventsAvailable)
                            inputTypeAttrib.sequential = cms.untracked.bool(True)
                    # Shuffle according to the seed above or randomly
                    random.shuffle(inputTypeAttrib.fileNames)
                    self.logger.info("Added %s events from the pileup blocks", eventsAvailable)

                    # Handle enforceGUIDInFileName for pileup
                    self.handleEnforceGUIDInFileName(inputTypeAttrib)

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
            if value.type_() in ["MixingModule", "DataMixingModule", "PreMixingModule"]:
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
        self.logger.info("Pileup JSON configuration file: '%s'", jsonPileupConfig)
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

        runIsComplete = getattr(self.jobBag, "runIsComplete", False)
        multiRun = getattr(self.jobBag, "multiRun", False)
        runLimits = getattr(self.jobBag, "runLimits", "")
        self.logger.info("DQMFileSaver set to multiRun: %s, runIsComplete: %s, runLimits: %s",
                         multiRun, runIsComplete, runLimits)

        self.process.dqmSaver.runIsComplete = cms.untracked.bool(runIsComplete)
        if multiRun and isCMSSWSupported(self.getCmsswVersion(), "CMSSW_8_0_0"):
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

    def handleLHEInput(self):
        """
        _handleLHEInput_

        Enable lazy-download for jobs reading LHE articles from CERN, such
        that these jobs can read data remotely
        """
        if getattr(self.jobBag, "lheInputFiles", False):
            self.logger.info("Enabling 'lazy-download' for lheInputFiles job")
            self.process.add_(cms.Service("SiteLocalConfigService",
                                          overrideSourceCacheHintDir=cms.untracked.string("lazy-download")))

        return

    def handleRepackSettings(self):
        """
        _handleRepackSettings_

        Repacking small events is super inefficient reading directly from EOS.
        """
        self.logger.info("Hardcoding read/cache strategies for repack")
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
            self.logger.info("Using fastCloning/lazydownload")
            self.process.add_(cms.Service("SiteLocalConfigService",
                                          overrideSourceCloneCacheHintDir=cms.untracked.string("lazy-download")))
        elif funcName == "merge":
            self.logger.info("Using lazydownload")
            self.process.add_(cms.Service("SiteLocalConfigService",
                                          overrideSourceCacheHintDir=cms.untracked.string("lazy-download")))
        return

    def handleCondorStatusService(self):
        """
        _handleCondorStatusService_

        Enable CondorStatusService for CMSSW releases that support it.
        """
        if isCMSSWSupported(self.getCmsswVersion(), "CMSSW_7_6_0"):
            self.logger.info("Tag chirp updates from CMSSW with step %s", self.step.data._internal_name)
            self.process.add_(cms.Service("CondorStatusService",
                                          tag=cms.untracked.string("_%s_" % self.step.data._internal_name)))

        return

    def handleEnforceGUIDInFileName(self, secondaryInput=None):
        """
        _handleEnforceGUIDInFileName_

        Enable enforceGUIDInFileName for CMSSW releases that support it.
        """
        # skip it for CRAB jobs
        if self.crabPSet:
            return

        if secondaryInput:
            inputSource = secondaryInput
            self.logger.info("Evaluating enforceGUIDInFileName parameter for secondary input data.")
        else:
            inputSource = self.process.source

        # only enable if source is PoolSource or EmbeddedRootSource
        if inputSource.type_() not in ["PoolSource", "EmbeddedRootSource"]:
            self.logger.info("Not evaluating enforceGUIDInFileName parameter for process source %s",
                             inputSource.type_())
            return

        self.logger.info("Evaluating if release %s supports enforceGUIDInFileName parameter...",
                         self.getCmsswVersion())

        # enable if release supports enforceGUIDInFileName
        if isEnforceGUIDInFileNameSupported(self.getCmsswVersion()):
            # check to make sure primary input files follow guid naming convention
            # prevents enabling guid checks on some workflows (StoreResults/StepChain) that use custom input file names
            # EmbeddedRootSource input files will always follow guid naming convention
            if inputSource.type_() == "PoolSource" and inputSource.fileNames:
                guidRegEx = re.compile("[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}.root$")
                if not guidRegEx.search(inputSource.fileNames[0]):
                    self.logger.info("Not enabling enforceGUIDInFileName due to non-GUID input file names")
                    return
            self.logger.info("Setting enforceGUIDInFileName to True.")
            inputSource.enforceGUIDInFileName = cms.untracked.bool(True)
        else:
            self.logger.info("CMSSW release does not support enforceGUIDInFileName.")

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
        self.logger.info("Executing SetupCMSSWPSet...")
        self.jobBag = self.job.getBaggage()

        scenario = getattr(self.step.data.application.configuration, "scenario", None)
        if scenario is not None and scenario != "":
            self.logger.info("Setting up job scenario/process")
            funcName = getattr(self.step.data.application.configuration, "function", None)
            if getattr(self.step.data.application.configuration, "pickledarguments", None) is not None:
                funcArgs = pickle.loads(self.step.data.application.configuration.pickledarguments)
            else:
                funcArgs = {}
            try:
                self.createProcess(scenario, funcName, funcArgs)
            except Exception as ex:
                self.logger.exception("Error creating process for Config/DataProcessing:")
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
                self.logger.exception("Error loading PSet:")
                raise ex

        # Check process.source exists
        if getattr(self.process, "source", None) is None:
            msg = "Error in CMSSW PSet: process is missing attribute 'source'"
            msg += " or process.source is defined with None value."
            self.logger.error(msg)
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
                    self.logger.info(
                        "Resizing a job with nStreams != nCores. Setting nStreams = nCores. This may end badly.")
                    eventStreams = 0
                options = getattr(self.process, "options", None)
                if options is None:
                    self.process.options = cms.untracked.PSet()
                    options = getattr(self.process, "options")
                options.numberOfThreads = cms.untracked.uint32(numCores)
                options.numberOfStreams = cms.untracked.uint32(eventStreams)
            except AttributeError as ex:
                self.logger.error("Failed to override numberOfThreads: %s", str(ex))

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

        # tweak for jobs reading LHE articles from CERN
        self.handleLHEInput()

        # tweak jobs for enforceGUIDInFileName
        self.handleEnforceGUIDInFileName()

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
            self.logger.info("Found a TFC override: %s", self.step.data.application.overrideCatalog)
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
            self.logger.exception("Error writing out PSet:")
            raise ex
        self.logger.info("CMSSW PSet setup completed!")

        return 0
