"""
_SetupCMSSWPset_

Create a CMSSW PSetTweak JSON to modify the PSet Configuration
for the WMAgent Job

"""
from __future__ import print_function

import json
import logging
import os
import pickle
import random
import socket
import re

from PSetTweaks.PSetTweak import PSetTweak, PSetHolder
from PSetTweaks.WMTweak import applyTweak, makeJobTweak, makeOutputTweak, makeTaskTweak, resizeResources
from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig
from WMCore.Storage.TrivialFileCatalog import TrivialFileCatalog
from WMCore.WMRuntime.ScriptInterface import ScriptInterface
from WMCore.WMRuntime.Tools.Scram import isCMSSWSupported, isEnforceGUIDInFileNameSupported


class SetupCMSSWPset(ScriptInterface):
    """
    _SetupCMSSWPset_

    """

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
        stepName = self.step.data._internal_name
        seeding = getattr(self.jobBag, "seeding", None)
        self.logger.info("Job seeding set to: %s", seeding)
        if seeding == "ReproducibleSeeding":
            randService = self.process.RandomNumberGeneratorService
            tweak = PSetTweak()
            for x in randService:
                parameter = "process.RandomNumberGeneratorService.%s.initialSeed" % x._internal_name
                tweak.addParameter(parameter, x.initialSeed)
            self.logger.info("--- Random seeding Level Tweaks ---")
            seedTweakfile = "/tmp/seedTweak_{0}.json".format(stepName)
            seedTweakInput = "/tmp/seedTweak_input_{0}.pkl".format(stepName)
            seedTweakOutput = "/tmp/seedTweak_output_{0}.pkl".format(stepName)
            self.logger.info("Create job level tweak, write to: {0}".format(seedTweakfile))
            self.logger.info("Pickled applyTweak input object: {0}".format(seedTweakInput))
            pickle.dump(self.process, open(seedTweakInput, "wb"))
            tweak.persist(seedTweakfile, formatting="json")
            applyTweak(self.process, tweak, self.fixupDict)
            self.logger.info("Pickled applyTweak output object: {0}".format(seedTweakOutput))
            pickle.dump(self.process, open(seedTweakOutput, "wb"))
        else:
            if hasattr(self.process, "RandomNumberGeneratorService"):
                from IOMC.RandomEngine.RandomServiceHelper import RandomNumberServiceHelper
                helper = RandomNumberServiceHelper(self.process.RandomNumberGeneratorService)
                helper.populate()
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
                                inputTypeAttrib.fileNames.append(str(fileLFN['logical_file_name']))
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

            if funcName in ["merge", "alcaHarvesting"]:
                self.handleSingleCoreOverride()

        # Check process.source exists
        if self.process is None:
            self.process = PSetHolder("process")
            setattr(self.process, "source", PSetHolder("source"))
            self.process.parameters_.append("source")


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
                    setattr(self.process, "options", PSetHolder("options"))

                setattr(self.process.options, "numberOfThreads", numCores)
                setattr(self.process.options, "numberOfStreams", eventStreams)
                self.process.options.numberOfThreads.parameters_.append("numberOfThreads")
                self.process.options.numberOfThreads.parameters_.append("numberOfStreams")

            except AttributeError as ex:
                self.logger.error("Failed to override numberOfThreads: %s", str(ex))

        tweak = PSetTweak()
        # Apply task level tweaks
        stepName = self.step.data._internal_name
        self.logger.info("--- Task Level Tweaks ---")
        makeTaskTweak(self.step.data, tweak)

        # Check if chained processing is enabled
        # If not - apply the per job tweaks
        # If so - create an override TFC (like done in PA) and then modify thePSet accordingly
        if hasattr(self.step.data.input, "chainedProcessing") and self.step.data.input.chainedProcessing:
            pass
            #self.handleChainedProcessing()
        else:
            # Apply per job PSet Tweaks
            self.logger.info("--- Job Level Tweaks ---")
            jobTweak = makeJobTweak(self.job, tweak)

        # check for pileup settings presence, pileup support implementation
        # and if enabled, process pileup configuration / settings
        if hasattr(self.step.data, "pileup"):
            self.handlePileup()

        # Apply per output module PSet Tweaks
        cmsswStep = self.step.getTypeHelper()
        for om in cmsswStep.listOutputModules():
            mod = cmsswStep.getOutputModule(om)
            modName = str(getattr(mod, "_internal_name"))
            self.logger.info("--- outputModule Level Tweaks ---")
            outTweak = makeOutputTweak(mod, self.job, tweak)

        # revlimiter for testing
        if getattr(self.step.data.application.command, "oneEventMode", False):
            self.process.maxEvents.input = 1

        # check for random seeds and the method of seeding which is in the job baggage
        self.handleSeeding()

        # check for event numbers in the producers
        #self.handleProducersNumberOfEvents()

        # tweak jobs for enforceGUIDInFileName
        #self.handleEnforceGUIDInFileName()

        # Check if we accept skipping bad files
        if hasattr(self.step.data.application.configuration, "skipBadFiles"):
            tweak.addParameter("process.source.skipBadFiles", bool(self.step.data.application.configuration.skipBadFiles))

        # Apply events per lumi section if available
        if hasattr(self.step.data.application.configuration, "eventsPerLumi"):
            tweak.addParameter("process.source.numberEventsInLuminosityBlock",
                self.step.data.application.configuration.eventsPerLumi)

        # limit run time if desired
        if hasattr(self.step.data.application.configuration, "maxSecondsUntilRampdown"):
            setattr(self.process, "maxSecondsUntilRampdown", PSetHolder("maxSecondsUntilRampdown"))
            setattr(self.process.maxSecondsUntilRampdown, "input", self.step.data.application.configuration.maxSecondsUntilRampdown)
            self.process.maxSecondsUntilRampdown.parameters_.append("input")

        # accept an overridden TFC from the step
        if hasattr(self.step.data.application, 'overrideCatalog'):
            self.logger.info("Found a TFC override: %s", self.step.data.application.overrideCatalog)
            tweak.addParameter("process.source.overrideCatalog",
                str(self.step.data.application.overrideCatalog))

        configFile = self.step.data.application.command.configuration
        configPickle = getattr(self.step.data.application.command, "configurationPickle", "PSet.pkl")
        workingDir = self.stepSpace.location
        try:
            with open("%s/%s" % (workingDir, configPickle), 'wb') as pHandle:
                pickle.dump(self.process, pHandle)
        except Exception as ex:
            self.logger.exception("Error writing out PSet:")
            raise
        tweak.persist("PSetTweak.json")
        self.logger.info("CMSSW PSetTweak JSON completed!")

        return 0
