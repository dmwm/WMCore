"""
_SetupCMSSWPset_

Create a CMSSW PSet suitable for running a WMAgent job.

"""

import os
import random
import types
import socket
import traceback
import pickle

from WMCore.WMRuntime.ScriptInterface import ScriptInterface
from WMCore.Storage.TrivialFileCatalog import TrivialFileCatalog
from PSetTweaks.PSetTweak import PSetTweak
import WMCore.WMSpec.WMStep as WMStep
from PSetTweaks.WMTweak import makeTweak, applyTweak
from PSetTweaks.WMTweak import makeOutputTweak, makeJobTweak, makeTaskTweak
from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig
from WMCore.Wrappers.JsonWrapper import JSONDecoder

import FWCore.ParameterSet.Config as cms


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
        process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32(-1))
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
                print msg
                return None
        elif funcName == "repack":
            try:
                from Configuration.DataProcessing.Repack import repackProcess
                self.process = repackProcess(**funcArgs)
            except Exception as ex:
                msg = "Failed to create a repack process."
                print msg
                return None
        else:
            try:
                from Configuration.DataProcessing.GetScenario import getScenario
                scenarioInst = getScenario(scenario)
                self.process = getattr(scenarioInst, funcName)(**funcArgs)
            except Exception as ex:
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

        Load a PSet that was shipped with the job sandbox.

        """
        psetModule = "WMTaskSpace.%s.PSet" % self.step.data._internal_name

        try:
            processMod = __import__(psetModule, globals(), locals(), ["process"], -1)
            self.process = processMod.process
        except ImportError as ex:
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
        import FWCore.ParameterSet.Config as PSetConfig

        # include the default performance report services
        if getattr(self.step.data.application.command, 'silentMemoryCheck', False):
            self.process.add_(PSetConfig.Service("SimpleMemoryCheck", jobReportOutputOnly=PSetConfig.untracked.bool(True)))
        else:
            self.process.add_(PSetConfig.Service("SimpleMemoryCheck"))

        self.process.add_(PSetConfig.Service("CPU"))
        self.process.add_(PSetConfig.Service("Timing"))
        self.process.Timing.summaryOnly = PSetConfig.untracked(PSetConfig.bool(True))

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
        tfc.addMapping("direct", inputFile, inputFile,
                       mapping_type = "lfn-to-pfn")
        tfc.addMapping("direct", inputFile, inputFile,
                       mapping_type = "pfn-to-lfn")

        fixupFileNames(self.process)
        fixupMaxEvents(self.process)
        self.process.source.fileNames.setValue([inputFile])
        self.process.maxEvents.input.setValue(-1)

        tfcName = "override_catalog.xml"
        tfcPath = os.path.join(os.getcwd(), tfcName)
        print "Creating override TFC, contents below, saving into '%s'" % tfcPath
        tfcStr = tfc.getXML()
        print tfcStr
        tfcFile = open(tfcPath, 'w')
        tfcFile.write(tfcStr)
        tfcFile.close()
        self.step.data.application.overrideCatalog = "trivialcatalog_file:" +tfcPath + "?protocol=direct"

        return

    def handlePileup(self):
        """
        _handlePileup_

        Handle pileup settings.
        """
        # find out local site SE name
        siteConfig = loadSiteLocalConfig()
        seLocalName = siteConfig.localStageOut["se-name"]
        print "Running on site '%s', local SE name: '%s'" % (siteConfig.siteName, seLocalName)

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
        self._processPileupMixingModules(pileupDict, seLocalName, dataMixModules, "data")
        self._processPileupMixingModules(pileupDict, seLocalName, mixModules, "mc")

        return

    def _processPileupMixingModules(self, pileupDict, seLocalName, modules, requestedPileupType):
        """
        Iterates over all modules and over all pileup configuration types.
        The only considered types are "data" and "mc" (input to this method).
        If other pileup types are specified by the user, the method doesn't
        modify anything.

        The method considers only files which are present on this local
        SE (seLocalName). The job will use only those. Dataset, divided into
        blocks, may not have all blocks present on a particular SE. However,
        all files belonging into a block will be present when reported by DBS.

        The structure of the pileupDict: PileupFetcher._queryDbsAndGetPileupConfig

        2011-02-03:
        According to the current implementation of helper testing module
        WMCore_t/WMRuntime_t/Scripts_t/WMTaskSpace/cmsRun1/PSet.py
        each type of modules instances can have either "secsource"
        or "input" attribute, so need to probe both, one shall succeed.
        """
        for m in modules:
            for pileupType in self.step.data.pileup.listSections_():
                # there should be either "input" or "secsource" attributes
                # and both "MixingModule", "DataMixingModule" can have both
                inputTypeAttrib = getattr(m, "input", None) or getattr(m, "secsource", None)
                if not inputTypeAttrib:
                    continue
                inputTypeAttrib.fileNames = cms.untracked.vstring()
                if pileupType == requestedPileupType:
                    # not all blocks may be stored on the local SE, loop over
                    # all blocks and consider only files stored locally
                    eventsAvailable = 0
                    for blockName in sorted(pileupDict[pileupType].keys()):
                        blockDict = pileupDict[pileupType][blockName]
                        if seLocalName in blockDict["PhEDExNodeNames"]:
                            eventsAvailable += int(blockDict.get('NumberOfEvents', 0))
                            for fileLFN in blockDict["FileList"]:
                                inputTypeAttrib.fileNames.append(fileLFN['logical_file_name'])
                    if requestedPileupType == 'data':
                        baggage = self.job.getBaggage()
                        if getattr(baggage, 'skipPileupEvents', None) is not None:
                            # For deterministic pileup, we want to shuffle the list the
                            # same for every job in the task and skip events
                            random.seed(self.job['task'])
                            print "Skipping %d pileup events for deterministic data mixing" % baggage.skipPileupEvents
                            inputTypeAttrib.skipEvents = cms.untracked.uint32(int(baggage.skipPileupEvents) % eventsAvailable)
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
            if value.type_() in [ "MixingModule", "DataMixingModule"] :
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
        print "Pileup JSON configuration file: '%s'" % jsonPileupConfig
        # load the JSON config file into a Python dictionary
        decoder = JSONDecoder()
        try:
            f = open(jsonPileupConfig, 'r')
            json = f.read()
            pileupDict =  decoder.decode(json)
            f.close()
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
        baggage = self.job.getBaggage()
        runIsComplete = getattr(baggage, "runIsComplete", False)
        if hasattr(self.process, "dqmSaver"):
            self.process.dqmSaver.runIsComplete = cms.untracked.bool(runIsComplete)
            if hasattr(self.step.data.application.configuration, "pickledarguments"):
                args = pickle.loads(self.step.data.application.configuration.pickledarguments)
                datasetName = args.get('datasetName', None)
                if datasetName is not None:
                    self.process.dqmSaver.workflow = cms.untracked.string(datasetName)
        return

    def handleRepackSettings(self):
        """
        _handleRepackSettings_

        Disable lazy-download for repacking (no benefit on streamer files).

        Repack jobs should only use one core in CMSSW
        """
        print "Hardcoding read/cache strategies for repack"
        self.process.add_(
            cms.Service("SiteLocalConfigService",
                        overrideSourceCacheHintDir = cms.untracked.string("storage-only"),
                        overrideSourceReadHint = cms.untracked.string("read-ahead-buffered"),
                        overrideSourceTTreeCacheSize = cms.untracked.uint32(20*1024*1024)
                        )
            )

        try:
            if int(self.step.data.application.multicore.numberOfCores) > 1:
                self.step.data.application.multicore.numberOfCores = 1
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
        cmsswVersion = os.environ['CMSSW_VERSION']
        if cmsswVersion.startswith("CMSSW_7_5") and False:
            print "Using fastCloning/lazydownload"
            self.process.add_(
                cms.Service("SiteLocalConfigService",
                            overrideSourceCloneCacheHintDir=cms.untracked.string("lazy-download")
                            )
                )
        elif funcName == "merge":
            print "Using lazydownload"
            self.process.add_(
                cms.Service("SiteLocalConfigService",
                            overrideSourceCacheHintDir=cms.untracked.string("lazy-download")
                            )
                )
        return

    def __call__(self):
        """
        _call_

        Examine the step configuration and construct a PSet from that.

        """
        self.process = None
        funcName = None

        scenario = getattr(self.step.data.application.configuration, "scenario", None)
        if scenario != None and scenario != "":
            funcName = getattr(self.step.data.application.configuration, "function", None)
            if getattr(self.step.data.application.configuration, "pickledarguments", None) != None:
                funcArgs = pickle.loads(getattr(self.step.data.application.configuration, "pickledarguments", None))
            else:
                funcArgs = {}
            try:
                self.createProcess(scenario, funcName, funcArgs)
            except Exception as ex:
                print "Error creating process for Config/DataProcessing:"
                print traceback.format_exc()
                raise ex

            if funcName == "repack":
                self.handleRepackSettings()

            if socket.getfqdn().endswith("cern.ch"):
                self.handleSpecialCERNMergeSettings(funcName)

        else:
            try:
                self.loadPSet()
            except Exception as ex:
                print "Error loading PSet:"
                print traceback.format_exc()
                raise ex

        self.fixupProcess()

        try:
            if int(self.step.data.application.multicore.numberOfCores) > 1:
                numCores = int(self.step.data.application.multicore.numberOfCores)
                options = getattr(self.process, "options", None)
                if options == None:
                    self.process.options = cms.untracked.PSet()
                    options = getattr(self.process, "options")

                options.numberOfThreads = cms.untracked.uint32(numCores)
                options.numberOfStreams = cms.untracked.uint32(0)        # For now, same as numCores
        except AttributeError:
                print "No value for numberOfCores. Not setting"

        psetTweak = getattr(self.step.data.application.command, "psetTweak", None)
        if psetTweak != None:
            self.applyPSetTweak(psetTweak, self.fixupDict)

        # Apply task level tweaks
        taskTweak = makeTaskTweak(self.step.data)
        applyTweak(self.process, taskTweak, self.fixupDict)

        # Check if chained processing is enabled
        # If not - apply the per job tweaks
        # If so - create an override TFC (like done in PA) and then modify thePSet accordingly
        if (hasattr(self.step.data.input, "chainedProcessing") and
            self.step.data.input.chainedProcessing):
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

        #Apply events per lumi section if available
        if hasattr(self.step.data.application.configuration, "eventsPerLumi"):
            self.process.source.numberEventsInLuminosityBlock = \
                cms.untracked.uint32(self.step.data.application.configuration.eventsPerLumi)

        # accept an overridden TFC from the step
        if hasattr(self.step.data.application,'overrideCatalog'):
            print "Found a TFC override: %s" % self.step.data.application.overrideCatalog
            self.process.source.overrideCatalog = \
                cms.untracked.string(self.step.data.application.overrideCatalog)

        configFile = self.step.data.application.command.configuration
        configPickle = getattr(self.step.data.application.command, "configurationPickle", "PSet.pkl")
        workingDir = self.stepSpace.location
        handle = open("%s/%s" % (workingDir, configFile), 'w')
        pHandle = open("%s/%s" % (workingDir, configPickle), 'wb')
        try:
            pickle.dump(self.process, pHandle)
            handle.write("import FWCore.ParameterSet.Config as cms\n")
            handle.write("import pickle\n")
            handle.write("handle = open('%s', 'rb')\n" % configPickle)
            handle.write("process = pickle.load(handle)\n")
            handle.write("handle.close()\n")
        except Exception as ex:
            print "Error writing out PSet:"
            print traceback.format_exc()
            raise ex
        finally:
            handle.close()
            pHandle.close()

        return 0
