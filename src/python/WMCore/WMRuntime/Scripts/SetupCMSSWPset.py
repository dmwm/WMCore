"""
_SetupCMSSWPset_

Create a CMSSW PSet suitable for running a WMAgent job.

"""

import os
import types
import socket
import traceback

from WMCore.WMRuntime.ScriptInterface import ScriptInterface
from WMCore.Storage.TrivialFileCatalog import TrivialFileCatalog 
from PSetTweaks.PSetTweak import PSetTweak
import WMCore.WMSpec.WMStep as WMStep
from PSetTweaks.WMTweak import makeTweak, applyTweak
from PSetTweaks.WMTweak import makeOutputTweak, makeJobTweak, makeTaskTweak
from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig
from WMCore.Wrappers.JsonWrapper import JSONDecoder

import FWCore.ParameterSet.Config as cms

applyPromptReco = lambda s, a: s.promptReco(**a)
applyAlcaSkim = lambda s, a: s.alcaSkim(**a)
applySkimming = lambda s, a: s.skimming(**a)
applyDqmHarvesting = lambda s, a: s.dqmHarvesting(**a)



def fixupGlobalTag(process):
    """
    _fixupGlobalTag_

    Make sure that the process has a GlobalTag PSet and a globaltag string.
    
    """
    if not hasattr(process, "GlobalTag"):
        process.GlobalTag = cms.PSet(globalTag = cms.string(""))
    if not hasattr(process.GlobalTag, "globaltag"):
        process.GlobalTag.globaltag = cms.string("")
        

def fixupFirstRun(process):
    """
    _fixupFirstRun_

    Make sure that the process has a firstRun parameter.
    
    """
    if not hasattr(process.source, "firstRun"):
        process.source.firstRun = cms.untracked.uint32(0)


def fixupLastRun(process):
    """
    _fixupLastRun_

    Make sure that the process has a lastRun parameter.
    
    """
    if not hasattr(process.source, "lastRun"):
        process.source.firstRun = cms.untracked.uint32(0)        


def fixupLumisToProcess(process):
    """
    _fixupLumitsToProcess_

    Make sure that the process has a lumisToProcess parameter.
    
    """
    if not hasattr(process.source, "lumisToProcess"):
        process.source.lumisToProcess = cms.untracked.VLuminosityBlockRange()


def fixupSkipEvents(process):
    """
    _fixupSkipEvents_

    Make sure that the process has a skip events parameter.
    
    """
    if not hasattr(process.source, "skipEvents"):
        process.source.skipEvents = cms.untracked.uint32(0)


def fixupMaxEvents(process):
    """
    _fixupMaxEvents_

    Make sure that the process has a max events parameter.
    
    """
    if not hasattr(process, "maxEvents"):
        process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32(-1))
    if not hasattr(process.maxEvents, "input"):
        process.maxEvents.input = cms.untracked.int32(-1)


def fixupFileNames(process):
    """
    _fixupFileNames_

    Make sure that the process has a fileNames parameter.  This will also
    configure lazy download for the process.
    
    """
    if not process.services.has_key("AdaptorConfig"):
        process.add_(cms.Service("AdaptorConfig"))

    process.services["AdaptorConfig"].cacheHint = cms.untracked.string("lazy-download")
    process.services["AdaptorConfig"].readHint = cms.untracked.string("auto-detect")

    if not hasattr(process.source, "fileNames"):
        process.source.fileNames = cms.untracked.vstring()


def fixupSecondaryFileNames(process):
    """
    _fixupSecondaryFileNames_

    Make sure that the process has a secondaryFileNames parameter.
    
    """
    if not hasattr(process.source, "secondaryFileNames"):
        process.source.secondaryFileNames = cms.untracked.vstring()

def fixupFirstLumi(process):
    """
    _fixupFirstLumi
    
    Make sure that the process has firstLuminosityBlock parameter.
    """
    if not hasattr(process.source, "firstLuminosityBlock"):
        process.source.firstLuminosityBlock = cms.untracked.uint32(1)

class SetupCMSSWPset(ScriptInterface):
    """
    _SetupCMSSWPset_

    """
    funcMap = {
        "promptReco": applyPromptReco,
        "alcaSkim": applyAlcaSkim,
        "skimming": applySkimming,
        "dqmHarvesting": applyDqmHarvesting
        }

    fixupDict = {"process.GlobalTag.globaltag": fixupGlobalTag,
                 "process.source.fileNames": fixupFileNames,
                 "process.source.secondaryFileNames": fixupSecondaryFileNames,
                 "process.maxEvents.input": fixupMaxEvents,
                 "process.source.skipEvents": fixupSkipEvents,
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
            try:
                from Configuration.DataProcessing.Merge import mergeProcess
                self.process = mergeProcess([])
            except Exception, ex:
                msg = "Failed to create a merge process."
                print msg
                return None
        else:
            try:
                from Configuration.DataProcessing.GetScenario import getScenario
                scenarioInst = getScenario(scenario)
                applicationFunc = self.funcMap[funcName]

                if type(funcArgs) == type({}):
                    # Our function arguments are already a dictionary, which
                    # means they're probably the result of some JSON decoding.
                    # We'll have to make sure they don't contain any unicode
                    # strings.
                    strArgs = {}
                    for key in funcArgs.keys():
                        value = funcArgs[key]
                        
                        if type(key) in types.StringTypes:
                            key = str(key)
                        if type(value) in types.StringTypes:
                            value = str(value)
                        elif type(value) == type([]):
                            newValue = []
                            for item in value:
                                if type(item) in types.StringTypes:
                                    newValue.append(str(item))
                                else:
                                    newValue.append(item)

                            value = newValue
                        
                        strArgs[key] = value
                        
                    self.process = applicationFunc(scenarioInst,  strArgs)
                else:
                    # Our function arguments are most likely in the form of a
                    # config section.
                    self.process = applicationFunc(scenarioInst,
                                                   funcArgs.dictionary_())
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

        Load a PSet that was shipped with the job sandbox.
        
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


    def applyTweak(self, setTweak):
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
        if seeding == None:
            return
        if seeding == "AutomaticSeeding":
            from IOMC.RandomEngine.RandomServiceHelper import RandomNumberServiceHelper
            helper = RandomNumberServiceHelper(self.process.RandomNumberGeneratorService)
            helper.populate()
            return
        if seeding == "ReproducibleSeeding":
            randService = self.process.RandomNumberGeneratorService
            tweak = PSetTweak()
            for x in randService:
                parameter = "process.RandomNumberGeneratorService.%s.initialSeed" % x._internal_name
                tweak.addParameter(parameter, x.initialSeed)
            applyTweak(self.process, tweak, self.fixupDict)
            return
        # still here means bad seeding algo name
        raise RuntimeError, "Bad Seeding Algorithm: %s" % seeding
    
    def handlePerformanceSettings(self):
        """
        _handlePerformanceSettings_
        
        Install the standard performance report services
        """
        import FWCore.ParameterSet.Config as PSetConfig
       
        # include the default performance report services
        self.process.add_(PSetConfig.Service("SimpleMemoryCheck"))
        self.process.add_(PSetConfig.Service("Timing"))
        self.process.Timing.summaryOnly = PSetConfig.untracked(PSetConfig.bool(True))
    
    
    def _handleChainedProcessing(self):
        """
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


    def _handlePileup(self):
        """
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
        
        
    def _processPileupMixingModules(self, pileupDict, seLocalName, modules,
                                    requestedPileupType):
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
                inputTypeAttrib.fileNames = cms.untracked.vstring()
                if pileupType == requestedPileupType:
                    # not all blocks may be stored on the local SE, loop over
                    # all blocks and consider only files stored locally                    
                    for blockDict in pileupDict[pileupType].values():
                        if seLocalName in blockDict["StorageElementNames"]:
                            for fileLFN in blockDict["FileList"]:
                                inputTypeAttrib.fileNames.append(str(fileLFN))
                    
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
            if value.type_() == "MixingModule":
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
        
        
    def __call__(self):
        """
        _call_

        Examine the step configuration and construct a PSet from that.
        
        """
        self.process = None

        scenario = getattr(self.step.data.application.configuration, "scenario", None)
        if scenario != None and scenario != "":
            funcName = getattr(self.step.data.application.configuration, "function", None)
            funcArgs = getattr(self.step.data.application.configuration, "arguments", None)
            try:
                self.createProcess(scenario, funcName, funcArgs)
            except Exception, ex:
                print "Error creating process for Config/DataProcessing:"
                print traceback.format_exc()
                raise ex
        else:
            try:
                self.loadPSet()
            except Exception, ex:
                print "Error loading PSet:"
                print traceback.format_exc()
                raise ex

        self.fixupProcess()

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
            self._handleChainedProcessing()
        else:
            # Apply per job PSet Tweaks
            jobTweak = makeJobTweak(self.job)
            applyTweak(self.process, jobTweak, self.fixupDict)
            
        # check for pileup settings presence, pileup support implementation
        # and if enabled, process pileup configuration / settings
        if hasattr(self.step.data, "pileup"):
            self._handlePileup()        

        # Apply per output module PSet Tweaks
        cmsswStep = self.step.getTypeHelper()
        for om in cmsswStep.listOutputModules():
            mod = cmsswStep.getOutputModule(om)
            outTweak = makeOutputTweak(mod, self.job)
            applyTweak(self.process, outTweak, self.fixupDict)
            
        # revlimiter for testing
        #self.process.maxEvents.input = 2
        
        # check for random seeds and the method of seeding which is in the job baggage
        self.handleSeeding()

        # make sure default parametersets for perf reports are installed
        self.handlePerformanceSettings()
        
        # accept an overridden TFC from the step
        if hasattr(self.step.data.application,'overrideCatalog'):
            print "Found a TFC override: %s" % self.step.data.application.overrideCatalog
            self.process.source.overrideCatalog = \
                cms.untracked.string(self.step.data.application.overrideCatalog)

        # If we're running on a FNAL worker node override the TFC so we can
        # test lustre.
        hostname = socket.gethostname()
        if hostname.endswith("fnal.gov"):
            for inputFile in self.job["input_files"]:
                if inputFile["lfn"].find("unmerged") != -1:
                    self.process.source.overrideCatalog = \
                        cms.untracked.string("trivialcatalog_file:/uscmst1/prod/sw/cms/SITECONF/T1_US_FNAL/PhEDEx/storage-test.xml?protocol=dcap")
        
        configFile = self.step.data.application.command.configuration
        workingDir = self.stepSpace.location
        handle = open("%s/%s" % (workingDir, configFile), 'w')
        try:
            handle.write(self.process.dumpPython())
        except Exception, ex:
            print "Error writing out PSet:"
            print traceback.format_exc()
            raise ex
        
        handle.close()
        return 0
