#!/usr/bin/env python
"""
_CMSSW_

Template for a CMSSW Step

"""

import pickle

from future.utils import viewitems

from Utils.Utilities import encodeUnicodeToBytes
from WMCore.WMSpec.ConfigSectionTree import nodeName
from WMCore.WMSpec.Steps.Template import CoreHelper, Template
from WMCore.Cache.WMConfigCache import ConfigCacheException
import shlex


class CMSSWStepHelper(CoreHelper):
    """
    _CMSSWStepHelper_

    Add API calls and helper methods to the basic WMStepHelper to specialise
    for CMSSW tasks

    """
    def setAcqEra(self, acqEra):
        """
        _setAcqEra_
        Set the acquisition era attribute for this step.
        """
        self.data.output.acqEra = acqEra

    def setProcStr(self, procStr):
        """
        _setProcStr_
        Set the processing string attribute for this step.
        """
        self.data.output.procStr = procStr

    def setProcVer(self, procVer):
        """
        _setProcVer_
        Set the processing version era attribute for this step.
        """
        self.data.output.procVer = procVer

    def getAcqEra(self):
        """
        _getAcqEra_
        Retrieve the acquisition era for this step, or return None if non-existent.
        """
        return getattr(self.data.output, 'acqEra', None)

    def getProcStr(self):
        """
        _getProcStr_
        Retrieve the processing string for this step, or return None if non-existent.
        """
        return getattr(self.data.output, 'procStr', None)

    def getProcVer(self):
        """
        _getProcVer_
        Retrieve the processing version for this step, or return None if non-existent.
        """
        return getattr(self.data.output, 'procVer', None)

    def setPrepId(self, prepId):
        """
        _setPrepId_
        Set the prep_id attribute for this step.
        """
        self.data.output.prepId = prepId

    def getPrepId(self):
        """
        _getPrepId_
        Retrieve the prep_id for this step, or return None if non-existent.
        """
        return getattr(self.data.output, 'prepId', None)

    def addOutputModule(self, moduleName, **details):
        """
        _addOutputModule_

        Add in an output module settings, all default to None unless
        the value is provided in details

        """
        modules = self.data.output.modules

        if getattr(modules, moduleName, None) == None:
            modules.section_(moduleName)
        module = getattr(modules, moduleName)

        for key, value in viewitems(details):
            setattr(module, key, value)

        return

    def listOutputModules(self):
        """
        _listOutputModules_

        retrieve list of output module names

        """
        if hasattr(self.data.output, "modules"):
            return list(self.data.output.modules.dictionary_())

        return []

    def getOutputModule(self, name):
        """
        _getOutputModule_

        retrieve the data structure for an output module by name
        None if not found

        """
        return getattr(self.data.output.modules, name, None)

    def setConfigCache(self, url, document, dbName="config_cache"):
        """
        _setConfigCache_

        Set the information required to retrieve a configuration from
        the config cache.

        url - base URL for the config cache instance
        document - GUID for the config document
        dbName - optional, name of the db instance in the couch server

        """
        self.data.application.configuration.configCacheUrl = url
        self.data.application.configuration.configId = document
        self.data.application.configuration.cacheName = dbName
        docUrl = "%s/%s/%s" % (url, dbName, document)
        self.data.application.configuration.configUrl = docUrl
        self.data.application.configuration.retrieveConfigUrl = "%s/configFile" % docUrl

    def setDataProcessingConfig(self, scenarioName, functionName, **args):
        """
        _setDataProcessingConfig_

        Set a configuration library to be used from the CMSSW Release
        DataProcessing package.

        """
        self.data.application.configuration.scenario = scenarioName
        self.data.application.configuration.function = functionName
        # assume if this crashes we are dealing with complex data
        # which is only supported in new agents that only look
        # at pickledarguments anyways
        try:
            self.data.application.configuration.section_('arguments')
            [setattr(self.data.application.configuration.arguments, k, v) for k, v in viewitems(args)]
        except Exception:
            pass
        # FIXME: once both central services and WMAgent are in Py3, we can remove protocol=0
        self.data.application.configuration.pickledarguments = pickle.dumps(args, protocol=0)
        return

    def cmsswSetup(self, cmsswVersion, **options):
        """
        _cmsswSetup_

        Provide setup details for CMSSW.

        cmsswVersion - required - version of CMSSW to use

        Optional:

        scramCommand - defaults to scramv1
        scramProject - defaults to CMSSW
        scramArch    - optional scram architecture, defaults to None
        buildArch    - optional scram build architecture, defaults to None
        softwareEnvironment - setup command to bootstrap scram,defaults to None
        """
        self.data.application.setup.cmsswVersion = cmsswVersion
        for k, v in viewitems(options):
            setattr(self.data.application.setup, k, v)
        return

    def getScramArch(self):
        """
        _getScramArch_

        Retrieve the scram architecture used for this step.
        """
        return self.data.application.setup.scramArch

    def getCMSSWVersion(self):
        """
        _getCMSSWVersion_

        Retrieve the version of the framework used for this step.
        """
        return self.data.application.setup.cmsswVersion

    def setGlobalTag(self, globalTag):
        """
        _setGlobalTag_

        Set the global tag.
        """
        self.data.application.configuration.section_('arguments')
        self.data.application.configuration.arguments.globalTag = globalTag

        args = {}
        if hasattr(self.data.application.configuration, "pickledarguments"):
            args = pickle.loads(encodeUnicodeToBytes(self.data.application.configuration.pickledarguments))
        args['globalTag'] = globalTag
        # FIXME: once both central services and WMAgent are in Py3, we can remove protocol=0
        self.data.application.configuration.pickledarguments = pickle.dumps(args, protocol=0)

        return

    def getGlobalTag(self):
        """
        _getGlobalTag_

        Retrieve the global tag.
        """
        if hasattr(self.data.application.configuration, "arguments"):
            if hasattr(self.data.application.configuration.arguments, "globalTag"):
                return self.data.application.configuration.arguments.globalTag

        pickledArgs = encodeUnicodeToBytes(self.data.application.configuration.pickledarguments)
        return pickle.loads(pickledArgs)['globalTag']

    def setDatasetName(self, datasetName):
        """
        _setDatasetName_

        Set the dataset name in the pickled arguments
        """
        self.data.application.configuration.section_('arguments')
        self.data.application.configuration.arguments.datasetName = datasetName

        args = {}
        if hasattr(self.data.application.configuration, "pickledarguments"):
            args = pickle.loads(encodeUnicodeToBytes(self.data.application.configuration.pickledarguments))
        args['datasetName'] = datasetName
        # FIXME: once both central services and WMAgent are in Py3, we can remove protocol=0
        self.data.application.configuration.pickledarguments = pickle.dumps(args, protocol=0)

        return

    def getDatasetName(self):
        """
        _getDatasetName_

        Retrieve the dataset name from the pickled arguments
        """
        if hasattr(self.data.application.configuration, "arguments"):
            if hasattr(self.data.application.configuration.arguments, "datasetName"):
                return self.data.application.configuration.arguments.datasetName

        return pickle.loads(self.data.application.configuration.pickledarguments).get('datasetName', None)

    def getScenario(self):
        """
        _getScenario_

        Retrieve the scenario from the pickled arguments, if any
        """
        if hasattr(self.data.application.configuration, "scenario"):
            return self.data.application.configuration.scenario

        return None

    def setUserSandbox(self, userSandbox):
        """
        _setUserSandbox_

        Sets the userSandbox. Eventually may have to move this to a proper
        list rather than a one element list
        """
        if userSandbox:
            self.data.user.inputSandboxes = [userSandbox]
        return

    def setUserFiles(self, userFiles):
        """
        _setUserFiles_

        Sets the list of extra files the user needs
        """
        if userFiles:
            self.data.user.userFiles = userFiles
        return

    def setUserLFNBase(self, lfnBase):
        """
        _setUserFiles_

        Sets the list of extra files the user needs
        """
        if lfnBase:
            self.data.user.lfnBase = lfnBase
        return

    def setupChainedProcessing(self, inputStepName, inputOutputModule):
        """
        _setupChainedProcessing_

        Set values to support chained CMSSW running.
        """
        self.data.input.chainedProcessing = True
        self.data.input.inputStepName = inputStepName
        self.data.input.inputOutputModule = inputOutputModule

    def keepOutput(self, keepOutput):
        """
        _keepOutput_

        Mark whether or not we should keep the output from this step.  We don't
        want to keep the output from certain chained steps.
        """
        self.data.output.keep = keepOutput
        return

    def getPileup(self):
        """
        _getPileup_

        Retrieve the pileup config from this step.
        """
        return getattr(self.data, "pileup", None)

    def setupPileup(self, pileupConfig, dbsUrl):
        """
        include pileup input configuration into this step configuration.
        pileupConfig is initially specified as input to the workload
        (user input) and here is available as a dict.

        """
        # so, e.g. this {"cosmics": "/some/cosmics/dataset", "minbias": "/some/minbias/dataset"}
        # would translate into
        # self.data.pileup.comics.dataset = "/some/cosmics/dataset"
        # self.data.pileup.minbias.dataset = "/some/minbias/dataset"
        self.data.section_("pileup")
        for pileupType, dataset in viewitems(pileupConfig):
            self.data.pileup.section_(pileupType)
            setattr(getattr(self.data.pileup, pileupType), "dataset", dataset)
        setattr(self.data, "dbsUrl", dbsUrl)

    def setEventsPerLumi(self, eventsPerLumi):
        """
        _setEventsPerLumi_
        Add event per lumi information to the step, so it can be added later
        to the process, this comes from user input
        """
        if eventsPerLumi != None:
            setattr(self.data.application.configuration, "eventsPerLumi", eventsPerLumi)

    def getSkipBadFiles(self):
        """
        _getSkipBadFiles_

        Check if we can skip inexistent files instead of failing the job
        """
        return getattr(self.data.application.configuration, "skipBadFiles", False)

    def setSkipBadFiles(self, skipBadFiles):
        """
        _setSkipBadFiles_

        Add a flag to indicate the CMSSW process if we can
        skip inexistent files instead of failing the job
        """
        setattr(self.data.application.configuration, "skipBadFiles", skipBadFiles)

    def setNumberOfCores(self, ncores, nEventStreams=0):
        """
        _setNumberOfCores_

        Set the number of cores and event streams for CMSSW to run on
        """
        # if None is passed for EventStreams, then set it to 0
        nEventStreams = nEventStreams or 0

        self.data.application.multicore.numberOfCores = ncores
        self.data.application.multicore.eventStreams = nEventStreams

    def getNumberOfCores(self):
        """
        _getNumberOfCores_

        Get number of cores
        """
        return self.data.application.multicore.numberOfCores

    def getEventStreams(self):
        """
        _getEventStreams_

        Get number of event streams
        """
        return self.data.application.multicore.eventStreams

    def setGPUSettings(self, requiresGPU, gpuParams):
        """
        Set whether this CMSSW step should require GPUs and if so, which
        setup should be allowed and/or used
        """
        self.data.application.gpu.gpuRequired = requiresGPU
        self.data.application.gpu.gpuRequirements = gpuParams

    def getGPUSettings(self):
        """
        Return the GPU configuration for this CMSSW step
        :return: a tuple with:
          * string whether GPU is required or not
          * dictionary with the GPU requirements (or None)
        """
        return (self.data.application.gpu.gpuRequired,
                self.data.application.gpu.gpuRequirements)

    def getPhysicsTypeFromStepsArg(self, stepsArg, hasPileup=False, hasDatamix=False):
        """
        Get information about the step physics type, based on the
        cmsDriver command line --steps argument
        Reference: https://github.com/cms-sw/cmssw/issues/42587
        :param stepsArg: str with the cmsDriver step arguments
        :param hasPileup: bool, cmsDriver option
        :param hasDatamix: bool, cmsDriver option
        :return: str with all physics types, comman separated
        """
        physicsTypes = {"GEN":"GEN", "SIM":"SIM", "DIGI":"DIGI", "RECO":"RECO", "PAT":"MINIAOD", "NANO":"NANOAOD", "ALL":"GEN,SIM,DIGI,RECO"}
        stepTypes = []
        for stepType in physicsTypes.keys():
            for step in stepsArg.split(","):
                # Steps like DIGI could come in the form of DIGI:pdigi_valid
                step = step.split(':')[0]
                if step == stepType:
                    physicsType = physicsTypes.get(stepType)
                    if step == "DIGI":
                        if hasPileup:
                            if hasDatamix: 
                                physicsType = "DIGI_premix"
                            else:
                                physicsType = "DIGI_classicalmix"
                        else:
                            physicsType = "DIGI_nopileup"
                    stepTypes.append(physicsType)
        if not stepTypes:
            stepTypes.append("UNKNOWN")

        return ",".join(stepTypes)

    def getCmsDriverCommandLineArgs(self, conf):
        """
        Find the command line arguments in a pset config file
        :param conf: config document from configCache object
        :return: str with the cmsDriver command line arguments
        """
        # First, split all new lines
        conf = conf.split('\n')
        # Comment line 4 usually has the command line arguments
        # but find it otherwise (from the comment lines in the conf)
        cmdPattern = "# with command line options:"
        cmd = None
        if conf[4].startswith(cmdPattern):
            cmd = conf[4]
        else:
            for line in conf:
                if line.startswith(cmdPattern):
                    cmd = line
                elif not line.startswith('#'):
                    break
        # Replace "=" with spaces
        # E.g.: --step=DIGI -> --step DIGI
        if cmd:
            cmd = cmd.replace("=", " ")
    
        return cmd

    def determinePhysicsType(self, configCache=None):
        """
        Get information about the step physics type
        :param configCache: configCache object
        :return: str with the physics types, command separated.
        """
        hasPileup = False
        hasDatamix = False
        try:
            if configCache:
                # command arguments are in line 4
                myconf = configCache.getConfig()
                if myconf == None:
                    return "UNKNOWN"
                cmd = self.getCmsDriverCommandLineArgs(myconf)
                cmd = shlex.split(cmd)
                # If we are processing data,
                # just return "DataProcessing"
                if "--data" in cmd:
                    return "DataProcessing"
                # The command step arg is either '--step' or '-s'
                # Check for premixing and pileup parameters
                if "--pileup_input" in cmd:
                    hasPileup = True
                if "--datamix" in cmd:
                    hasDatamix = True
                try:
                    stepsArg = cmd[cmd.index("--step")+1]
                except ValueError:
                    stepsArg = cmd[cmd.index("-s")+1]
            else:
                stepsArg = "UNKNOWN"
        except (ConfigCacheException, AttributeError, ValueError) as ex:
            stepsArg = "UNKNOWN"

        return self.getPhysicsTypeFromStepsArg(stepsArg, hasPileup, hasDatamix)

    def getPhysicsType(self):
        """
        Set the physics type of the step
        :return: str
        """
        return getattr(self.data, "stepPhysicsType", None)

    def setPhysicsType(self, configCache=None):
        """
        Set the physics type of the step.
        :param configCache: configCache object
        """
        self.data.stepPhysicsType = self.determinePhysicsType(configCache)


class CMSSW(Template):
    """
    _CMSSW_

    Tools for creating a template CMSSW Step

    """

    def install(self, step):
        """
        _install_

        Add the set of default fields to the step required for running
        a cmssw job

        """
        stepname = nodeName(step)
        step.stepType = "CMSSW"
        step.application.section_("setup")
        step.application.setup.scramCommand = "scramv1"
        step.application.setup.scramProject = "CMSSW"
        step.application.setup.cmsswVersion = None
        step.application.setup.scramArch = None
        step.application.setup.buildArch = None
        step.application.setup.softwareEnvironment = None

        step.application.section_("command")
        step.application.command.executable = "cmsRun"
        step.application.command.configuration = "PSet.py"
        step.application.command.configurationPickle = "PSet.pkl"
        step.application.command.configurationHash = None
        step.application.command.psetTweak = None
        step.application.command.arguments = ""
        step.output.jobReport = "FrameworkJobReport.xml"
        step.output.stdout = "%s-stdout.log" % stepname
        step.output.stderr = "%s-stderr.log" % stepname
        step.output.keep = True
        step.output.section_("modules")

        step.output.section_("analysisFiles")

        step.section_("runtime")
        step.runtime.preScripts = []
        step.runtime.scramPreScripts = []
        step.runtime.postScripts = []
        step.runtime.postScramScripts = []

        step.section_("debug")
        step.debug.verbosity = 0
        step.debug.keepLogs = False

        step.section_("user")
        step.user.inputSandboxes = []
        step.user.script = None
        step.user.outputFiles = []
        step.user.userFiles = []
        step.user.lfnBase = None

        step.section_("monitoring")

        # support for multicore cmssw running mode
        step.application.section_("multicore")
        step.application.multicore.numberOfCores = 1
        step.application.multicore.eventStreams = 0

        # support for GPU in CMSSW (using defaults from StdBase)
        step.application.section_("gpu")
        step.application.gpu.gpuRequired = "forbidden"
        step.application.gpu.gpuRequirements = None

    def helper(self, step):
        """
        _helper_

        Wrap the WMStep provided in the CMSSW helper class that
        includes the ability to add and manipulate the details
        of a CMSSW workflow step

        """
        return CMSSWStepHelper(step)
