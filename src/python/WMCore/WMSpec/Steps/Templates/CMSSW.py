#!/usr/bin/env python
"""
_CMSSW_

Template for a CMSSW Step

"""

import pickle

from WMCore.WMSpec.Steps.Template import Template
from WMCore.WMSpec.Steps.Template import CoreHelper
from WMCore.WMSpec.ConfigSectionTree import nodeName
from WMCore.WMSpec.ConfigSectionTree import ConfigSectionTree



class CMSSWStepHelper(CoreHelper):
    """
    _CMSSWStepHelper_

    Add API calls and helper methods to the basic WMStepHelper to specialise
    for CMSSW tasks

    """


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

        for key, value in details.items():
            setattr(module, key, value)

        return


    def addAnalysisFile(self, fileLabel, **details):
        """
        _addAnalysisFile_

        Add in an additional file produced by the user to be staged out
        """
        analysisFiles = self.data.output.analysisFiles

        if getattr(analysisFiles, fileLabel, None) == None:
            analysisFiles.section_(fileLabel)
        analysisFile = getattr(analysisFiles, fileLabel)

        for key, value in details.items():
            setattr(analysisFile, key, value)

        return


    def listOutputModules(self):
        """
        _listOutputModules_

        retrieve list of output module names

        """
        if hasattr(self.data.output, "modules"):
            return self.data.output.modules.dictionary_().keys()

        return []

    def getOutputModule(self, name):
        """
        _getOutputModule_

        retrieve the data structure for an output module by name
        None if not found

        """
        return getattr(self.data.output.modules, name, None)


    def setConfigCache(self, url, document, dbName = "config_cache"):
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
        self.data.application.configuration.retrieveConfigUrl = \
                                        "%s/configFile" % docUrl



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
            [ setattr(self.data.application.configuration.arguments, k, v)
              for k, v in args.items() ]
        except Exception:
            pass
        self.data.application.configuration.pickledarguments = pickle.dumps(args)
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
        for k,v in options.items():
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
            args = pickle.loads(self.data.application.configuration.pickledarguments)
        args['globalTag'] = globalTag
        self.data.application.configuration.pickledarguments = pickle.dumps(args)

        return

    def getGlobalTag(self):
        """
        _getGlobalTag_

        Retrieve the global tag.
        """
        if hasattr(self.data.application.configuration, "arguments"):
            if hasattr(self.data.application.configuration.arguments, "globalTag"):
                return self.data.application.configuration.arguments.globalTag

        return pickle.loads(self.data.application.configuration.pickledarguments)['globalTag']

    def setDatasetName(self, datasetName):
        """
        _setDatasetName_

        Set the dataset name in the pickled arguments
        """
        self.data.application.configuration.section_('arguments')
        self.data.application.configuration.arguments.datasetName = datasetName

        args = {}
        if hasattr(self.data.application.configuration, "pickledarguments"):
            args = pickle.loads(self.data.application.configuration.pickledarguments)
        args['datasetName'] = datasetName
        self.data.application.configuration.pickledarguments = pickle.dumps(args)

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
        for pileupType, dataset in pileupConfig.items():
            self.data.pileup.section_(pileupType)
            setattr(getattr(self.data.pileup, pileupType), "dataset", dataset)
        setattr(self.data, "dbsUrl", dbsUrl)

    def setOverrideCatalog(self, overrideCatalog):
        """
        _setOverrideCatalog_

        set the override catalog

        needed at least at CERN to use production castor pools

        """
        if overrideCatalog != None:
            self.data.application.overrideCatalog = overrideCatalog

    def setEventsPerLumi(self, eventsPerLumi):
        """
        _setEventsPerLumi_
        Add event per lumi information to the step, so it can be added later
        to the process, this comes from user input
        """
        if eventsPerLumi != None:
            setattr(self.data.application.configuration, "eventsPerLumi",
                    eventsPerLumi)

    def getSkipBadFiles(self):
        """
        _getSkipBadFiles_

        Check if we can skip inexistent files instead of failing the job
        """
        return getattr(self.data.application.configuration, "skipBadFiles",
                False)

    def setSkipBadFiles(self, skipBadFiles):
        """
        _setSkipBadFiles_

        Add a flag to indicate the CMSSW process if we can
        skip inexistent files instead of failing the job
        """
        setattr(self.data.application.configuration, "skipBadFiles",
                skipBadFiles)

    def setMulticoreCores(self, ncores):
        """
        _setMulticoreCores_

        Preset the number of cores for CMSSW to run on, expect this to dribble away
        as batch systems get better at dynamic discovery etc, or be used as an override for
        testing
        """
        self.data.application.multicore.numberOfCores = ncores
        self.data.application.multicore.enabled = True

    def numberOfCores(self):
        """
        _numberOfCores_

        Get number of cores
        """
        return self.data.application.multicore.numberOfCores

    def multicoreEnabled(self):
        """
        _multicoreEnabled_

        True/False flag to determine wether multicore is enabled
        """
        return self.data.application.multicore.enabled


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

        step.section_("monitoring")

        #
        # support for multicore cmssw running mode
        #
        step.application.section_("multicore")
        step.application.multicore.enabled =  False
        step.application.multicore.numberOfCores = 1
        step.application.multicore.inputfilelist = "input.filelist"
        step.application.multicore.inputmanifest = "manifest.json"
        step.application.multicore.edmFileUtil = "edmFileUtil --JSON -F input.filelist > manifest.json"


    def helper(self, step):
        """
        _helper_

        Wrap the WMStep provided in the CMSSW helper class that
        includes the ability to add and manipulate the details
        of a CMSSW workflow step

        """
        return CMSSWStepHelper(step)
