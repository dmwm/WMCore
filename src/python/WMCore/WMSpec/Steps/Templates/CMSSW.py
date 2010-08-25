#!/usr/bin/env python
"""
_CMSSW_

Template for a CMSSW Step

"""

from WMCore.WMSpec.Steps.Template import Template
from WMCore.WMSpec.Steps.Template import CoreHelper
from WMCore.WMSpec.ConfigSectionTree import nodeName



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

    def listOutputModules(self):
        """
        _listOutputModules_

        retrieve list of output module names

        """
        return self.data.output.modules.dictionary_().keys()

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
                                        "%s/original_script" % docUrl



    def setDataProcessingConfig(self, scenarioName, functionName, **args):
        """
        _setDataProcessingConfig_

        Set a configuration library to be used from the CMSSW Release
        DataProcessing package.

        """
        self.data.application.configuration.scenario = scenarioName
        self.data.application.configuration.function = functionName
        self.data.application.configuration.section_('arguments')
        [ setattr(self.data.application.configuration.arguments, k, v)
          for k, v in args.items() ]
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

    def getCMSSWVersion(self):
        """
        _getCMSSWVersion_

        Retrieve the version of the framework used for this step.
        """
        return self.data.application.setup.cmsswVersion
    
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
        step.application.command.configurationHash = None
        step.application.command.psetTweak = None
        step.application.command.arguments = ""
        step.output.jobReport = "FrameworkJobReport.xml"
        step.output.stdout = "%s-stdout.log" % stepname
        step.output.stderr = "%s-stderr.log" % stepname
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

        step.section_("monitoring")




    def helper(self, step):
        """
        _helper_

        Wrap the WMStep provided in the CMSSW helper class that
        includes the ability to add and manipulate the details
        of a CMSSW workflow step

        """
        return CMSSWStepHelper(step)

