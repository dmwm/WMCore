#!/usr/bin/env python
# pylint: disable=E1101
"""
_WMStep_

Basic unit of executable work within a task.
Equivalent of a PayloadNode in the old production system WorkflowSpec

"""



from WMCore.WMSpec.ConfigSectionTree import ConfigSectionTree, TreeHelper
from WMCore.WMSpec.Steps.StepFactory import getStepTypeHelper


class WMStepHelper(TreeHelper):
    """
    _WMStepHelper_

    Utils, methods and functions for manipulating the data in a WMStep

    """
    def __init__(self, stepInstance):
        TreeHelper.__init__(self, stepInstance)

    def name(self):
        return self.data._internal_name

    def setStepType(self, stepType):
        """
        _setStepType_

        Set the type of the step.
        """
        self.data.stepType = stepType

    def stepType(self):
        """
        _stepType_

        Retrieve the step type.
        """
        return self.data.stepType

    def getNumberOfCores(self):
        """
        _getNumberOfCores_

        Return the number of cores for the step in question
        """
        try:
            return int(self.data.application.multicore.numberOfCores)
        except Exception:
            return 1

    def getNumberOfStreams(self):
        """
        _getNumberOfStreams_

        Return the number of event streams for the step in question
        """
        try:
            return int(self.data.application.multicore.eventStreams)
        except Exception:
            return 0

    def addStep(self, stepName):
        """
        _addStep_

        Add a new step with the name provided to this step as a child
        """
        node = WMStepHelper(WMStep(stepName))
        self.addNode(node)
        return node

    def addTopStep(self, stepName):
        """
        _addTopStep_

        Add a new step with the name provided to this step as a child.  This
        will be the first top step of all the children.
        """
        node = WMStepHelper(WMStep(stepName))
        self.addTopNode(node)
        return node

    def getStep(self, stepName):
        """
        _getStep_

        Retrieve the named step and wrap it in a helper

        """
        node = self.getNode(stepName)
        if node == None:
            return None
        return WMStepHelper(node)

    def setUserDN(self, userDN):
        """
        _setUserDN_

        Set the user DN

        """
        self.data.userDN = userDN

    def setAsyncDest(self, asyncDest):
        """
        _setAsyncDest_

        Set the async. destination

        """
        self.data.asyncDest = asyncDest

    def setUserRoleAndGroup(self, owner_vogroup, owner_vorole):
        """
        _setUserRoleAndGroup_

        Set user group and role.

        """
        self.data.owner_vogroup = owner_vogroup
        self.data.owner_vorole = owner_vorole

    def getTypeHelper(self):
        """
        _getTypeHelper_

        Get a step type specific helper for this object using the StepFactory

        """
        return getStepTypeHelper(self.data)


    def addOverride(self, override, overrideValue):
        """
        _addOverride_

        Add overrides for use in step executors
        """

        setattr(self.data.override, override, overrideValue)

        return



    def getOverrides(self):
        """
        _getOverrides_

        Get overrides for use in executors
        """

        return self.data.override.dictionary_()

    def getOutputModule(self, moduleName):
        """
        _getOutputModule_

        Get an output module from the step
        Return None if non-existant
        """

        if hasattr(self.data.output, 'modules'):
            if hasattr(self.data.output.modules, moduleName):
                return getattr(self.data.output.modules, moduleName)

        return None

    def setIgnoredOutputModules(self, moduleList):
        """
        _setIgnoredOutputModules_

        Set a list of output modules to be ignored,
        only CMSSW steps will use this
        """

        self.data.output.ignoredModules = moduleList
        return

    def setNewStageoutOverride(self, newValue):
        """
        A toggle for steps to use old or new stageout code
        """
        self.data.newStageout = newValue

    def getNewStageoutOverride(self):
        """
        A toggle for steps to use old or new stageout code
        """
        if hasattr(self.data, 'newStageout'):
            return self.data.newStageout
        else:
            return False

    def getIgnoredOutputModules(self):
        """
        _ignoreOutputModules_

        Get a list of output modules to be ignored,
        if the attribute is not set then return an empty list
        """

        if hasattr(self.data.output, 'ignoredModules'):
            return self.data.output.ignoredModules
        return []

    def getUserSandboxes(self):
        if hasattr(self.data, 'user'):
            if hasattr(self.data.user, 'inputSandboxes'):
                return self.data.user.inputSandboxes
        return []

    def getUserFiles(self):
        if hasattr(self.data, 'user'):
            if hasattr(self.data.user, 'userFiles'):
                return self.data.user.userFiles
        return []

    def setErrorDestinationStep(self, stepName):
        """
        _setErrorDestinationStep_

        In case of error, give the name of the step that
        the execute process should go to.
        """

        self.data.errorDestinationStep = stepName
        return

    def getErrorDestinationStep(self):
        """
        _getErrorDestinationStep_

        In case of error, get the step that should be
        next in the process
        """

        return getattr(self.data, 'errorDestinationStep', None)


    def getConfigInfo(self):
        """
        _getConfigInfo_

        Get information about the config cache location
        """

        cacheUrl = getattr(self.data.application.configuration, 'configCacheUrl', None)
        cacheDb  = getattr(self.data.application.configuration, 'cacheName', None)
        configId = getattr(self.data.application.configuration, 'configId', None)

        return cacheUrl, cacheDb, configId


    def listAnalysisFiles(self):
        """
        _listAnalysisFiles_

        retrieve list of output module names

        """
        if hasattr(self.data.output, "analysisFiles"):
            return self.data.output.analysisFiles.dictionary_().keys()

        return []


    def getAnalysisFile(self, name):
        """
        _getAnalysisFile_

        retrieve the data structure for an analysis file by name
        None if not found
        """
        return getattr(self.data.output.analysisFiles, name, None)

    def getConfigCacheID(self):
        """
        _getConfigCacheID_

        If we have a configCacheID return it, otherwise return None
        """
        return getattr(self.data.application.configuration, 'retrieveConfigUrl', None)



class WMStep(ConfigSectionTree):
    """
    _WMStep_

    Container for an executable unit within a Task

    """
    def __init__(self, name):
        ConfigSectionTree.__init__(self, name)
        self.objectType = self.__class__.__name__
        self.stepType = None

        self.section_("application")
        self.application.section_("controls")
        self.application.section_("configuration")

        self.section_("input")
        self.input.section_("links")
        self.section_("output")
        self.section_("sandbox")

        self.section_("emulator")
        self.section_("override")





def makeWMStep(stepName):
    """
    _makeWMStep_

    Convienience method, instantiate a new WMStep with the name
    provided and wrap it in a helper

    """
    return WMStepHelper(WMStep(stepName))
