#!/usr/bin/env python
# pylint: disable-msg=E1101
"""
_WMStep_

Basic unit of executable work within a task.
Equivalent of a PayloadNode in the old production system WorkflowSpec

"""




from WMCore.WMSpec.ConfigSectionTree import ConfigSectionTree, TreeHelper, nodeMap, getNode
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

    def applicationSection(self):
        """get application ConfigSection ref"""
        return self.data.application

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

    def setPublishName(self, publishName):
        """
        _setPublishName_

        Set the publish data name for asynchronous stageout

        """
        self.data.publishName = publishName

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
