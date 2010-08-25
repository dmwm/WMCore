#!/usr/bin/env python
# pylint: disable-msg=E1101
"""
_WMStep_

Basic unit of executable work within a task.
Equivalent of a PayloadNode in the old production system WorkflowSpec

"""

__revision__ = "$Id: WMStep.py,v 1.5 2009/06/12 16:53:20 evansde Exp $"
__version__ = "$Revision: 1.5 $"

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

        Set the type of the step

        """
        self.data.stepType = stepType

    def stepType(self):
        """get stepType"""
        return self.data.stepType

    def getTask(self):
        """
        get Task instance that contains this Step
        """



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

    def getStep(self, stepName):
        """
        _getStep_

        Retrieve the named step and wrap it in a helper

        """
        node = self.getNode(stepName)
        if node == None:
            return None
        return WMStepHelper(node)

    def getTypeHelper(self):
        """
        _getTypeHelper_

        Get a step type specific helper for this object using the StepFactory

        """
        return getStepTypeHelper(self.data)


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





def makeWMStep(stepName):
    """
    _makeWMStep_

    Convienience method, instantiate a new WMStep with the name
    provided and wrap it in a helper

    """
    return WMStepHelper(WMStep(stepName))
