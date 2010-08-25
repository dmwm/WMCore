#!/usr/bin/env python
# pylint: disable-msg=E1101
"""
_WMStep_

Basic unit of executable work within a task.
Equivalent of a PayloadNode in the old production system WorkflowSpec

"""

__revision__ = "$Id: WMStep.py,v 1.3 2009/05/22 16:04:49 evansde Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMSpec.ConfigSectionTree import ConfigSectionTree, TreeHelper, nodeMap, getNode

class WMStepHelper(TreeHelper):
    """
    _WMStepHelper_

    Utils, methods and functions for manipulating the data in a WMStep

    """
    def __init__(self, stepInstance):
        TreeHelper.__init__(self, stepInstance)

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
