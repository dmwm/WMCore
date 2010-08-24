#!/usr/bin/env python
# pylint: disable-msg=E1101
"""
_WMStep_

Basic unit of executable work within a task.
Equivalent of a PayloadNode in the old production system WorkflowSpec

"""

__revision__ = "$Id: WMStep.py,v 1.1 2009/02/04 20:21:56 evansde Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMSpec.ConfigSectionTree import ConfigSectionTree, TreeHelper

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


class WMStep(ConfigSectionTree):
    """
    _WMStep_

    Container for an executable unit within a Task

    """
    def __init__(self, name):
        ConfigSectionTree.__init__(self, name)
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
