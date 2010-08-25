#!/usr/bin/env python
"""
_WMTask_

Object containing a set of executable Steps which form a template for a
set of jobs.

Equivalent of a WorkflowSpec in the ProdSystem

"""


__version__ = "$Id: WMTask.py,v 1.1 2009/05/08 15:32:32 sryu Exp $"
__revision__ = "$Revision: 1.1 $"


from WMCore.WMSpec.ConfigSectionTree import ConfigSectionTree, TreeHelper
from WMCore.WMSpec.WMStep import WMStep, WMStepHelper



class WMTaskHelper(TreeHelper):
    """
    _WMTaskHelper_

    Util wrapper containing tools & methods for manipulating the WMTask
    data object

    """
    def __init__(self, wmTask):
        TreeHelper.__init__(self, wmTask)


    def addTask(self, taskName):
        """
        _addTask_

        Add a new task as a subtask with the name provided and
        return it wrapped in a TaskHelper

        """
        node = WMTaskHelper(WMTask(taskName))
        self.addNode(node)
        return node


    def steps(self):
        """get WMStep structure"""
        if self.data.steps.topStepName == None:
            return None
        step = getattr(self.data.steps, self.data.steps.topStepName, None)
        return WMStepHelper(step)

    def setStep(self, wmStep):
        """set topStep to be the step instance provided"""
        stepData = wmStep
        if isinstance(wmStep, WMStepHelper):
            stepData = wmStep.data
            stepHelper = wmStep
        else:
            stepHelper = WMStepHelper(wmStep)

        stepName = stepHelper.name()
        stepHelper.setTopOfTree()
        setattr(self.data.steps, stepName, stepData)
        setattr(self.data.steps, "topStepName" ,stepName)
        return


    def getStep(self, stepName):
        """get a particular step from the workflow"""
        if self.data.steps.topStepName == None:
            return None
        topStep = self.steps()
        return topStep.getStep(stepName)


class WMTask(ConfigSectionTree):
    """
    _WMTask_

    workload management task.
    Allow a set of processing job specifications that are interdependent
    to be modelled as a tree structure.

    """
    def __init__(self, name):
        ConfigSectionTree.__init__(self, name)

        self.section_("steps")
        self.steps.topStepName = None
        self.section_("parameters")
        self.section_("pythonLibs")
        self.section_("constraints")
        self.constraints.section_("sites")
        self.constraints.sites.whitelist = []
        self.constraints.sites.blacklist = []


def makeWMTask(taskName):
    """
    _makeWMTask_

    Convienience method to instantiate a new WMTask with the name
    provided and wrap it in a helper

    """
    return WMTaskHelper(WMTask(taskName))


