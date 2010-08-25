#!/usr/bin/env python
"""
_WMTask_

Object containing a set of executable Steps which form a template for a
set of jobs.

Equivalent of a WorkflowSpec in the ProdSystem

"""


__version__ = "$Id: WMTask.py,v 1.6 2009/05/22 16:04:49 evansde Exp $"
__revision__ = "$Revision: 1.6 $"


from WMCore.WMSpec.ConfigSectionTree import ConfigSectionTree, TreeHelper
from WMCore.WMSpec.WMStep import WMStep, WMStepHelper
import WMCore.WMSpec.Steps.StepFactory as StepFactory
from WMCore.WMSpec.Steps.BuildMaster import BuildMaster
from WMCore.WMSpec.Steps.ExecuteMaster import ExecuteMaster


def findTaskAboveNode(node):
    """
    _findTaskAboveNode_

    Given a config section (tree or not) traverse up the parent
    structure until finding the first entry containing an objectType
    setting that is set to WMTask

    """
    if getattr(node, "objectType", None) == "WMTask":
        return node
    if node._internal_parent_ref == None:
        return None
    return findTaskAboveNode(node._internal_parent_ref)

def getTaskFromStep(stepRef):
    """
    _getTaskFromStep_

    Traverse up the step tree until finding the first WMTask entry,
    return it wrapped in a WMTaskHelper

    """
    nodeData = stepRef
    if isinstance(stepRef, WMStepHelper):
        nodeData = stepRef.data

    taskNode = findTaskAboveNode(nodeData)
    if taskNode == None:
        msg = "Unable to find Task containing step\n"
        #TODO: Replace with real exception class
        raise RuntimeError, msg

    return WMTaskHelper(taskNode)



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


    def listAllStepNames(self):
        """
        _listAllStepNames_

        Get a list of all the step names contained in this task

        """
        step = self.steps()
        return step.allNodeNames()

    def getStep(self, stepName):
        """get a particular step from the workflow"""
        if self.data.steps.topStepName == None:
            return None
        topStep = self.steps()
        return topStep.getStep(stepName)

    def makeStep(self, stepName):
        """
        _makeStep_

        create a new WMStep instance, install it as the top step and
        return the reference to the new step wrapped in a StepHelper

        """
        newStep = WMStep(stepName)
        self.setStep(newStep)
        return WMStepHelper(newStep)


    def applyTemplates(self):
        """
        _applyTemplates_

        For each step, load the appropriate template and install the default structure

        TODO: Exception handling

        """
        for step in self.steps().nodeIterator():
            stepType = step.stepType
            template = StepFactory.getStepTemplate(stepType)
            template(step)

    def getStepHelper(self, stepName):
        """
        _getStepHelper_

        Get the named step, look up its type specific helper and retrieve
        the step wrapped in the type based helper.

        """
        step = self.getStep(stepName)
        stepType = step.stepType()
        template = StepFactory.getStepTemplate(stepType)
        helper = template.helper(step.data)
        return helper


    def build(self, workingDir):
        """
        _build_

        Invoke the build process to create the job in the working dir provided

        """
        master = BuildMaster(workingDir)
        master(self)
        return

    def execute(self, wmbsJob, emulator = None):
        """
        _execute_

        Invoke execution of the steps using an optional Emulator

        """
        master = ExecuteMaster(emulator)
        master(self, wmbsJob)
        return




class WMTask(ConfigSectionTree):
    """
    _WMTask_

    workload management task.
    Allow a set of processing job specifications that are interdependent
    to be modelled as a tree structure.

    """
    def __init__(self, name):
        ConfigSectionTree.__init__(self, name)
        self.objectType = self.__class__.__name__
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


