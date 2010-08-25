#!/usr/bin/env python
"""
_WMTask_

Object containing a set of executable Steps which form a template for a
set of jobs.

Equivalent of a WorkflowSpec in the ProdSystem

"""


__version__ = "$Id: WMTask.py,v 1.12 2009/09/17 15:15:31 evansde Exp $"
__revision__ = "$Revision: 1.12 $"


from WMCore.WMSpec.ConfigSectionTree import ConfigSectionTree, TreeHelper
from WMCore.WMSpec.WMStep import WMStep, WMStepHelper
import WMCore.WMSpec.Steps.StepFactory as StepFactory
from WMCore.WMSpec.Steps.BuildMaster import BuildMaster
from WMCore.WMSpec.Steps.ExecuteMaster import ExecuteMaster
import WMCore.WMSpec.Utilities as SpecUtils


def getTaskFromStep(stepRef):
    """
    _getTaskFromStep_

    Traverse up the step tree until finding the first WMTask entry,
    return it wrapped in a WMTaskHelper

    """
    nodeData = stepRef
    if isinstance(stepRef, WMStepHelper):
        nodeData = stepRef.data

    taskNode = SpecUtils.findTaskAboveNode(nodeData)
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
        pName = "%s/%s" % (self.getPathName(), taskName)
        node.setPathName(pName)
        return node

    def setPathName(self, pathName):
        """
        _setPathName_

        Set the path name of the task within the workload
        Used internally when addin tasks to workloads or subtasks

        """
        self.data.pathName = pathName

    def getPathName(self):
        """
        _getPathName_

        get the path name of this task reflecting its
        structure within the workload and task tree

        """
        return self.data.pathName


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


    def setInputReference(self, stepRef, **extras):
        """
        _setInputReference_

        Add details to the input reference for the task providing
        input to this task.
        The reference is the step in the input task, plus
        any extra information.


        """
        stepId = SpecUtils.stepIdentifier(stepRef)
        setattr(self.data.input, "inputStep", stepId)
        [ setattr(self.data.input, key, val)
          for key, val in extras.items() ]
        return

    def inputReference(self):
        """
        _inputReference_

        Get information about the input reference for this task.

        """
        return self.data.input

    def setSplittingAlgorithm(self, algoName, **params):
        """
        _setSplittingAlgorithm_

        Set the splitting algorithm name and arguments

        """
        # Could consider checking values against the JobSplitting package
        # here...
        setattr(self.data.input.splitting, "algorithm", algoName)
        [ setattr(self.data.input.splitting, key, val)
          for key, val in params.items() ]

    def jobSplittingAlgorithm(self):
        """
        _jobSplittingAlgorithm_

        Get the job Splitting algo name

        """
        return getattr(self.data.input.splitting, "algorithm", None)

    def jobSplittingParameters(self):
        """
        _jobSplittingParameters_

        get the parameters to pass to the job splitting algo

        """
        datadict = getattr(self.data.input, "splitting")
        return datadict.dictionary_()

    def getSeederConfigs(self):
        """
        _getSeederConfigs_

        Returns a list of seeder config options in a dict with the seeder name as the key.
        """

        result = []

        if not hasattr(self.data, "seeders"):
            return result

        for seederName in self.data.seeders.listSections_():
            confValues = TreeHelper(getattr(self.data.seeders, seederName))
            args = {}
            tempArgs = confValues.pythoniseDict(sections = False)
            for entry in tempArgs.keys():
                args[entry.split('%s.' %seederName)[1]] = tempArgs[entry]
            result.append({seederName: args})

        return result


    def addSeeder(self, seederName, args = None):
        """
        _addSeeder_

        This SHOULD allow you to add a new seeder to the config section with any variable you want
        """

        if not 'seeders' in self.data.listSections_():
            self.data.section_('seeders')
        if not seederName in self.data.seeders.listSections_():
            self.data.seeders.section_(seederName)

        if args:
            helper = TreeHelper(getattr(self.data.seeders, seederName))
            helper.addValue(args)

        return

    def addInputDataset(self, **options):
        """
        _addInputDataset_

        Add details of an input dataset to this Task.
        This dataset will be used as input for the first step
        in the task

        options should contain at least:

        - primary - primary dataset name
        - processed - processed dataset name
        - tier - data tier name

        optional args:

        - analysis - analysis dataset path extension
        - dbsurl - dbs url if not global
        - block_whitelist - list of whitelisted fileblocks
        - block_blacklist - list of blacklisted fileblocks

        """
        self.data.input.section_("dataset")
        self.data.input.dataset.section_("blocks")
        self.data.input.dataset.blocks.whitelist = []
        self.data.input.dataset.blocks.blacklist = []

        primary = options.get("primary", None)
        processed = options.get("processed", None)
        tier = options.get("tier", None)

        if primary == None or processed == None or tier == None:
            msg = "Primary, Processed and Tier must be set"
            raise RuntimeError, msg

        self.data.input.dataset.primary = primary
        self.data.input.dataset.processed = processed
        self.data.input.dataset.tier = tier


        for opt, arg in options.items():
            # already handled/checked
            if opt in ['primary', 'processed', 'tier']: continue
            # blocks
            if opt == 'block_blacklist':
                self.data.input.dataset.blocks.blacklist = arg
                continue
            if opt == 'block_whitelist':
                self.data.input.dataset.blocks.whitelist = arg
                continue
            # all other options
            setattr(self.data.input.dataset, opt, arg)

        return

    def inputDataset(self):
        """
        _inputDataset_

        Get the input.dataset structure from this task

        """
        return getattr(self.data.input, "dataset", None)


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
        self.pathName = None
        self.section_("steps")
        self.steps.topStepName = None
        self.section_("parameters")
        self.section_("pythonLibs")
        self.section_("constraints")
        self.section_("input")
        self.input.section_("splitting")
        self.input.splitting.algorithm = None
        self.constraints.section_("sites")
        self.constraints.sites.whitelist = []
        self.constraints.sites.blacklist = []
        self.input.section_("WMBS")


def makeWMTask(taskName):
    """
    _makeWMTask_

    Convienience method to instantiate a new WMTask with the name
    provided and wrap it in a helper

    """
    return WMTaskHelper(WMTask(taskName))


