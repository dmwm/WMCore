#!/usr/bin/env python
"""
_WMTask_

Object containing a set of executable Steps which form a template for a
set of jobs.

Equivalent of a WorkflowSpec in the ProdSystem

"""


__version__ = "$Id: WMTask.py,v 1.19 2010/01/12 19:59:57 evansde Exp $"
__revision__ = "$Revision: 1.19 $"

import os

from WMCore.WMSpec.ConfigSectionTree import ConfigSectionTree, TreeHelper
from WMCore.WMSpec.WMStep import WMStep, WMStepHelper
import WMCore.WMSpec.Steps.StepFactory as StepFactory
from WMCore.WMSpec.Steps.BuildMaster import BuildMaster
from WMCore.WMSpec.Steps.ExecuteMaster import ExecuteMaster
import WMCore.WMSpec.Utilities as SpecUtils
from WMCore.DataStructs.Workflow import Workflow as DataStructsWorkflow

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

    def taskIterator(self):
        """
        _taskIterator_

        return output of nodeIterator(self) wrapped in TaskHelper instance

        """
        for x in self.nodeIterator():
            yield WMTaskHelper(x)


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

    def makeWorkflow(self):
        """
        _makeWorkflow_

        Create a WMBS compatible Workflow structure that represents this
        task and the information contained within it

        """
        workflow = DataStructsWorkflow()
        workflow.task = self.getPathName()
        return workflow





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

    def setupEnvironment(self):
        """
        _setupEnvironment_
        
        I don't know if this should go here.
        Setup the environment variables mandated in the WMTask
        """
        
        if not hasattr(self.data, 'environment'):
            #No environment to setup, pass
            return

        envDict = self.data.environment.dictionary_()
        
        for key in envDict.keys():
            if str(envDict[key].__class__) == "<class 'WMCore.Configuration.ConfigSection'>":
                #At this point we do not support the setting of sub-sections for environment variables
                continue
            else:
                os.environ[key] = envDict[key]

        return

    def execute(self, wmbsJob, emulator = None):
        """
        _execute_

        Invoke execution of the steps using an optional Emulator

        TODO: emulator is now deprecated, remove from API 

        """
        self.setupEnvironment()
        master = ExecuteMaster()
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


    def addGenerator(self, generatorName, **settings):
        """
        _addGenerator_


        """
        if not 'generators' in self.data.listSections_():
            self.data.section_('generators')
        if not generatorName in self.data.generators.listSections_():
            self.data.generators.section_(generatorName)


        helper = TreeHelper(getattr(self.data.generators, generatorName))
        helper.addValue(settings)

        return


    def listGenerators(self):
        generators = getattr(self.data, "generators", None)
        if generators == None:
            return []
        return generators.listSections_()


    def getGeneratorSettings(self, generatorName):
        generators = getattr(self.data, "generators", None)
        if generators == None:
            return {}
        generator = getattr(generators, generatorName, None)
        if generator == None:
            return {}

        confValues = TreeHelper(generator)
        args = {}
        tempArgs = confValues.pythoniseDict(sections = False)
        for entry in tempArgs.keys():
            args[entry.split('%s.' %generatorName)[1]] = tempArgs[entry]
        return args

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
        - totalevents - total events in dataset

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
            if opt == 'dbsurl':
                self.data.input.dataset.dbsurl = arg
            # all other options

            setattr(self.data.input.dataset, opt, arg)

        return

    def addProduction(self, **options):
        """
        _addProduction_

        Add details of production job related information.

        options should contain at least:
        TODO: Not sure what is necessary data ask Dave
        optional
        - totalevents - total events in dataset

        """
        self.data.section_("production")

        for opt, arg in options.items():
            if opt == 'totalevents':
                self.data.production.totalEvents = arg

            setattr(self.data.production, opt, arg)

    def inputDataset(self):
        """
        _inputDataset_

        Get the input.dataset structure from this task

        """
        return getattr(self.data.input, "dataset", None)

    def siteWhitelist(self):
        """
        _siteWhitelist_

        accessor for white list for the task
        """

        return self.data.constraints.sites.whitelist

    def siteBlacklist(self):
        """
        _siteBlacklist_

        accessor for white list for the task
        """

        return self.data.constraints.sites.blacklist

    def parentProcessingFlag(self):
        """
        _parentProcessingFlag_

        accessor for parentProcessing information (two file input)
        """
        return getattr(self.data.input.dataset, "parentFlag", False)

    def totalEvents(self):
        """
        _totalEvents_

        accessor for total events in the given dataset
        """
        #TODO: save the total events for  the production job
        return self.data.production.totalEvents
        #return self.data.input.dataset.totalEvents

    def dbsUrl(self):
        """
        _dbsUrl_
        if local dbs url is set for the task, return it
        otherwise return None
        """
        if getattr(self.data.input, "dataset", False):
            return getattr(self.data.input.dataset, "dbsurl", None)
        else:
            return None

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


