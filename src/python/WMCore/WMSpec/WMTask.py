#!/usr/bin/env python
# pylint: disable=W0212
# W0212 (protected-access): Access to protected names of a client class.
"""
_WMTask_

Object containing a set of executable Steps which form a template for a
set of jobs.

Equivalent of a WorkflowSpec in the ProdSystem.
"""
import json
from builtins import map, zip, str as newstr, bytes
from future.utils import viewitems

import logging
import os.path
import time

import WMCore.WMSpec.Steps.StepFactory as StepFactory
import WMCore.WMSpec.Utilities as SpecUtils
from WMCore.Configuration import ConfigSection
from WMCore.DataStructs.LumiList import LumiList
from WMCore.DataStructs.Workflow import Workflow as DataStructsWorkflow
from WMCore.Lexicon import lfnBase
from WMCore.WMSpec.ConfigSectionTree import ConfigSectionTree, TreeHelper
from WMCore.WMSpec.Steps.BuildMaster import BuildMaster
from WMCore.WMSpec.Steps.ExecuteMaster import ExecuteMaster
from WMCore.WMSpec.Steps.Template import CoreHelper
from WMCore.WMSpec.WMStep import WMStep, WMStepHelper


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
    if taskNode is None:
        msg = "Unable to find Task containing step\n"
        # TODO: Replace with real exception class
        raise RuntimeError(msg)

    return WMTaskHelper(taskNode)


def buildLumiMask(runs, lumis):
    """
    Runs are saved in the spec as a list of integers.
    The lumi mask associated to each run is saved as a list of strings
    where each string is in a format like '1,4,23,45'

    The method convert these parameters in the corresponding lumiMask,
    e.g.:  runs=['3','4'], lumis=['1,4,23,45', '5,84,234,445'] => lumiMask = {'3':[[1,4],[23,45]],'4':[[5,84],[234,445]]}
    """

    if len(runs) != len(lumis):
        raise ValueError("runs and lumis must have same length")
    for lumi in lumis:
        if len(lumi.split(',')) % 2:
            raise ValueError("Needs an even number of lumi in each element of lumis list")

    lumiLists = [list(map(list, list(zip([int(y) for y in x.split(',')][::2], [int(y) for y in x.split(',')][1::2]))))
                 for x
                 in lumis]
    strRuns = [str(run) for run in runs]

    lumiMask = dict(list(zip(strRuns, lumiLists)))

    return lumiMask


class WMTaskHelper(TreeHelper):
    """
    _WMTaskHelper_

    Util wrapper containing tools & methods for manipulating the WMTask
    data object.
    """

    def __init__(self, wmTask):
        TreeHelper.__init__(self, wmTask)
        self.startTime = None
        self.endTime = None
        self.monitoring = None

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

    def childTaskIterator(self):
        """
        _childTaskIterator_

        Iterate over all the first generation child tasks.
        """
        for x in self.firstGenNodeChildIterator():
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

    def name(self):
        """
        _name_

        Retrieve the name of this task.
        """
        return self.data._internal_name

    def listPathNames(self):
        """
        _listPathNames_

        """
        for t in self.taskIterator():
            yield t.getPathName()

    def listNames(self):
        """
        _listNames_
        Returns a generator with the name of all the children tasks
        """
        for t in self.taskIterator():
            yield t.name()

    def listChildNames(self):
        """
        _listChildNames_
        Return a list with the name of the first generation children tasks
        """
        names = []
        for t in self.childTaskIterator():
            names.append(t.name())
        return names

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
        if self.data.steps.topStepName is None:
            return None
        step = getattr(self.data.steps, self.data.steps.topStepName, None)
        return WMStepHelper(step)

    def getTopStepName(self):
        """
        _getTopStepName_

        Retrieve the name of the top step.
        """
        return self.data.steps.topStepName

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
        setattr(self.data.steps, "topStepName", stepName)
        return

    def listAllStepNames(self, cmsRunOnly=False):
        """
        _listAllStepNames_

        Get a list of all the step names contained in this task.
        """
        step = self.steps()
        if step:
            stepNames = step.allNodeNames()
            if cmsRunOnly:
                stepNames = [step for step in stepNames if step.startswith("cmsRun")]
            return stepNames
        else:
            return []

    def getStep(self, stepName):
        """get a particular step from the workflow"""
        if self.data.steps.topStepName is None:
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

    def getOutputModulesForTask(self, cmsRunOnly=False):
        """
        _getOutputModulesForTask_

        Retrieve all the output modules in the given task.
        If cmsRunOnly is set to True, then return the output modules for
        cmsRun steps only.
        """
        outputModules = []
        for stepName in self.listAllStepNames(cmsRunOnly):
            outputModules.append(self.getOutputModulesForStep(stepName))
        return outputModules

    def getIgnoredOutputModulesForTask(self):
        """
        _getIgnoredOutputModulesForTask_

        Retrieve the ignored output modules in the given task.
        """
        ignoredOutputModules = []
        for stepName in self.listAllStepNames():
            stepHelper = self.getStepHelper(stepName)
            ignoredOutputModules.extend(stepHelper.getIgnoredOutputModules())
        return ignoredOutputModules

    def getOutputModulesForStep(self, stepName):
        """
        _getOutputModulesForStep_

        Retrieve all the output modules for the particular step.
        """
        step = self.getStep(stepName)

        if hasattr(step.data, "output"):
            if hasattr(step.data.output, "modules"):
                return step.data.output.modules

        return ConfigSection()

    def build(self, workingDir):
        """
        _build_

        Invoke the build process to create the job in the working dir provided

        """
        master = BuildMaster(workingDir)
        master(self)
        return

    def addEnvironmentVariables(self, envDict):
        """
        _addEnvironmentVariables_

        add a key = value style setting to the environment for this task and all
        its children
        """
        for key, value in viewitems(envDict):
            setattr(self.data.environment, key, value)
        for task in self.childTaskIterator():
            task.addEnvironmentVariables(envDict)
        return

    def setOverrideCatalog(self, tfcFile):
        """
        _setOverrideCatalog_

        Used for setting overrideCatalog option for each step in the task.
        """
        for step in self.steps().nodeIterator():
            step = CoreHelper(step)
            step.setOverrideCatalog(tfcFile)
        for task in self.childTaskIterator():
            task.setOverrideCatalog(tfcFile)
        return

    def getEnvironmentVariables(self):
        """
        _getEnvironmentVariables_

        Retrieve a dictionary with all environment variables defined for this task
        """
        return self.data.environment.dictionary_()

    def setupEnvironment(self):
        """
        _setupEnvironment_

        I don't know if this should go here.
        Setup the environment variables mandated in the WMTask
        """

        if not hasattr(self.data, 'environment'):
            # No environment to setup, pass
            return

        envDict = self.data.environment.dictionary_()

        for key in envDict:
            if str(envDict[key].__class__) == "<class 'WMCore.Configuration.ConfigSection'>":
                # At this point we do not support the
                # setting of sub-sections for environment variables
                continue
            else:
                os.environ[key] = envDict[key]

        return

    def execute(self, wmbsJob):
        """
        _execute_

        Invoke execution of the steps

        """
        self.startTime = time.time()
        self.setupEnvironment()
        master = ExecuteMaster()
        master(self, wmbsJob)
        self.endTime = time.time()
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
        for key, val in viewitems(extras):
            setattr(self.data.input, key, val)

        return

    def setInputStep(self, stepName):
        """
        _setInputStep_

        Set the name of the step used who's output is used as input for this
        task.
        """
        self.data.input.inputStep = stepName
        return

    def getInputStep(self):
        """
        _getInputStep_

        Retrieve the name of the input step, if there is one.
        """
        return getattr(self.data.input, "inputStep", None)

    def inputReference(self):
        """
        _inputReference_

        Get information about the input reference for this task.

        """
        return self.data.input

    def setFirstEventAndLumi(self, firstEvent, firstLumi):
        """
        _setFirstEventAndLumi_

        Set an arbitrary first event and first lumi
        Only used by production workflows
        """

        if not hasattr(self.data, "production"):
            self.data._section("production")
        setattr(self.data.production, "firstEvent", firstEvent)
        setattr(self.data.production, "firstLumi", firstLumi)

    def getFirstEvent(self):
        """
        _getFirstEvent_

        Get first event to produce for the task
        """
        if hasattr(self.data, "production"):
            if hasattr(self.data.production, "firstLumi"):
                return self.data.production.firstEvent
        return 1

    def getFirstLumi(self):
        """
        _getFirstLumi_

        Get first lumi to produce for the task
        """
        if hasattr(self.data, "production"):
            if hasattr(self.data.production, "firstLumi"):
                return self.data.production.firstLumi
        return 1

    def setSplittingParameters(self, **params):
        """
        _setSplittingParameters_

        Set the job splitting parameters.
        """
        for key, val in viewitems(params):
            setattr(self.data.input.splitting, key, val)

        return

    def setSplittingAlgorithm(self, algoName, **params):
        """
        _setSplittingAlgorithm_

        Set the splitting algorithm name and arguments.  Clear out any old
        splitting parameters while preserving the parameters for ACDC
        resubmission which are:
          collectionName, filesetName, couchURL, couchDB, owner, group

        This also needs to preserve the parameter we use to set the initial
        LFN counter, whether or not we merge across runs and the runWhitelist:
          initial_lfn_counter
          merge_across_runs
          runWhitelist

        Preserve parameters which can be set up at request creation and if not
        specified should remain unchanged, at the moment these are:
            include_parents
            lheInputFiles

        Also preserve the performance section.
        """
        setACDCParams = {}
        for paramName in ["collectionName", "filesetName", "couchURL",
                          "couchDB", "owner", "group", "initial_lfn_counter",
                          "merge_across_runs", "runWhitelist"]:
            if hasattr(self.data.input.splitting, paramName):
                setACDCParams[paramName] = getattr(self.data.input.splitting,
                                                   paramName)
        preservedParams = {}
        for paramName in ["lheInputFiles", "include_parents", "deterministicPileup"]:
            if hasattr(self.data.input.splitting, paramName):
                preservedParams[paramName] = getattr(self.data.input.splitting,
                                                     paramName)
        performanceConfig = getattr(self.data.input.splitting, "performance", None)

        delattr(self.data.input, "splitting")
        self.data.input.section_("splitting")
        self.data.input.splitting.section_("performance")

        setattr(self.data.input.splitting, "algorithm", algoName)
        self.setSplittingParameters(**preservedParams)
        self.setSplittingParameters(**params)
        self.setSplittingParameters(**setACDCParams)
        if performanceConfig is not None:
            self.data.input.splitting.performance = performanceConfig
        return

    def updateSplittingParameters(self, algoName, **params):
        """
        _updateSplittingAlgorithm_
        :param algoName: string Algorithm name
        :param params: splitting parameters
        :return:

        Only updates specific parameters in splitting Algorithm but doesn't remove the existing splitting parameters
        """
        performanceConfig = getattr(self.data.input.splitting, "performance", None)
        setattr(self.data.input.splitting, "algorithm", algoName)
        self.data.input.splitting.section_("performance")
        self.setSplittingParameters(**params)
        if performanceConfig is not None:
            self.data.input.splitting.performance = performanceConfig
        return

    def jobSplittingAlgorithm(self):
        """
        _jobSplittingAlgorithm_

        Retrieve the job splitting algorithm name.
        """
        return getattr(self.data.input.splitting, "algorithm", None)

    def jobSplittingParameters(self, performance=True):
        """
        _jobSplittingParameters_

        Retrieve the job splitting parameters.  This will combine the job
        splitting parameters specified in the spec with the site white list
        and black list as those are passed to the job splitting code.
        If required, also extract the performance parameters and pass them in the dict.
        """
        datadict = getattr(self.data.input, "splitting")
        if performance:
            splittingParams = datadict.dictionary_whole_tree_()
        else:
            splittingParams = datadict.dictionary_()
            if "performance" in splittingParams:
                del splittingParams['performance']
        splittingParams["siteWhitelist"] = self.siteWhitelist()
        splittingParams["siteBlacklist"] = self.siteBlacklist()
        splittingParams["trustSitelists"] = self.getTrustSitelists().get('trustlists')
        splittingParams["trustPUSitelists"] = self.getTrustSitelists().get('trustPUlists')

        if "runWhitelist" not in splittingParams and self.inputRunWhitelist() is not None:
            splittingParams["runWhitelist"] = self.inputRunWhitelist()
        if "runBlacklist" not in splittingParams and self.inputRunBlacklist() is not None:
            splittingParams["runBlacklist"] = self.inputRunBlacklist()

        return splittingParams

    def setJobResourceInformation(self, timePerEvent=None, sizePerEvent=None, memoryReq=None):
        """
        _setJobResourceInformation_

        Set the values to estimate the required computing resources for a job,
        the three key values are main memory usage, time per processing unit (e.g. time per event) and
        disk usage per processing unit (e.g. size per event).
        """
        if self.taskType() in ["Merge", "Cleanup", "LogCollect"]:
            # don't touch job requirements for these task types
            return

        performanceParams = getattr(self.data.input.splitting, "performance")

        timePerEvent = timePerEvent.get(self.name()) if isinstance(timePerEvent, dict) else timePerEvent
        sizePerEvent = sizePerEvent.get(self.name()) if isinstance(sizePerEvent, dict) else sizePerEvent
        memoryReq = memoryReq.get(self.name()) if isinstance(memoryReq, dict) else memoryReq

        if timePerEvent or getattr(performanceParams, "timePerEvent", None):
            performanceParams.timePerEvent = timePerEvent or getattr(performanceParams, "timePerEvent")
        if sizePerEvent or getattr(performanceParams, "sizePerEvent", None):
            performanceParams.sizePerEvent = sizePerEvent or getattr(performanceParams, "sizePerEvent")
        if memoryReq or getattr(performanceParams, "memoryRequirement", None):
            performanceParams.memoryRequirement = memoryReq or getattr(performanceParams, "memoryRequirement")
            # if we change memory requirements, then we must change MaxPSS as well
            self.setMaxPSS(performanceParams.memoryRequirement)

        return

    def addGenerator(self, generatorName, **settings):
        """
        _addGenerator_


        """
        if 'generators' not in self.data.listSections_():
            self.data.section_('generators')
        if generatorName not in self.data.generators.listSections_():
            self.data.generators.section_(generatorName)

        helper = TreeHelper(getattr(self.data.generators, generatorName))
        helper.addValue(settings)

        return

    def listGenerators(self):
        """
        _listGenerators_

        """
        generators = getattr(self.data, "generators", None)
        if generators is None:
            return []
        return generators.listSections_()

    def getGeneratorSettings(self, generatorName):
        """
        _getGeneratorSettings_

        Extract the settings from the generator fields
        """
        generators = getattr(self.data, "generators", None)
        if generators is None:
            return {}
        generator = getattr(generators, generatorName, None)
        if generator is None:
            return {}

        confValues = TreeHelper(generator)
        args = {}
        tempArgs = confValues.pythoniseDict(sections=False)
        for entry in tempArgs:
            args[entry.split('%s.' % generatorName)[1]] = tempArgs[entry]
        return args

    def addInputACDC(self, serverUrl, databaseName, collectionName,
                     filesetName):
        """
        _addInputACDC_

        Set the ACDC input information for this task.
        """
        self.data.input.section_("acdc")
        self.data.input.acdc.server = serverUrl
        self.data.input.acdc.database = databaseName
        self.data.input.acdc.collection = collectionName
        self.data.input.acdc.fileset = filesetName
        return

    def getInputACDC(self):
        """
        _getInputACDC_

        Retrieve the ACDC input configuration.
        """
        if not hasattr(self.data.input, "acdc"):
            return None

        return {"server": self.data.input.acdc.server,
                "collection": self.data.input.acdc.collection,
                "fileset": self.data.input.acdc.fileset,
                "database": self.data.input.acdc.database}

    def addInputDataset(self, **options):
        """
        _addInputDataset_

        Add details of an input dataset to this Task.
        This dataset will be used as input for the first step
        in the task

        options should contain at least:
          - name - dataset name
          - primary - primary dataset name
          - processed - processed dataset name
          - tier - data tier name

        optional args:
          - dbsurl - dbs url if not global
          - block_whitelist - list of whitelisted fileblocks
          - block_blacklist - list of blacklisted fileblocks
          - run_whitelist - list of whitelist runs
          - run_blacklist - list of blacklist runs
        """
        self.data.input.section_("dataset")
        self.data.input.dataset.name = None
        self.data.input.dataset.dbsurl = None
        self.data.input.dataset.section_("blocks")
        self.data.input.dataset.blocks.whitelist = []
        self.data.input.dataset.blocks.blacklist = []
        self.data.input.dataset.section_("runs")
        self.data.input.dataset.runs.whitelist = []
        self.data.input.dataset.runs.blacklist = []

        try:
            self.data.input.dataset.primary = options.pop('primary')
            self.data.input.dataset.processed = options.pop('processed')
            self.data.input.dataset.tier = options.pop('tier')
        except KeyError:
            raise RuntimeError("Primary, Processed and Tier must be set")

        for opt, arg in viewitems(options):
            if opt == 'block_blacklist':
                self.setInputBlockBlacklist(arg)
            elif opt == 'block_whitelist':
                self.setInputBlockWhitelist(arg)
            elif opt == 'dbsurl':
                self.data.input.dataset.dbsurl = arg
            elif opt == "run_whitelist":
                self.setInputRunWhitelist(arg)
            elif opt == "run_blacklist":
                self.setInputRunBlacklist(arg)
            else:
                setattr(self.data.input.dataset, opt, arg)

        return

    def setInputBlockWhitelist(self, blockWhitelist):
        """
        _setInputBlockWhitelist_

        Set the block white list for the input dataset.  This must be called
        after setInputDataset().
        """
        self.data.input.dataset.blocks.whitelist = blockWhitelist
        return

    def inputBlockWhitelist(self):
        """
        _inputBlockWhitelist_

        Retrieve the block white list for the input dataset if it exists, none
        otherwise.
        """
        if hasattr(self.data.input, "dataset"):
            return self.data.input.dataset.blocks.whitelist
        return None

    def setInputBlockBlacklist(self, blockBlacklist):
        """
        _setInputBlockBlacklist_

        Set the block black list for the input dataset.  This must be called
        after setInputDataset().
        """
        self.data.input.dataset.blocks.blacklist = blockBlacklist
        return

    def inputBlockBlacklist(self):
        """
        _inputBlockBlacklist_

        Retrieve the block black list for the input dataset if it exsits, none
        otherwise.
        """
        if hasattr(self.data.input, "dataset"):
            return self.data.input.dataset.blocks.blacklist
        return None

    def setInputRunWhitelist(self, runWhitelist):
        """
        _setInputRunWhitelist_

        Set the run white list for the input dataset.  This must be called
        after setInputDataset().
        """
        self.data.input.dataset.runs.whitelist = runWhitelist
        return

    def inputRunWhitelist(self):
        """
        _inputRunWhitelist_

        Retrieve the run white list for the input dataset if it exists, none
        otherwise.
        """
        if hasattr(self.data.input, "dataset"):
            return self.data.input.dataset.runs.whitelist
        return None

    def setInputRunBlacklist(self, runBlacklist):
        """
        _setInputRunBlacklist_

        Set the run black list for the input dataset.  This must be called
        after setInputDataset().
        """
        self.data.input.dataset.runs.blacklist = runBlacklist
        return

    def inputRunBlacklist(self):
        """
        _inputRunBlacklist_

        Retrieve the run black list for the input dataset if it exists, none
        otherwise.
        """
        if hasattr(self.data.input, "dataset"):
            return self.data.input.dataset.runs.blacklist
        return None

    def addProduction(self, **options):
        """
        _addProduction_

        Add details of production job related information.

        options should contain at least:
        TODO: Not sure what is necessary data ask Dave
        optional
        - totalevents - total events in dataset

        """
        if not hasattr(self.data, "production"):
            self.data.section_("production")

        for opt, arg in viewitems(options):
            setattr(self.data.production, opt, arg)

    def inputDataset(self):
        """
        _inputDataset_

        Get the input.dataset structure from this task

        """
        return getattr(self.data.input, "dataset", None)

    def getInputDatasetPath(self):
        """
        _getInputDatasetPath_

        Get the input dataset path because it's useful
        """

        if hasattr(self.data.input, 'dataset'):
            return getattr(self.data.input.dataset, 'name', None)

        return None

    def setInputPileupDatasets(self, dsetName):
        """
        _setInputPileupDatasets_

        Create a list of pileup datasets to be used by this task (possible
        multiple CMSSW steps)
        """
        self.data.input.section_("pileup")
        if not hasattr(self.data.input.pileup, "datasets"):
            self.data.input.pileup.datasets = []

        if isinstance(dsetName, list):
            self.data.input.pileup.datasets.extend(dsetName)
        elif isinstance(dsetName, (newstr, bytes)):
            self.data.input.pileup.datasets.append(dsetName)
        else:
            raise ValueError("Pileup dataset must be either a list or a string (unicode or bytes)")
        # make the list unique
        self.data.input.pileup.datasets = list(set(self.data.input.pileup.datasets))

    def getInputPileupDatasets(self):
        """
        _getInputPileupDatasets_

        Get a list of the input pileup dataset name(s) for this task.
        """
        if hasattr(self.data.input, 'pileup'):
            return getattr(self.data.input.pileup, 'datasets', [])
        return []

    def siteWhitelist(self):
        """
        _siteWhitelist_

        Accessor for the site white list for the task.
        """
        return self.data.constraints.sites.whitelist

    def setSiteWhitelist(self, siteWhitelist):
        """
        _setSiteWhitelist_

        Set the set white list for the task.
        """
        self.data.constraints.sites.whitelist = siteWhitelist
        return

    def siteBlacklist(self):
        """
        _siteBlacklist_

        Accessor for the site white list for the task.
        """
        return self.data.constraints.sites.blacklist

    def setSiteBlacklist(self, siteBlacklist):
        """
        _setSiteBlacklist_

        Set the site black list for the task.
        """
        self.data.constraints.sites.blacklist = siteBlacklist
        return

    def getTrustSitelists(self):
        """
        _getTrustSitelists_

        Get the input and pileup flag for 'trust site lists' in the task.
        """
        # handle backward compatibility for the request which doesn't contain trustPUlists
        return {'trustlists': getattr(self.data.constraints.sites, 'trustlists', False),
                'trustPUlists': getattr(self.data.constraints.sites, 'trustPUlists', False)}

    def setTrustSitelists(self, trustSitelists, trustPUSitelists):
        """
        _setTrustSitelists_

        Set the input and the pileup flags for 'trust site lists' in the task.
        """
        self.data.constraints.sites.trustlists = trustSitelists
        self.data.constraints.sites.trustPUlists = trustPUSitelists
        return

    def listOutputDatasetsAndModules(self):
        """
        _listOutputDatasetsAndModules_

        Get the output datasets per output module for this task
        """
        outputDatasets = []
        for stepName in self.listAllStepNames():
            stepHelper = self.getStepHelper(stepName)

            if not getattr(stepHelper.data.output, "keep", True):
                continue

            if stepHelper.stepType() == "CMSSW":
                for outputModuleName in stepHelper.listOutputModules():
                    outputModule = stepHelper.getOutputModule(outputModuleName)
                    outputDataset = "/%s/%s/%s" % (outputModule.primaryDataset,
                                                   outputModule.processedDataset,
                                                   outputModule.dataTier)
                    outputDatasets.append({"outputModule": outputModuleName,
                                           "outputDataset": outputDataset})

        return outputDatasets

    def setSubscriptionInformation(self, custodialSites=None, nonCustodialSites=None,
                                   priority="Low", primaryDataset=None,
                                   useSkim=False, isSkim=False,
                                   dataTier=None, deleteFromSource=False,
                                   datasetLifetime=None):
        """
        _setSubscriptionsInformation_

        Set the subscription information for this task's datasets
        The subscriptions information is structured as follows:
        data.subscriptions.outputSubs is a list with the output section names (1 per dataset)
        data.subscriptions.<outputSection>.dataset
        data.subscriptions.<outputSection>.outputModule
        data.subscriptions.<outputSection>.custodialSites
        data.subscriptions.<outputSection>.nonCustodialSites
        data.subscriptions.<outputSection>.priority

        The filters arguments allow to define a dataTier and primaryDataset. Only datasets
        matching those values will be configured.
        """
        custodialSites = custodialSites or []
        nonCustodialSites = nonCustodialSites or []

        if not hasattr(self.data, "subscriptions"):
            self.data.section_("subscriptions")
            self.data.subscriptions.outputSubs = []

        outputDatasets = self.listOutputDatasetsAndModules()

        for entry in enumerate(outputDatasets, start=1):
            subSectionName = "output%s" % entry[0]
            outputDataset = entry[1]["outputDataset"]
            outputModule = entry[1]["outputModule"]

            dsSplit = outputDataset.split('/')
            primDs = dsSplit[1]
            tier = dsSplit[3]
            procDsSplit = dsSplit[2].split('-')
            skim = (len(procDsSplit) == 4)

            if primaryDataset and primDs != primaryDataset:
                continue
            if useSkim and isSkim != skim:
                continue
            if dataTier and tier != dataTier:
                continue

            self.data.subscriptions.outputSubs.append(subSectionName)
            outputSection = self.data.subscriptions.section_(subSectionName)
            outputSection.dataset = outputDataset
            outputSection.outputModule = outputModule
            outputSection.custodialSites = custodialSites
            outputSection.nonCustodialSites = nonCustodialSites
            outputSection.priority = priority
            outputSection.deleteFromSource = deleteFromSource
            outputSection.datasetLifetime = datasetLifetime

        return

    def getSubscriptionInformation(self):
        """
        _getSubscriptionInformation_

        Get the subscription configuration for the task
        return a dictionary with the following structure
        {<dataset> : {CustodialSites : [],
                      NonCustodialSites : [],
                      Priority : "Low"
                     }
        }
        """
        if not hasattr(self.data, "subscriptions"):
            return {}

        subKeyName = 'outputSubs'

        subInformation = {}
        for outputSub in getattr(self.data.subscriptions, subKeyName):
            outputSection = getattr(self.data.subscriptions, outputSub)
            dataset = outputSection.dataset

            subInformation[dataset] = {"CustodialSites": outputSection.custodialSites,
                                       "NonCustodialSites": outputSection.nonCustodialSites,
                                       "Priority": outputSection.priority,
                                       # These might not be present in all specs
                                       "DeleteFromSource": getattr(outputSection, "deleteFromSource", False),
                                       # Spec assigned for T0 ContainerRules
                                       "DatasetLifetime": getattr(outputSection, "datasetLifetime", 0)}
        return subInformation

    def parentProcessingFlag(self):
        """
        _parentProcessingFlag_

        accessor for parentProcessing information (two file input)
        """
        return self.jobSplittingParameters().get("include_parents", False)

    def totalEvents(self):
        """
        _totalEvents_

        accessor for total events in the given dataset
        """
        # TODO: save the total events for  the production job
        return int(self.data.production.totalEvents)
        # return self.data.input.dataset.totalEvents

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

    def setTaskType(self, taskType):
        """
        _setTaskType_

        Set the type field of this task
        """
        self.data.taskType = taskType

    def taskType(self):
        """
        _taskType_

        Get the task Type setting
        """
        return self.data.taskType

    def completeTask(self, jobLocation, reportName):
        """
        _completeTask_

        Combine all the logs from all the steps in the task to a single log

        If necessary, output to Dashboard
        """
        from WMCore.FwkJobReport.Report import Report

        finalReport = Report()
        # We left the master report at the pilot scratch area level
        testPath = os.path.join(jobLocation, '../../', reportName)
        logging.info("Looking for master report at %s", testPath)
        if os.path.exists(testPath):
            logging.info("  found it!")
            # If a report already exists, we load it and
            # append our steps to it
            finalReport.load(testPath)
        taskSteps = self.listAllStepNames()
        for taskStep in taskSteps:
            reportPath = os.path.join(jobLocation, taskStep, "Report.pkl")
            logging.info("Looking for a taskStep report at %s", reportPath)
            if os.path.isfile(reportPath):
                logging.info("  found it!")
                stepReport = Report()
                stepReport.unpersist(reportPath, taskStep)
                finalReport.setStep(taskStep, stepReport.retrieveStep(taskStep))
                logURL = stepReport.getLogURL()
                if logURL:
                    finalReport.setLogURL(logURL)
            else:
                msg = "  failed to find it."
                msg += "Files in the directory are:\n%s" % os.listdir(os.path.join(jobLocation, taskStep))
                logging.error(msg)
                # Then we have a missing report
                # This should raise an alarm bell, as per Steve's request
                # TODO: Change error code
                finalReport.addStep(reportname=taskStep, status=1)
                finalReport.addError(stepName=taskStep, exitCode=99996, errorType="ReportManipulatingError",
                                     errorDetails="Failed to find a step report for %s!" % taskStep)

        finalReport.data.completed = True
        finalReport.persist(reportName)

        return finalReport

    def taskLogBaseLFN(self):
        """
        _taskLogBaseLFN_

        Get the base LFN for the task's log archive file.
        """
        return getattr(self.data, "logBaseLFN", "/store/temp/WMAgent/unmerged")

    def setTaskLogBaseLFN(self, logBaseLFN):
        """
        _setTaskLogBaseLFN_

        Set the base LFN for the task's log archive file.
        """
        self.data.logBaseLFN = logBaseLFN
        return

    def addNotification(self, target):
        """
        _addNotification_

        Add a target to be notified on workflow completion
        """

        self.data.notifications.targets.append(target)
        return

    def getNotifications(self):
        """
        _getNotifications_

        Get all targets for notification at workflow completion
        """

        return self.data.notifications.targets

    def _setPerformanceMonitorConfig(self):
        """
        if config section for the PerformanceMonitor. If not set, it will set one
        """
        if self.monitoring is not None:
            return

        self.monitoring = self.data.section_("watchdog")
        if not hasattr(self.data.watchdog, 'monitors'):
            self.data.watchdog.monitors = []
        if 'PerformanceMonitor' not in self.monitoring.monitors:
            self.monitoring.monitors.append('PerformanceMonitor')
            self.monitoring.section_("PerformanceMonitor")
        return

    def setMaxPSS(self, maxPSS):
        """
        _setMaxPSS_

        Set MaxPSS performance monitoring for this task.
        :param maxPSS: maximum Proportional Set Size (PSS) memory consumption in MiB
        """
        if self.taskType() in ["Merge", "Cleanup", "LogCollect"]:
            # keep the default settings (from StdBase) for these task types
            return

        if isinstance(maxPSS, dict):
            maxPSS = maxPSS.get(self.name(), None)

        if maxPSS:
            self._setPerformanceMonitorConfig()
            self.monitoring.PerformanceMonitor.maxPSS = int(maxPSS)
            for task in self.childTaskIterator():
                task.setMaxPSS(maxPSS)
        return

    def setPerformanceMonitor(self, softTimeout=None, gracePeriod=None):
        """
        _setPerformanceMonitor_

        Set/Update the performance monitor options for the task
        """
        # make sure there is a PerformanceMonitor section in the task
        self._setPerformanceMonitorConfig()

        if softTimeout:
            self.monitoring.PerformanceMonitor.softTimeout = int(softTimeout)
            if gracePeriod:
                self.monitoring.PerformanceMonitor.hardTimeout = int(softTimeout + gracePeriod)

        return

    def getSwVersion(self, allSteps=False):
        """
        _getSwVersion_

        Get the CMSSW version for the first CMSSW step in this task.
        :param allSteps: set it to True to retrieve a list of CMSSW releases
         used in this task
        :return: a string with the release name or a list of releases if allSteps is True.
        """
        versions = []
        for stepName in self.listAllStepNames():
            stepHelper = self.getStepHelper(stepName)
            if stepHelper.stepType() in ["CMSSW", "LogCollect"]:
                if not allSteps:
                    return stepHelper.getCMSSWVersion()
                else:
                    versions.append(stepHelper.getCMSSWVersion())
        return versions

    def getScramArch(self, allSteps=False):
        """
        _getScramArch_

        Get the scram architecture for the first CMSSW step of workload.
        Set allSteps to true to retrieve all the scramArchs used in this task.
        """
        scrams = []
        for stepName in self.listAllStepNames():
            stepHelper = self.getStepHelper(stepName)
            if stepHelper.stepType() in ["CMSSW", "LogCollect"]:
                if not allSteps:
                    return stepHelper.getScramArch()
                else:
                    scrams.append(stepHelper.getScramArch())
        return scrams

    def setPrimarySubType(self, subType):
        """
        _setPrimarySubType_

        Set the subType that should be used by WorkQueue for the
        primary subscription
        """

        self.data.parameters.primarySubType = subType
        return

    def getPrimarySubType(self):
        """
        _getPrimarySubType_

        Retrieve the primary subType
        If not available, use the taskType
        """

        return getattr(self.data.parameters, 'primarySubType',
                       self.taskType())

    def getConfigCacheIDs(self):
        """
        _getConfigCacheIDs_

        Search constituent steps for ConfigCacheID
        """

        IDs = []
        for stepName in self.listAllStepNames():
            stepHelper = self.getStepHelper(stepName)
            ID = stepHelper.getConfigCacheID()
            if ID:
                IDs.append(ID)
        return IDs

    def setProcessingVersion(self, procVer, parentProcessingVersion=0, stepChainMap=False):
        """
        _setProcessingVersion_

        Set the task processing version
        """
        if isinstance(procVer, dict) and stepChainMap:
            taskProcVer = self._getStepValue(procVer, parentProcessingVersion)
            self._setStepProperty("ProcessingVersion", procVer, stepChainMap)
        elif isinstance(procVer, dict):
            taskProcVer = procVer.get(self.name(), parentProcessingVersion)
            if taskProcVer is None:
                for taskname in procVer:
                    if taskname in self.name():
                        taskProcVer = procVer[taskname]
        else:
            taskProcVer = procVer

        self.data.parameters.processingVersion = int(taskProcVer)
        for task in self.childTaskIterator():
            task.setProcessingVersion(procVer, taskProcVer, stepChainMap)
        return

    def getProcessingVersion(self):
        """
        _getProcessingVersion_

        Get the task processing version
        """
        return getattr(self.data.parameters, 'processingVersion', 0)

    def setProcessingString(self, procString, parentProcessingString=None, stepChainMap=False):
        """
        _setProcessingString_

        Set the task processing string
        """
        if isinstance(procString, dict) and stepChainMap:
            taskProcString = self._getStepValue(procString, parentProcessingString)
            self._setStepProperty("ProcessingString", procString, stepChainMap)
        elif isinstance(procString, dict):
            taskProcString = procString.get(self.name(), parentProcessingString)
            if taskProcString is None:
                for taskname in procString:
                    if taskname in self.name():
                        taskProcString = procString[taskname]
        else:
            taskProcString = procString

        self.data.parameters.processingString = taskProcString

        for task in self.childTaskIterator():
            task.setProcessingString(procString, taskProcString, stepChainMap)
        return

    def getProcessingString(self):
        """
        _getProcessingString_

        Get the task processing string
        """
        return getattr(self.data.parameters, 'processingString', None)

    def getCMSSWVersionsWithMergeTask(self):
        """
        _getCMSSWVersionsWithMergeTask_

        Get the all the cmssw versions for this task plus first generation merge task cmssw version.
        This will be used to validate and check in the script.
        Merge cmssw version should be the same as processing version
        """
        versions = set()
        for stepName in self.listAllStepNames():

            stepHelper = self.getStepHelper(stepName)
            if stepHelper.stepType() != "CMSSW":
                continue
            version = stepHelper.getCMSSWVersion()
            versions.add(version)

        for task in self.childTaskIterator():
            if task.taskType() == "Merge":
                for stepName in task.listAllStepNames():

                    stepHelper = task.getStepHelper(stepName)
                    if stepHelper.stepType() != "CMSSW":
                        continue
                    version = stepHelper.getCMSSWVersion()
                    versions.add(version)

        return versions

    def setNumberOfCores(self, cores, nStreams):
        """
        _setNumberOfCores_

        Set number of cores and event streams for each CMSSW step in this task and its children
        """
        if self.taskType() in ["Merge", "Harvesting", "Cleanup", "LogCollect"]:
            return

        if isinstance(cores, dict):
            taskCores = cores.get(self.name())
        else:
            taskCores = cores

        if isinstance(nStreams, dict):
            taskStreams = nStreams.get(self.name(), 0)
        else:
            taskStreams = nStreams

        if taskCores:
            for stepName in self.listAllStepNames():
                stepHelper = self.getStepHelper(stepName)
                if stepHelper.stepType() == "CMSSW":
                    stepHelper.setNumberOfCores(taskCores, taskStreams)

        for task in self.childTaskIterator():
            task.setNumberOfCores(cores, nStreams)

        return

    def getNumberOfCores(self):
        """
        Retrieves the number of cores for this task.
        If it's a multi-step task, it returns only the greatest value
        :return: an integer with the number of cores required by this task
        """
        maxCores = 1
        for stepName in self.listAllStepNames():
            stepHelper = self.getStep(stepName)
            maxCores = max(maxCores, stepHelper.getNumberOfCores())
        return maxCores

    def setTaskGPUSettings(self, requiresGPU, gpuParams):
        """
        Setter method for the GPU settings, applied to this Task object and
        all underneath CMSSW type step object.
        :param requiresGPU: string defining whether GPUs are needed. For TaskChains, it
            could be a dictionary key'ed by the taskname.
        :param gpuParams: GPU settings. A JSON encoded object, from either a None object
            or a dictionary. For TaskChains, it could be a dictionary key'ed by the taskname
        :return: nothing, the workload spec is updated in place.
        """
        # these job types shall not have these settings
        if self.taskType() in ["Merge", "Harvesting", "Cleanup", "LogCollect"]:
            return

        # default values come from StdBase
        if isinstance(requiresGPU, dict):
            thisTaskGPU = requiresGPU.get(self.name(), "forbidden")
        else:
            thisTaskGPU = requiresGPU

        decodedGpuParams = json.loads(gpuParams)
        if self.name() in decodedGpuParams:
            thisTaskGPUParams = decodedGpuParams[self.name()]
        else:
            thisTaskGPUParams = decodedGpuParams

        for stepName in self.listAllStepNames():
            stepHelper = self.getStepHelper(stepName)
            if stepHelper.stepType() == "CMSSW":
                stepHelper.setGPUSettings(thisTaskGPU, thisTaskGPUParams)

        for task in self.childTaskIterator():
            task.setTaskGPUSettings(requiresGPU, gpuParams)

        return

    def getRequiresGPU(self):
        """
        Return whether this task is supposed to use GPUs or not.
        If it's a multi-step task, decision follows this order:
          1. "required"
          2. "optional"
          3. "forbidden"
        :return: a string (default to "forbidden")
        """
        requiresGPU = set(["forbidden"])
        for stepName in self.listAllStepNames():
            stepHelper = self.getStep(stepName)
            if stepHelper.stepType() == "CMSSW" and stepHelper.getGPURequired():
                requiresGPU.add(stepHelper.getGPURequired())

        # now decide what value has higher weight
        if len(requiresGPU) == 1:
            return requiresGPU.pop()
        elif "required" in requiresGPU:
            return "required"
        elif "optional" in requiresGPU:
            return "optional"
        else:
            return "forbidden"

    def getGPURequirements(self):
        """
        Return the GPU requirements for this task.
        If it's a multi-step task, the first step with a meaningful
        dictionary value will be returned
        :return: a dictionary with the GPU requirements for this task
        """
        gpuRequirements = {}
        for stepName in sorted(self.listAllStepNames()):
            stepHelper = self.getStep(stepName)
            if stepHelper.stepType() == "CMSSW" and stepHelper.getGPURequirements():
                return stepHelper.getGPURequirements()
        return gpuRequirements

    def _getStepValue(self, keyDict, defaultValue):
        """
        __getStepValue_

        Maps this taskName - in somehow a hacky way - to a 'StepName' value
        that should exist in a StepChain request. Used only on tasks that have
        output module
        :param keyDict: a dict with either AcqEra/ProcStr/ProcVer key/value pairs,
        where the key corresponds to the StepName
        """
        if self.taskType() == "Merge":
            extractedTaskName = self.name().split("Merge")[0]
            value = keyDict.get(extractedTaskName)
        elif self.taskType() in ["Production", "Processing"]:
            value = keyDict.get(self.name())
        else:
            value = defaultValue

        return value

    def _setStepProperty(self, propertyName, propertyDict, stepMap):
        """
        For StepChain workloads, we also need to set AcqEra/ProcStr/ProcVer
        at the WMStep level, such that we can properly map different cmsRun
        steps - within the same task - to different meta data information.
        :param propertyName: the name of the property to set at step level
        :param propertyDict: a dictionary mapping StepName to its value
        :param stepMap: map between step name, step number and cmsRun number,
                        same as returned from the workload getStepMapping
        """
        propMethodMap = {"AcquisitionEra": "setAcqEra",
                         "ProcessingString": "setProcStr",
                         "ProcessingVersion": "setProcStr"}

        if self.taskType() not in ["Production", "Processing"]:
            # then there is no need to set anything, single cmsRun step at most
            return

        for stepName, stepValues in viewitems(stepMap):
            cmsRunNum = stepValues[1]
            stepHelper = self.getStepHelper(cmsRunNum)
            callableMethod = getattr(stepHelper, propMethodMap[propertyName])
            callableMethod(propertyDict[stepName])

    def setAcquisitionEra(self, era, parentAcquisitionEra=None, stepChainMap=False):
        """
        _setAcquistionEra_

        Set the task acquisition era
        """

        if isinstance(era, dict) and stepChainMap:
            taskEra = self._getStepValue(era, parentAcquisitionEra)
            self._setStepProperty("AcquisitionEra", era, stepChainMap)
        elif isinstance(era, dict):
            taskEra = era.get(self.name(), parentAcquisitionEra)
            if taskEra is None:
                # We cannot properly set AcqEra for ACDC of TaskChain Merge
                # failures, so we should look up for a similar taskname in
                # the acqera dict passed from the requestor
                for taskname in era:
                    if taskname in self.name():
                        taskEra = era[taskname]
        else:
            taskEra = era

        self.data.parameters.acquisitionEra = taskEra

        for task in self.childTaskIterator():
            task.setAcquisitionEra(era, taskEra, stepChainMap)
        return

    def getAcquisitionEra(self):
        """
        _getAcquisitionEra_

        Get the task acquisition era.
        """
        return getattr(self.data.parameters, 'acquisitionEra', None)

    def setLumiMask(self, lumiMask=None, override=True):
        """
        Attach the given LumiMask to the task
        At this point the lumi mask is just the compactList dict not the LumiList object
        """

        if not lumiMask:
            return

        runs = getattr(self.data.input.splitting, 'runs', None)
        lumis = getattr(self.data.input.splitting, 'lumis', None)
        if not override and runs and lumis:  # Unless instructed, don't overwrite runs and lumis which may be there from a task already
            return

        runs = []
        lumis = []
        for run, runLumis in viewitems(lumiMask):
            runs.append(int(run))
            lumiList = []
            for lumi in runLumis:
                lumiList.extend([str(l) for l in lumi])
            lumis.append(','.join(lumiList))

        self.data.input.splitting.runs = runs
        self.data.input.splitting.lumis = lumis

        for task in self.childTaskIterator():
            task.setLumiMask(lumiMask, override)

        return

    def getLumiMask(self):
        """
            return the lumi mask
        """
        runs = getattr(self.data.input.splitting, 'runs', None)
        lumis = getattr(self.data.input.splitting, 'lumis', None)
        if runs and lumis:
            return LumiList(wmagentFormat=(runs, lumis))

        return {}

    def _propMethodMap(self):
        """
        internal mapping methop which maps which method need to be call for each
        property.
        For now only contains properties which updates in assignment stage.
        """
        propMap = {"ProcessingVersion": self.setProcessingVersion,
                   "AcquisitionEra": self.setAcquisitionEra,
                   "ProcessingString": self.setProcessingString
                   }
        return propMap

    def setProperties(self, properties):
        """
        set task properties (only for assignment stage but make it more general)
        """
        for prop, value in viewitems(properties):
            self._propMethodMap()[prop](value)

    def deleteChild(self, childName):
        """
        _deleteChild_

        Remove the child task from the tree, if it exists
        """
        self.deleteNode(childName)

    def setPrepID(self, prepID):
        """
        _setPrepID_

        Set the prepID to for all the tasks below
        """
        # if prepID doesn exist set it, if exist ignore.
        if not self.getPrepID() and prepID:
            self.data.prepID = prepID

        prepID = self.getPrepID()
        # set child prepid
        if prepID:
            for task in self.childTaskIterator():
                task.setPrepID(prepID)

    def getPrepID(self):
        """
        _getPrepID_

        Get the prepID for the workflow
        """
        return getattr(self.data, 'prepID', None)

    def setLFNBase(self, mergedLFNBase, unmergedLFNBase):
        """
        _setLFNBase_

        Set the merged and unmerged base LFNs for all tasks.
        """
        self.data.mergedLFNBase = mergedLFNBase
        self.data.unmergedLFNBase = unmergedLFNBase
        for task in self.childTaskIterator():
            task.setLFNBase(mergedLFNBase, unmergedLFNBase)

        return

    def _getLFNBase(self):
        """
        private method getting lfn base.
        lfn base should be set by workflow
        """
        return (getattr(self.data, 'mergedLFNBase', "/store/data"),
                getattr(self.data, 'unmergedLFNBase', "/store/unmerged"))

    def _getKeyValue(self, keyname, stepname, values):
        if keyname not in values:
            return
        elif isinstance(values[keyname], (newstr, bytes)):
            return values[keyname]
        elif isinstance(values[keyname], dict):
            return values[keyname].get(stepname)

    def _updateLFNsStepChain(self, stepName, dictValues, stepMapping):
        """
        __updateLFNsStepChain_

        Helper function needed for a proper StepChain LFN/ProcessedDataset handling

        :param stepName: is the cmsRun name (cmsRun1, cmsRun2, ...)
        :param dictValues: part of the arguments provided during assignment
        :param stepMapping: built during StepChain creation
        :return: a single string for each of those 3 properties
        """
        reqStepName = None
        for reqStep, values in viewitems(stepMapping):
            if stepName == values[1]:
                reqStepName = reqStep
        if not reqStepName:
            # I have no idea which cmsRun is that...
            return None, None, None

        era = self._getKeyValue('AcquisitionEra', reqStepName, dictValues)
        if not era:
            era = self.getAcquisitionEra()
        procstr = self._getKeyValue('ProcessingString', reqStepName, dictValues)
        if not procstr:
            procstr = self.getProcessingString()
        procver = self._getKeyValue('ProcessingVersion', reqStepName, dictValues)
        if not procver:
            procver = self.getProcessingVersion()

        return era, procstr, procver

    def updateLFNsAndDatasets(self, runNumber=None, dictValues=None, stepMapping=None):
        """
        _updateLFNsAndDatasets_

        Update all the output LFNs and data names for all tasks in the workflow.
        This needs to be called after updating the acquisition era, processing
        version or merged/unmerged lfn base.
        """
        mergedLFNBase, unmergedLFNBase = self._getLFNBase()
        taskType = self.taskType()

        for stepName in self.listAllStepNames():
            stepHelper = self.getStepHelper(stepName)

            if stepHelper.stepType() == "CMSSW":
                if dictValues and stepMapping:
                    # if it's a StepChain, then cast a dark spell on it
                    acqera, procstr, procver = self._updateLFNsStepChain(stepName, dictValues, stepMapping)
                else:
                    acqera = self.getAcquisitionEra()
                    procstr = self.getProcessingString()
                    procver = self.getProcessingVersion()

                for outputModuleName in stepHelper.listOutputModules():
                    outputModule = stepHelper.getOutputModule(outputModuleName)
                    filterName = getattr(outputModule, "filterName", None)

                    if procstr:
                        processingEra = "%s-v%i" % (procstr, procver)
                    else:
                        processingEra = "v%i" % procver
                    if filterName:
                        processedDataset = "%s-%s-%s" % (acqera, filterName, processingEra)
                        processingString = "%s-%s" % (filterName, processingEra)
                    else:
                        processedDataset = "%s-%s" % (acqera, processingEra)
                        processingString = processingEra

                    unmergedLFN = "%s/%s/%s/%s/%s" % (unmergedLFNBase,
                                                      acqera,
                                                      getattr(outputModule, "primaryDataset"),
                                                      getattr(outputModule, "dataTier"),
                                                      processingString)
                    mergedLFN = "%s/%s/%s/%s/%s" % (mergedLFNBase,
                                                    acqera,
                                                    getattr(outputModule, "primaryDataset"),
                                                    getattr(outputModule, "dataTier"),
                                                    processingString)

                    if runNumber is not None and runNumber > 0:
                        runString = str(runNumber).zfill(9)
                        lfnSuffix = "/%s/%s/%s" % (runString[0:3],
                                                   runString[3:6],
                                                   runString[6:9])
                        unmergedLFN += lfnSuffix
                        mergedLFN += lfnSuffix

                    lfnBase(unmergedLFN)
                    lfnBase(mergedLFN)
                    setattr(outputModule, "processedDataset", processedDataset)

                    # For merge tasks, we want all output to go to the merged LFN base.
                    if taskType == "Merge":
                        setattr(outputModule, "lfnBase", mergedLFN)
                        setattr(outputModule, "mergedLFNBase", mergedLFN)

                        if getattr(outputModule, "dataTier") in ["DQM", "DQMIO"]:
                            datasetName = "/%s/%s/%s" % (getattr(outputModule, "primaryDataset"),
                                                         processedDataset,
                                                         getattr(outputModule, "dataTier"))
                            self.updateDatasetName(datasetName)
                    else:
                        setattr(outputModule, "lfnBase", unmergedLFN)
                        setattr(outputModule, "mergedLFNBase", mergedLFN)

        self.setTaskLogBaseLFN(unmergedLFNBase)

        # do the samething for all the child
        for task in self.childTaskIterator():
            task.updateLFNsAndDatasets(runNumber=runNumber)

        return

    def updateDatasetName(self, datasetName):
        """
        _updateDatasetName_

        Updates the dataset name argument of the mergeTask's harvesting
        children tasks
        """
        for task in self.childTaskIterator():
            if task.taskType() == "Harvesting":
                for stepName in task.listAllStepNames():
                    stepHelper = task.getStepHelper(stepName)

                    if stepHelper.stepType() == "CMSSW":
                        cmsswHelper = stepHelper.getTypeHelper()
                        cmsswHelper.setDatasetName(datasetName)

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
        self.pathName = None
        self.taskType = None
        self.prepID = None
        self.section_("steps")
        self.steps.topStepName = None
        self.section_("parameters")
        self.section_("pythonLibs")
        self.section_("constraints")
        self.section_("input")
        self.section_("notifications")
        self.section_("subscriptions")
        self.section_("environment")
        self.notifications.targets = []
        self.input.sandbox = None
        self.input.section_("splitting")
        self.input.splitting.algorithm = None
        self.input.splitting.section_("performance")
        self.constraints.section_("sites")
        self.constraints.sites.whitelist = []
        self.constraints.sites.blacklist = []
        self.constraints.sites.trustlists = False
        self.constraints.sites.trustPUlists = False
        self.subscriptions.outputSubs = []
        self.input.section_("WMBS")


def makeWMTask(taskName):
    """
    _makeWMTask_

    Convienience method to instantiate a new WMTask with the name
    provided and wrap it in a helper

    """
    return WMTaskHelper(WMTask(taskName))
