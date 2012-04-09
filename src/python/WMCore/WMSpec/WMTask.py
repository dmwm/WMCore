#!/usr/bin/env python
#pylint: disable-msg=E1101
# E1101:  Doesn't recognize section_() as defining objects
"""
_WMTask_

Object containing a set of executable Steps which form a template for a
set of jobs.

Equivalent of a WorkflowSpec in the ProdSystem.
"""

import os
import os.path
import time

from WMCore.Configuration import ConfigSection
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
    data object.    
    """
    def __init__(self, wmTask):
        TreeHelper.__init__(self, wmTask)
        self.startTime = None
        self.endTime   = None

    
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
        _listPathNames
        
        """
        for t in self.taskIterator():
            yield t.getPathName()
    
    def listNames(self):
        """
        _listPathNames
        
        """
        for t in self.taskIterator():
            yield t.name()

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

    def listAllStepNames(self):
        """
        _listAllStepNames_
        
        Get a list of all the step names contained in this task.        
        """
        step = self.steps()
        if step:
            return step.allNodeNames()
        else:
            return []
    
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

    def getOutputModulesForTask(self):
        """
        _getOutputModulesForTask_

        Retrieve all the output modules in the given task.
        """
        outputModules = []
        for stepName in self.listAllStepNames():
            outputModules.append(self.getOutputModulesForStep(stepName))

        return outputModules
    
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
                # At this point we do not support the
                # setting of sub-sections for environment variables
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
        self.startTime = time.time()
        self.setupEnvironment()
        master = ExecuteMaster()
        master(self, wmbsJob)
        self.endTime   = time.time()
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

    def setSplittingParameters(self, **params):
        """
        _setSplittingParameters_

        Set the job splitting parameters.
        """        
        [setattr(self.data.input.splitting, key, val)
         for key, val in params.items() ]
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
        """
        setACDCParams = {}
        for paramName in ["collectionName", "filesetName", "couchURL",
                          "couchDB", "owner", "group", "initial_lfn_counter",
                          "merge_across_runs", "runWhitelist"]:
            if hasattr(self.data.input.splitting, paramName):
                setACDCParams[paramName] = getattr(self.data.input.splitting,
                                                   paramName)
        
        delattr(self.data.input, "splitting")
        self.data.input.section_("splitting")
        
        setattr(self.data.input.splitting, "algorithm", algoName)
        self.setSplittingParameters(**params)
        self.setSplittingParameters(**setACDCParams)
        return

    def jobSplittingAlgorithm(self):
        """
        _jobSplittingAlgorithm_
        
        Retrieve the job splitting algorithm name.
        """
        return getattr(self.data.input.splitting, "algorithm", None)
    
    def jobSplittingParameters(self):
        """
        _jobSplittingParameters_
        
        Retrieve the job splitting parameters.  This will combine the job
        splitting parameters specified in the spec with the site white list
        and black list as those are passed to the job splitting code.
        """
        datadict = getattr(self.data.input, "splitting")
        splittingParams = datadict.dictionary_()
        splittingParams["siteWhitelist"] = self.siteWhitelist()
        splittingParams["siteBlacklist"] = self.siteBlacklist()
        return splittingParams

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
        """
        _listGenerators_

        """
        generators = getattr(self.data, "generators", None)
        if generators == None:
            return []
        return generators.listSections_()

    def getGeneratorSettings(self, generatorName):
        """
        _getGeneratorSettings_

        Extract the settings from the generator fields
        """
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
        self.data.input.dataset.dbsurl = None
        self.data.input.dataset.section_("blocks")
        self.data.input.dataset.blocks.whitelist = []
        self.data.input.dataset.blocks.blacklist = []
        self.data.input.dataset.section_("runs")
        self.data.input.dataset.runs.whitelist = []
        self.data.input.dataset.runs.blacklist = []
        
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
            if opt in ['primary', 'processed', 'tier']:
                continue
            elif opt == 'block_blacklist':
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

    def inputDatasetDBSURL(self):
        """
        _inputDatasetDBSURL_

        Retrieve the DBS URL for the input dataset if it exists, none otherwise.
        """
        if hasattr(self.data.input, "dataset"):
            return self.data.input.dataset.dbsurl
        return None

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

    def getInputDatasetPath(self):
        """
        _getInputDatasetPath_

        Get the input dataset path because it's useful
        """

        if hasattr(self.data.input, 'dataset'):
            ds = getattr(self.data.input, 'dataset')
            return '/%s/%s/%s' % (ds.primary, ds.processed, ds.tier)
        return None
    
    def siteWhitelist(self):
        """
        _siteWhitelist_
        
        Accessor for the site white list for the task.
        """        
        return self.data.constraints.sites.whitelist

    def setSiteWhitelist(self, siteWhitelist):
        """
        _setSiteWhitelist_

        Set the set white list for this task.
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

        Set the site black list for this task.
        """
        self.data.constraints.sites.blacklist = siteBlacklist
        return
    
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
        #TODO: save the total events for  the production job
        return int(self.data.production.totalEvents)
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

    def completeTask(self, jobLocation, logLocation):
        """
        _completeTask_

        Combine all the logs from all the steps in the task to a single log

        If necessary, output to Dashboard
        """
        import WMCore.FwkJobReport.Report as Report

        finalReport = Report.Report()
        # We left the master report somewhere way up at the top
        testPath = os.path.join(jobLocation, '../../', logLocation)
        if os.path.exists(testPath):
            # If a report already exists, we load it and
            # append our steps to it
            finalReport.load(testPath)
        taskSteps = self.listAllStepNames()
        for taskStep in taskSteps:
            reportPath = os.path.join(jobLocation, taskStep, "Report.pkl")
            if os.path.isfile(reportPath):
                stepReport = Report.Report(taskStep)
                stepReport.unpersist(reportPath)
                finalReport.setStep(taskStep, stepReport.retrieveStep(taskStep))
            else:
                # Then we have a missing report
                # This should raise an alarm bell, as per Steve's request
                # TODO: Change error code
                finalReport.addStep(reportname = taskStep, status = 1)
                finalReport.addError(stepName = taskStep, exitCode = 99999, errorType = "ReportManipulatingError", 
                                     errorDetails = "Could not find report file for step %s!" % taskStep)

        finalReport.data.completed = True
        finalReport.persist(logLocation)


        return

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

    def setTaskTimeOut(self, taskTimeOut):
        """
        _setTaskTimeOut_

        Set the timeout for the task.
        """
        monitoring = self.data.section_("watchdog")
        monitoring.monitors = ["DashboardMonitor"]
        monitoring.section_("DashboardMonitor")
        monitoring.DashboardMonitor.softTimeOut = taskTimeOut
        monitoring.DashboardMonitor.hardTimeOut = taskTimeOut + 600
        return

    def getTaskTimeOut(self):
        """
        _getTaskTimeOut_

        Get the timeout for the task.
        """
        return self.data.watchdog.DashboardMonitor.softTimeOut


    def setTaskPriority(self, priority):
        """
        _setTaskPriority_

        Set the relative priority of this task
        Determines run order in compatible batch systems.
        Expects an integer.
        Higher is better (will be given first shot at open slots)
        """
        if not type(priority) == int:
            try:
                priority = int(priority)
            except ValueError:
                # Can't really do anything if you don't give an int
                return

        self.data.taskPriority = priority
        return

    def getTaskPriority(self):
        """
        _getTaskPriority_

        Get the priority level for the task
        """
        return getattr(self.data, 'taskPriority', None)

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

    def setPerformanceMonitor(self, maxRSS = None, maxVSize = None):
        """
        _setPerformanceMonitor_

        Set the setup for a non-standard optional plugin that
        you may or may not use because Oli wants something.
        """
        monitoring = self.data.section_("watchdog")
        if not hasattr(self.data.watchdog, 'monitors'):
            self.data.watchdog.monitors = []
        if not 'PerformanceMonitor' in monitoring.monitors:
            monitoring.monitors.append('PerformanceMonitor')
            monitoring.section_("PerformanceMonitor")
        monitoring.PerformanceMonitor.maxRSS   = maxRSS
        monitoring.PerformanceMonitor.maxVSize = maxVSize
        return

    def getSwVersion(self):
        """
        _getSwVersion_

        Get the CMSSW version for the first CMSSW step of workload.
        """

        for stepName in self.listAllStepNames():
            stepHelper = self.getStepHelper(stepName)
            if stepHelper.stepType() == "CMSSW":
                return stepHelper.getCMSSWVersion()
        return None

    def getScramArch(self):
        """
        _getScramArch_

        Get the scram architecture for the first CMSSW step of workload.
        """

        for stepName in self.listAllStepNames():
            stepHelper = self.getStepHelper(stepName)
            if stepHelper.stepType() == "CMSSW":
                return stepHelper.getScramArch()
        return None

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
        self.section_("steps")
        self.steps.topStepName = None
        self.section_("parameters")
        self.section_("pythonLibs")
        self.section_("constraints")
        self.section_("input")
        self.section_("notifications")
        self.notifications.targets = []
        self.input.sandbox = None
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


