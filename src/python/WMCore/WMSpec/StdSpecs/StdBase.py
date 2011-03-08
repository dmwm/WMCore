#!/usr/bin/env python
"""
_StdBase_

Base class with helper functions for standard WMSpec files.
"""

from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.WMStep import makeWMStep
from WMCore.WMSpec.Steps.StepFactory import getStepTypeHelper

from WMCore.Cache.WMConfigCache import ConfigCache
from WMCore.Lexicon import lfnBase

class StdBase(object):
    """
    _StdBase_

    Base class with helper functions for standard WMSpec file.
    """
    def __init__(self):
        """
        __init__

        Setup parameters that will be used by all workflows.  These parameters
        are not required to be set when the workflow is first created but will
        need to be set before the sandbox is created and jobs are made.

        These parameters can be changed after the workflow has been created by
        the methods in the WMWorkloadHelper class.
        """
        self.workloadName = None
        self.priority = 0
        self.owner = None
        self.group = None
        self.acquisitionEra = None
        self.scramArch = None
        self.processingVersion = None
        self.siteBlacklist = []
        self.siteWhitelist = []
        self.unmergedLFNBase = None
        self.mergedLFNBase = None
        self.minMergeSize = 500000000
        self.maxMergeSize = 4294967296
        self.maxMergeEvents = 100000
        self.validStatus = None
        return

    def __call__(self, workloadName, arguments):
        """
        __call__

        Look through the arguments that were passed into the workload's call
        method and pull out any that are setup by this base class.
        """
        self.workloadName = workloadName
        self.priority = arguments.get("Priority", 0)
        self.owner = arguments.get("Requestor", None)
        self.group = arguments.get("Group", None)
        self.acquisitionEra = arguments.get("AcquisitionEra", None)
        self.scramArch = arguments.get("ScramArch", None)
        self.processingVersion = arguments.get("ProcessingVersion", None)
        self.siteBlacklist = arguments.get("SiteWhitelist", [])
        self.siteWhitelist = arguments.get("SiteBlacklist", [])
        self.unmergedLFNBase = arguments.get("UnmergedLFNBase", "/store/unmerged")
        self.mergedLFNBase = arguments.get("MergedLFNBase", "/store/data")
        self.minMergeSize = arguments.get("MinMergeSize", 500000000)
        self.maxMergeSize = arguments.get("MaxMergeSize", 4294967296)
        self.maxMergeEvents = arguments.get("MaxMergeEvents", 100000)
        self.validStatus = arguments.get("ValidStatus", "PRODUCTION")
        return

    def determineOutputModules(self, scenarioName = None, scenarioArgs = None,
                               configDoc = None, couchURL = None,
                               couchDBName = None):
        """
        _determineOutputModules_

        Determine the output module names and associated metadata for the
        given config.
        """
        outputModules = {}
        if configDoc != None and configDoc != "":
            configCache = ConfigCache(couchURL, couchDBName)
            configCache.loadByID(configDoc)
            outputModules = configCache.getOutputModuleInfo()
        else:
            for dataTier in scenarioArgs["writeTiers"]:
                outputModuleName = "output%s%s" % (dataTier, dataTier)
                outputModules[outputModuleName] = {"dataTier": dataTier,
                                                   "filterName": None}

        return outputModules

    def addDashboardMonitoring(self, task):
        """
        _addDashboardMonitoring_

        Add dashboard monitoring for the given task.
        """
        monitoring = task.data.section_("watchdog")
        monitoring.interval = 600
        monitoring.monitors = ["DashboardMonitor"]
        monitoring.section_("DashboardMonitor")
        monitoring.DashboardMonitor.softTimeOut = 300000
        monitoring.DashboardMonitor.hardTimeOut = 600000
        monitoring.DashboardMonitor.destinationHost = "cms-pamon.cern.ch"
        monitoring.DashboardMonitor.destinationPort = 8884
        return task

    def createWorkload(self):
        """
        _createWorkload_

        Create a new workload.
        """
        workload = newWorkload(self.workloadName)
        workload.setOwnerDetails(self.owner, self.group)
        workload.setStartPolicy("DatasetBlock", SliceType = "NumberOfFiles", SliceSize = 1)
        workload.setEndPolicy("SingleShot")
        workload.setAcquisitionEra(acquisitionEra = self.acquisitionEra)
        workload.setProcessingVersion(processingVersion = self.processingVersion)
        workload.setValidStatus(validStatus = self.validStatus)
        return workload

    def setupProcessingTask(self, procTask, taskType, inputDataset = None, inputStep = None,
                            inputModule = None, scenarioName = None,
                            scenarioFunc = None, scenarioArgs = None, couchURL = None,
                            couchDBName = None, configDoc = None, splitAlgo = "FileBased",
                            splitArgs = {'files_per_job': 1}, seeding = None, totalEvents = None,
                            userDN = None, asyncDest = None, stepType = "CMSSW",
                            userSandbox = None, userFiles = []):

        """
        _setupProcessingTask_

        Given an empty task add cmsRun, stagOut and logArch steps.  Configure
        the input depending on the method parameters:
          inputDataset not empty: This is a top level processing task where the
            input will be fed in by DBS.  Setup the whitelists and blacklists.
          inputDataset empty: This processing task will be fed from the output
            of another task.  The inputStep and name of the output module from
            that step (inputModule) must be specified.

        Processing config will be setup as follows:
          configDoc not empty - Use a ConfigCache config, couchURL and
            couchDBName must not be empty.
          configDoc empty - Use a Configuration.DataProcessing config.  The
            scenarioName, scenarioFunc and scenarioArgs parameters must not be
            empty.

        The seeding and totalEvents parameters are only used for production jobs.
        """
        self.addDashboardMonitoring(procTask)
        procTaskCmssw = procTask.makeStep("cmsRun1")
        procTaskCmssw.setStepType(stepType)
        procTaskStageOut = procTaskCmssw.addStep("stageOut1")
        procTaskStageOut.setStepType("StageOut")
        procTaskStageOut.setUserDN(userDN)
        procTaskStageOut.setAsyncDest(asyncDest)
        procTaskLogArch = procTaskCmssw.addStep("logArch1")
        procTaskLogArch.setStepType("LogArchive")
        procTask.applyTemplates()
        procTask.setTaskPriority(self.priority)

        procTask.setTaskLogBaseLFN(self.unmergedLFNBase)
        procTask.setSiteWhitelist(self.siteWhitelist)
        procTask.setSiteBlacklist(self.siteBlacklist)

        newSplitArgs = {}
        for argName in splitArgs.keys():
            newSplitArgs[str(argName)] = splitArgs[argName]

        procTask.setSplittingAlgorithm(splitAlgo, **newSplitArgs)
        procTask.setTaskType(taskType)
        procTask.addGenerator("BasicNaming")
        procTask.addGenerator("BasicCounter")

        if taskType == "Production":
            procTask.addGenerator(seeding)
            procTask.addProduction(totalevents = totalEvents)
        else:
            if inputDataset != None:
                (primary, processed, tier) = self.inputDataset[1:].split("/")
                procTask.addInputDataset(primary = primary, processed = processed,
                                         tier = tier, dbsurl = self.dbsUrl,
                                         block_blacklist = self.blockBlacklist,
                                         block_whitelist = self.blockWhitelist,
                                         run_blacklist = self.runBlacklist,
                                         run_whitelist = self.runWhitelist)
            else:
                procTask.setInputReference(inputStep, outputModule = inputModule)

        procTaskCmsswHelper = procTaskCmssw.getTypeHelper()
        procTaskStageHelper = procTaskStageOut.getTypeHelper()
        procTaskCmsswHelper.setUserSandbox(userSandbox)
        procTaskCmsswHelper.setUserFiles(userFiles)
        procTaskCmsswHelper.setGlobalTag(self.globalTag)
        procTaskStageHelper.setMinMergeSize(self.minMergeSize, self.maxMergeEvents)
        procTaskCmsswHelper.cmsswSetup(self.frameworkVersion, softwareEnvironment = "",
                                       scramArch = self.scramArch)
        if configDoc != None and configDoc != "":
            procTaskCmsswHelper.setConfigCache(couchURL, configDoc, couchDBName)
        else:
            procTaskCmsswHelper.setDataProcessingConfig(scenarioName, scenarioFunc,
                                                        **scenarioArgs)

        configOutput = self.determineOutputModules(scenarioName, scenarioArgs,
                                                   configDoc, couchURL, couchDBName)
        outputModules = {}
        for outputModuleName in configOutput.keys():
            outputModule = self.addOutputModule(procTask, outputModuleName,
                                                configOutput[outputModuleName]["dataTier"],
                                                configOutput[outputModuleName]["filterName"])
            outputModules[outputModuleName] = outputModule

        return outputModules

    def addOutputModule(self, parentTask, outputModuleName, dataTier, filterName):
        """
        _addOutputModule_

        Add an output module to the given processing task.
        """
        if parentTask.name() == 'Analysis':
            # TODO in case of user data need to implement policy to define
            #  1  processedDatasetName
            #  2  primaryDatasetName
            #  ( 3  dataTier should be always 'USER'.)
            #  4 then we'll know how to deal with Merge
            dataTier = 'USER'
            processedDatasetName = None
            unmergedLFN = self.userUnmergedLFN
            mergedLFN = None
        else:
            if filterName != None and filterName != "":
                processedDatasetName = "%s-%s-%s" % (self.acquisitionEra, filterName,
                                                     self.processingVersion)
                processingString = "%s-%s" % (filterName, self.processingVersion)
            else:
                processedDatasetName = "%s-%s" % (self.acquisitionEra,
                                                  self.processingVersion)
                processingString = "%s" % (self.processingVersion)

            unmergedLFN = "%s/%s/%s/%s/%s" % (self.unmergedLFNBase, self.acquisitionEra,
                                              self.inputPrimaryDataset, dataTier,
                                              processingString)
            mergedLFN = "%s/%s/%s/%s/%s" % (self.mergedLFNBase, self.acquisitionEra,
                                            self.inputPrimaryDataset, dataTier,
                                            processingString)
            lfnBase(unmergedLFN)
            lfnBase(mergedLFN)

        cmsswStep = parentTask.getStep("cmsRun1")
        cmsswStepHelper = cmsswStep.getTypeHelper()
        cmsswStepHelper.addOutputModule(outputModuleName,
                                        primaryDataset = self.inputPrimaryDataset,
                                        processedDataset = processedDatasetName,
                                        dataTier = dataTier,
                                        filterName = filterName,
                                        lfnBase = unmergedLFN,
                                        mergedLFNBase = mergedLFN)

        return {"dataTier": dataTier, "processedDataset": processedDatasetName,
                "filterName": filterName}

    def addLogCollectTask(self, parentTask, taskName = "LogCollect"):
        """
        _addLogCollectTask_

        Create a LogCollect task for log archives that are produced by the
        parent task.
        """
        logCollectTask = parentTask.addTask(taskName)
        self.addDashboardMonitoring(logCollectTask)
        logCollectStep = logCollectTask.makeStep("logCollect1")
        logCollectStep.setStepType("LogCollect")
        logCollectTask.applyTemplates()
        logCollectTask.setSplittingAlgorithm("EndOfRun", files_per_job = 500)
        logCollectTask.addGenerator("BasicNaming")
        logCollectTask.addGenerator("BasicCounter")
        logCollectTask.setTaskType("LogCollect")

        parentTaskLogArch = parentTask.getStep("logArch1")
        logCollectTask.setInputReference(parentTaskLogArch, outputModule = "logArchive")
        return

    def addMergeTask(self, parentTask, parentTaskSplitting, parentOutputModule,
                     dataTier, filterName, processedDatasetName):
        """
        _addMergeTask_

        Create a merge task for files produced by the parent task.
        """
        mergeTask = parentTask.addTask("%sMerge%s" % (parentTask.name(), parentOutputModule))
        self.addDashboardMonitoring(mergeTask)
        mergeTaskCmssw = mergeTask.makeStep("cmsRun1")
        mergeTaskCmssw.setStepType("CMSSW")

        mergeTaskStageOut = mergeTaskCmssw.addStep("stageOut1")
        mergeTaskStageOut.setStepType("StageOut")
        mergeTaskLogArch = mergeTaskCmssw.addStep("logArch1")
        mergeTaskLogArch.setStepType("LogArchive")

        mergeTask.setTaskLogBaseLFN(self.unmergedLFNBase)
        self.addLogCollectTask(mergeTask, taskName = "%s%sMergeLogCollect" % (parentTask.name(), parentOutputModule))

        mergeTask.addGenerator("BasicNaming")
        mergeTask.addGenerator("BasicCounter")
        mergeTask.setTaskType("Merge")
        mergeTask.applyTemplates()
        mergeTask.setTaskPriority(self.priority + 5)

        if parentTaskSplitting == "EventBased" and parentTask.taskType() != "Production":
            splitAlgo = "WMBSMergeBySize"
        else:
            splitAlgo = "ParentlessMergeBySize"

        mergeTask.setSplittingAlgorithm(splitAlgo,
                                        max_merge_size = self.maxMergeSize,
                                        min_merge_size = self.minMergeSize,
                                        max_merge_events = self.maxMergeEvents,
                                        siteWhitelist = self.siteWhitelist,
                                        siteBlacklist = self.siteBlacklist)

        mergeTaskCmsswHelper = mergeTaskCmssw.getTypeHelper()
        mergeTaskCmsswHelper.cmsswSetup(self.frameworkVersion, softwareEnvironment = "",
                                        scramArch = self.scramArch)
        mergeTaskCmsswHelper.setDataProcessingConfig("cosmics", "merge")

        mergedLFN = "%s/%s/%s/%s/%s" % (self.mergedLFNBase, self.acquisitionEra,
                                        self.inputPrimaryDataset, dataTier,
                                        self.processingVersion)
        mergeTaskCmsswHelper.addOutputModule("Merged",
                                             primaryDataset = self.inputPrimaryDataset,
                                             processedDataset = processedDatasetName,
                                             dataTier = dataTier,
                                             filterName = filterName,
                                             lfnBase = mergedLFN)

        parentTaskCmssw = parentTask.getStep("cmsRun1")
        mergeTask.setInputReference(parentTaskCmssw, outputModule = parentOutputModule)
        self.addCleanupTask(parentTask, parentOutputModule)
        return mergeTask

    def addCleanupTask(self, parentTask, parentOutputModuleName):
        """
        _addCleanupTask_

        Create a cleanup task to delete files produces by the parent task.
        """
        cleanupTask = parentTask.addTask("%sCleanupUnmerged%s" % (parentTask.name(), parentOutputModuleName))
        self.addDashboardMonitoring(cleanupTask)
        cleanupTask.setTaskType("Cleanup")

        parentTaskCmssw = parentTask.getStep("cmsRun1")
        cleanupTask.setInputReference(parentTaskCmssw, outputModule = parentOutputModuleName)
        cleanupTask.setSplittingAlgorithm("SiblingProcessingBased", files_per_job = 50)

        cleanupStep = cleanupTask.makeStep("cleanupUnmerged%s" % parentOutputModuleName)
        cleanupStep.setStepType("DeleteFiles")
        cleanupTask.applyTemplates()
        cleanupTask.setTaskPriority(self.priority + 5)
        return

    def setupPileup(self, task, pileupConfig):
        """
        Support for pileup input for MonteCarlo and RelValMC workloads

        """
        # task is instance of WMTaskHelper (WMTask module)
        # retrieve task helper (cmssw step helper), top step name is cmsRun1
        stepName = task.getTopStepName()
        stepHelper = task.getStepHelper(stepName)
        stepHelper.setupPileup(pileupConfig, self.dbsUrl)
