#!/usr/bin/env python
"""
_StdBase_

Base class with helper functions for standard WMSpec files.
"""
import logging

from WMCore.WMSpec.WMWorkload import newWorkload

from WMCore.Cache.WMConfigCache import ConfigCache, ConfigCacheException
from WMCore.Lexicon import lfnBase, identifier, procversion
from WMCore.WMException import WMException
from WMCore.Services.Dashboard.DashboardReporter import DashboardReporter
from WMCore.Configuration import ConfigSection

analysisTaskTypes = ['Analysis', 'PrivateMC']

class WMSpecFactoryException(WMException):
    """
    _WMSpecFactoryException_

    This exception will be raised by validation functions if
    the code fails validation.  It will then be changed into
    a proper HTTPError in the ReqMgr, with the message you enter
    used as the message for farther up the line.
    """
    pass

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
        self.owner = "unknown"
        self.owner_dn = "unknown"
        self.group = "unknown"
        self.owner_vogroup = "DEFAULT"
        self.owner_vorole = "DEFAULT"
        self.acquisitionEra = None
        self.scramArch = None
        self.inputPrimaryDataset = None
        self.inputProcessedDataset = None
        self.inputDataTier = None
        self.processingVersion = 0
        self.processingString  = None
        self.siteBlacklist = []
        self.siteWhitelist = []
        self.unmergedLFNBase = None
        self.mergedLFNBase = None
        self.minMergeSize = 2147483648
        self.maxMergeSize = 4294967296
        self.maxWaitTime = 24 * 3600
        self.maxMergeEvents = 100000
        self.validStatus = None
        self.includeParents = False
        self.dbsUrl = None
        self.multicore = False
        self.multicoreNCores = 1
        self.dashboardHost = None
        self.dashboardPort = 0
        self.overrideCatalog = None
        self.firstLumi = 1
        self.firstEvent = 1
        self.runNumber = 0
        self.timePerEvent = None
        self.memory = None
        self.sizePerEvent = None
        self.periodicHarvestingInterval = 0
        self.dqmUploadProxy = None
        self.dqmUploadUrl = None
        self.dqmSequences = None
        self.dqmConfigCacheID = None
        self.procScenario = None
        self.enableHarvesting = True
        self.enableNewStageout = False
        return

    def __call__(self, workloadName, arguments):
        """
        __call__

        Look through the arguments that were passed into the workload's call
        method and pull out any that are setup by this base class.
        """
        self.workloadName = workloadName
        self.priority = arguments.get("Priority", 0)
        self.owner = arguments.get("Requestor", "unknown")
        self.owner_dn = arguments.get("RequestorDN", "unknown")
        self.group = arguments.get("Group", "unknown")
        if arguments.has_key('VoGroup'):
            self.owner_vogroup = arguments['VoGroup']
        if arguments.has_key('VoRole'):
            self.owner_vorole = arguments['VoRole']
        self.acquisitionEra = arguments.get("AcquisitionEra", None)
        self.scramArch = arguments.get("ScramArch", None)
        self.processingVersion = int(arguments.get("ProcessingVersion", 0))
        self.processingString = arguments.get("ProcessingString", None)
        self.siteBlacklist = arguments.get("SiteBlacklist", [])
        self.siteWhitelist = arguments.get("SiteWhitelist", [])
        self.unmergedLFNBase = arguments.get("UnmergedLFNBase", "/store/unmerged")
        self.mergedLFNBase = arguments.get("MergedLFNBase", "/store/data")
        self.minMergeSize = arguments.get("MinMergeSize", 2147483648)
        self.maxMergeSize = arguments.get("MaxMergeSize", 4294967296)
        self.maxWaitTime = arguments.get("MaxWaitTime", 24 * 3600)
        self.maxMergeEvents = arguments.get("MaxMergeEvents", 100000)
        self.validStatus = arguments.get("ValidStatus", "PRODUCTION")
        self.dbsUrl = arguments.get("DbsUrl", "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet")
        self.dashboardHost = arguments.get("DashboardHost", "cms-wmagent-job.cern.ch")
        self.dashboardPort = arguments.get("DashboardPort", 8884)
        self.overrideCatalog = arguments.get("OverrideCatalog", None)
        self.runNumber = int(arguments.get("RunNumber", 0))
        self.timePerEvent = float(arguments.get("TimePerEvent", 12))
        self.memory = float(arguments.get("Memory", 2300))
        self.sizePerEvent = float(arguments.get("SizePerEvent", 512))
        self.periodicHarvestingInterval = int(arguments.get("PeriodicHarvest", 0))
        self.dqmUploadProxy = arguments.get("DQMUploadProxy", None)
        self.dqmUploadUrl = arguments.get("DQMUploadUrl", "https://cmsweb.cern.ch/dqm/dev")
        self.dqmSequences = arguments.get("DqmSequences", [])
        self.dqmConfigCacheID = arguments.get("DQMConfigCacheID", None)
        self.procScenario = arguments.get("ProcScenario", None)
        self.enableHarvesting = arguments.get("EnableHarvesting", True)
        self.enableNewStageout = arguments.get("EnableNewStageout", False)

        if arguments.get("IncludeParents", False) == "True":
            self.includeParents = True
        else:
            self.includeParents = False

        if arguments.has_key("Multicore"):
            numCores = arguments.get("Multicore")
            if numCores == None or numCores == "":
                self.multicore = False
            elif numCores == "auto":
                self.multicore = True
                self.multicoreNCores = "auto"
            else:
                self.multicore = True
                self.multicoreNCores = numCores

        return

    def determineOutputModules(self, scenarioFunc = None, scenarioArgs = None,
                               configDoc = None, couchURL = None,
                               couchDBName = None, configCacheUrl = None):
        """
        _determineOutputModules_

        Determine the output module names and associated metadata for the
        given config.
        """
        outputModules = {}
        if configDoc != None and configDoc != "":
            url = configCacheUrl or couchURL
            configCache = ConfigCache(url, couchDBName)
            configCache.loadByID(configDoc)
            outputModules = configCache.getOutputModuleInfo()
        else:
            if 'outputs' in scenarioArgs and scenarioFunc in [ "promptReco", "expressProcessing", "repack" ]:
                for output in scenarioArgs.get('outputs', []):
                    moduleLabel = output['moduleLabel']
                    outputModules[moduleLabel] = { 'dataTier' : output['dataTier'] }
                    if output.has_key('primaryDataset'):
                        outputModules[moduleLabel]['primaryDataset'] = output['primaryDataset']
                    if output.has_key('filterName'):
                        outputModules[moduleLabel]['filterName'] = output['filterName']

            elif 'writeTiers' in scenarioArgs and scenarioFunc == "promptReco":
                for dataTier in scenarioArgs.get('writeTiers'):
                    moduleLabel = "%soutput" % dataTier
                    outputModules[moduleLabel] = { 'dataTier' : dataTier }

            elif scenarioFunc == "alcaSkim":
                for alcaSkim in scenarioArgs.get('skims',[]):
                    moduleLabel = "ALCARECOStream%s" % alcaSkim
                    if alcaSkim == "PromptCalibProd":
                        dataTier = "ALCAPROMPT"
                    else:
                        dataTier = "ALCARECO"
                    outputModules[moduleLabel] = { 'dataTier' : dataTier,
                                                   'primaryDataset' : scenarioArgs.get('primaryDataset'),
                                                   'filterName' : alcaSkim }

        return outputModules

    def addDashboardMonitoring(self, task):
        """
        _addDashboardMonitoring_

        Add dashboard monitoring for the given task.
        """
        #A gigabyte defined as 1024^3 (assuming RSS and VSize is in KiByte)
        gb = 1024.0 * 1024.0
        #Default timeout defined in CMS policy
        softTimeout = 47.0 * 3600.0 + 40.0 * 60.0
        hardTimeout = 47.0 * 3600.0 + 45.0 * 60.0

        monitoring = task.data.section_("watchdog")
        monitoring.interval = 300
        monitoring.monitors = ["DashboardMonitor", "PerformanceMonitor"]
        monitoring.section_("DashboardMonitor")
        monitoring.DashboardMonitor.destinationHost = self.dashboardHost
        monitoring.DashboardMonitor.destinationPort = self.dashboardPort
        monitoring.section_("PerformanceMonitor")
        monitoring.PerformanceMonitor.maxRSS = 2.3 * gb
        monitoring.PerformanceMonitor.maxVSize = 2.3 * gb
        monitoring.PerformanceMonitor.softTimeout = softTimeout
        monitoring.PerformanceMonitor.hardTimeout = hardTimeout
        return task


    def reportWorkflowToDashboard(self, dashboardActivity):
        """
        _reportWorkflowToDashboard_
        Gathers workflow information from the arguments and reports it to the
        dashboard
        """
        try:
        #Create a fake config
            conf = ConfigSection()
            conf.section_('DashboardReporter')
            conf.DashboardReporter.dashboardHost = self.dashboardHost
            conf.DashboardReporter.dashboardPort = self.dashboardPort

            #Create the reporter
            reporter = DashboardReporter(conf)

            #Assemble the info
            workflow = {}
            workflow['name'] = self.workloadName
            workflow['application'] = self.frameworkVersion
            workflow['TaskType'] = dashboardActivity
            #Let's try to build information about the inputDataset
            dataset = 'DoesNotApply'
            if hasattr(self, 'inputDataset'):
                dataset = self.inputDataset
            workflow['datasetFull'] = dataset
            workflow['user'] = 'cmsdataops'

            #Send the workflow info
            reporter.addTask(workflow)
        except:
            #This is not critical, if it fails just leave it be
            logging.error("There was an error with dashboard reporting")


    def createWorkload(self):
        """
        _createWorkload_

        Create a new workload.
        """
        ownerProps = {'dn': self.owner_dn, 'vogroup': self.owner_vogroup, 'vorole': self.owner_vorole}

        workload = newWorkload(self.workloadName)
        workload.setOwnerDetails(self.owner, self.group, ownerProps)
        workload.setStartPolicy("DatasetBlock", SliceType = "NumberOfFiles", SliceSize = 1)
        workload.setEndPolicy("SingleShot")
        workload.setAcquisitionEra(acquisitionEras = self.acquisitionEra)
        workload.setProcessingVersion(processingVersions = self.processingVersion)
        workload.setProcessingString(processingStrings = self.processingString)
        workload.setValidStatus(validStatus = self.validStatus)
        return workload

    def setupProcessingTask(self, procTask, taskType, inputDataset = None, inputStep = None,
                            inputModule = None, scenarioName = None,
                            scenarioFunc = None, scenarioArgs = None, couchURL = None,
                            couchDBName = None, configDoc = None, splitAlgo = "LumiBased",
                            splitArgs = {'lumis_per_job': 8}, seeding = None,
                            totalEvents = None, eventsPerLumi = None,
                            userDN = None, asyncDest = None, owner_vogroup = "DEFAULT",
                            owner_vorole = "DEFAULT", stepType = "CMSSW",
                            userSandbox = None, userFiles = [], primarySubType = None,
                            forceMerged = False, forceUnmerged = False,
                            configCacheUrl = None, timePerEvent = None, memoryReq = None,
                            sizePerEvent = None):
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
          if configCacheUrl is not empty, use that plus couchDBName + configDoc if not empty  

        The seeding and totalEvents parameters are only used for production jobs.
        """
        self.addDashboardMonitoring(procTask)
        procTaskCmssw = procTask.makeStep("cmsRun1")
        procTaskCmssw.setStepType(stepType)
        procTaskStageOut = procTaskCmssw.addStep("stageOut1")
        procTaskStageOut.setStepType("StageOut")
        procTaskStageOut.setUserDN(userDN)
        procTaskStageOut.setAsyncDest(asyncDest)
        procTaskStageOut.setUserRoleAndGroup(owner_vogroup, owner_vorole)
        procTaskStageOut.setNewStageoutOverride(self.enableNewStageout)
        procTaskLogArch = procTaskCmssw.addStep("logArch1")
        procTaskLogArch.setStepType("LogArchive")
        procTaskLogArch.setNewStageoutOverride(self.enableNewStageout)
        
        procTask.applyTemplates()

        procTask.setTaskLogBaseLFN(self.unmergedLFNBase)
        procTask.setSiteWhitelist(self.siteWhitelist)
        procTask.setSiteBlacklist(self.siteBlacklist)

        newSplitArgs = {}
        for argName in splitArgs.keys():
            newSplitArgs[str(argName)] = splitArgs[argName]

        procTask.setSplittingAlgorithm(splitAlgo, **newSplitArgs)
        procTask.setJobResourceInformation(timePerEvent = timePerEvent,
                                           sizePerEvent = sizePerEvent,
                                           memoryReq = memoryReq)
        procTask.setTaskType(taskType)
        procTask.setProcessingVersion(self.processingVersion)
        procTask.setAcquisitionEra(self.acquisitionEra)
        procTask.setProcessingString(self.processingString)

        if taskType in ["Production", 'PrivateMC'] and totalEvents != None:
            procTask.addGenerator(seeding)
            procTask.addProduction(totalevents = totalEvents)
            procTask.setFirstEventAndLumi(firstEvent = self.firstEvent,
                                          firstLumi = self.firstLumi)
        else:
            if inputDataset != None:
                (primary, processed, tier) = self.inputDataset[1:].split("/")
                procTask.addInputDataset(primary = primary, processed = processed,
                                         tier = tier, dbsurl = self.dbsUrl,
                                         block_blacklist = self.blockBlacklist,
                                         block_whitelist = self.blockWhitelist,
                                         run_blacklist = self.runBlacklist,
                                         run_whitelist = self.runWhitelist)
            elif inputStep != None and inputModule != None:
                procTask.setInputReference(inputStep, outputModule = inputModule)

        if primarySubType:
            procTask.setPrimarySubType(subType = primarySubType)

        procTaskCmsswHelper = procTaskCmssw.getTypeHelper()
        procTaskStageHelper = procTaskStageOut.getTypeHelper()

        if self.multicore:
            # if multicore, poke in the number of cores setting
            procTaskCmsswHelper.setMulticoreCores(self.multicoreNCores)

        procTaskCmsswHelper.setUserSandbox(userSandbox)
        procTaskCmsswHelper.setUserFiles(userFiles)
        procTaskCmsswHelper.setGlobalTag(self.globalTag)
        procTaskCmsswHelper.setOverrideCatalog(self.overrideCatalog)
        procTaskCmsswHelper.setErrorDestinationStep(stepName = procTaskLogArch.name())

        if forceMerged:
            procTaskStageHelper.setMinMergeSize(0, 0)
        elif forceUnmerged:
            procTaskStageHelper.disableStraightToMerge()
        else:
            procTaskStageHelper.setMinMergeSize(self.minMergeSize, self.maxMergeEvents)

        procTaskCmsswHelper.cmsswSetup(self.frameworkVersion, softwareEnvironment = "",
                                       scramArch = self.scramArch)
        procTaskCmsswHelper.setEventsPerLumi(eventsPerLumi)

        configOutput = self.determineOutputModules(scenarioFunc, scenarioArgs,
                                                   configDoc, couchURL, couchDBName,
                                                   configCacheUrl=configCacheUrl)
        outputModules = {}
        for outputModuleName in configOutput.keys():
            outputModule = self.addOutputModule(procTask,
                                                outputModuleName,
                                                configOutput[outputModuleName].get('primaryDataset',
                                                                                   self.inputPrimaryDataset),
                                                configOutput[outputModuleName]['dataTier'],
                                                configOutput[outputModuleName].get('filterName', None),
                                                forceMerged = forceMerged, forceUnmerged = forceUnmerged)
            outputModules[outputModuleName] = outputModule

        if configDoc != None and configDoc != "":
            url = configCacheUrl or couchURL
            procTaskCmsswHelper.setConfigCache(url, configDoc, couchDBName)
        else:
            # delete dataset information from scenarioArgs
            if 'outputs' in scenarioArgs:
                for output in scenarioArgs['outputs']:
                    if 'primaryDataset' in output:
                        del output['primaryDataset']
            if 'primaryDataset' in scenarioArgs:
                del scenarioArgs['primaryDataset']

            procTaskCmsswHelper.setDataProcessingConfig(scenarioName, scenarioFunc,
                                                        **scenarioArgs)
        return outputModules

    def addOutputModule(self, parentTask, outputModuleName,
                        primaryDataset, dataTier, filterName,
                        stepName = "cmsRun1", forceMerged = False,
                        forceUnmerged = False):
        """
        _addOutputModule_

        Add an output module to the given processing task.

        """
        haveFilterName = (filterName != None and filterName != "")
        haveProcString = (self.processingString != None and self.processingString != "")
        haveRunNumber  = (self.runNumber != None and self.runNumber > 0)

        processedDataset = "%s-" % self.acquisitionEra
        if haveFilterName:
            processedDataset += "%s-" % filterName
        if haveProcString:
            processedDataset += "%s-" % self.processingString
        processedDataset += "v%i" % self.processingVersion

        if haveProcString:
            processingLFN = "%s-v%i" % (self.processingString, self.processingVersion)
        else:
            processingLFN = "v%i" % self.processingVersion

        if haveRunNumber:
            stringRunNumber = str(self.runNumber).zfill(9)
            runSections = [stringRunNumber[i:i+3] for i in range(0, 9, 3)]
            runLFN = "/".join(runSections)


        if parentTask.name() in analysisTaskTypes:

            # dataTier for user data is always USER
            dataTier = "USER"

            # output for user data is always unmerged
            forceUnmerged = True

            unmergedLFN = "%s/%s" % (self.unmergedLFNBase, primaryDataset)

            if haveFilterName:
                unmergedLFN += "/%s-%s" % (self.acquisitionEra, filterName)
            else:
                unmergedLFN += "/%s" % self.acquisitionEra

            unmergedLFN += "/%s" % processingLFN

            lfnBase(unmergedLFN)

        else:

            unmergedLFN = "%s/%s/%s/%s" % (self.unmergedLFNBase,
                                           self.acquisitionEra,
                                           primaryDataset, dataTier)
            mergedLFN = "%s/%s/%s/%s" % (self.mergedLFNBase,
                                         self.acquisitionEra,
                                         primaryDataset, dataTier)

            if haveFilterName:
                unmergedLFN += "/%s-%s" % (filterName, processingLFN)
                mergedLFN += "/%s-%s" % (filterName, processingLFN)
            else:
                unmergedLFN += "/%s" % processingLFN
                mergedLFN += "/%s" % processingLFN

            if haveRunNumber:
                unmergedLFN += "/%s" % runLFN
                mergedLFN += "/%s" % runLFN

            lfnBase(unmergedLFN)
            lfnBase(mergedLFN)

        isTransient = True

        if forceMerged:
            unmergedLFN = mergedLFN
            isTransient = False
        elif forceUnmerged:
            mergedLFN = unmergedLFN

        cmsswStep = parentTask.getStep(stepName)
        cmsswStepHelper = cmsswStep.getTypeHelper()
        cmsswStepHelper.addOutputModule(outputModuleName,
                                        primaryDataset = primaryDataset,
                                        processedDataset = processedDataset,
                                        dataTier = dataTier,
                                        filterName = filterName,
                                        lfnBase = unmergedLFN,
                                        mergedLFNBase = mergedLFN,
                                        transient = isTransient)

        return {"primaryDataset": primaryDataset,
                "dataTier": dataTier,
                "processedDataset": processedDataset,
                "filterName": filterName}

    def addLogCollectTask(self, parentTask, taskName = "LogCollect", filesPerJob = 500):
        """
        _addLogCollectTask_

        Create a LogCollect task for log archives that are produced by the
        parent task.
        """
        logCollectTask = parentTask.addTask(taskName)
        self.addDashboardMonitoring(logCollectTask)
        logCollectStep = logCollectTask.makeStep("logCollect1")
        logCollectStep.setStepType("LogCollect")
        logCollectStep.setNewStageoutOverride(self.enableNewStageout)
        logCollectTask.applyTemplates()
        logCollectTask.setSplittingAlgorithm("MinFileBased", files_per_job = filesPerJob)
        logCollectTask.setTaskType("LogCollect")

        parentTaskLogArch = parentTask.getStep("logArch1")
        logCollectTask.setInputReference(parentTaskLogArch, outputModule = "logArchive")
        return logCollectTask

    def addMergeTask(self, parentTask, parentTaskSplitting, parentOutputModuleName,
                     parentStepName = "cmsRun1", doLogCollect = True,
                     lfn_counter = 0):
        """
        _addMergeTask_

        Create a merge task for files produced by the parent task.
        """
        mergeTask = parentTask.addTask("%sMerge%s" % (parentTask.name(), parentOutputModuleName))
        self.addDashboardMonitoring(mergeTask)
        mergeTaskCmssw = mergeTask.makeStep("cmsRun1")
        mergeTaskCmssw.setStepType("CMSSW")

        mergeTaskStageOut = mergeTaskCmssw.addStep("stageOut1")
        mergeTaskStageOut.setStepType("StageOut")
        mergeTaskStageOut.setNewStageoutOverride(self.enableNewStageout)

        mergeTaskLogArch = mergeTaskCmssw.addStep("logArch1")
        mergeTaskLogArch.setStepType("LogArchive")
        mergeTaskLogArch.setNewStageoutOverride(self.enableNewStageout)


        mergeTask.setTaskLogBaseLFN(self.unmergedLFNBase)
        if doLogCollect:
            self.addLogCollectTask(mergeTask, taskName = "%s%sMergeLogCollect" % (parentTask.name(), parentOutputModuleName))

        mergeTask.setTaskType("Merge")
        mergeTask.applyTemplates()

        if parentTaskSplitting == "EventBased" and parentTask.taskType() != "Production":
            splitAlgo = "WMBSMergeBySize"
        else:
            splitAlgo = "ParentlessMergeBySize"

        parentTaskCmssw = parentTask.getStep(parentStepName)
        parentOutputModule = parentTaskCmssw.getOutputModule(parentOutputModuleName)

        mergeTask.setInputReference(parentTaskCmssw, outputModule = parentOutputModuleName)

        mergeTaskCmsswHelper = mergeTaskCmssw.getTypeHelper()
        mergeTaskCmsswHelper.cmsswSetup(self.frameworkVersion, softwareEnvironment = "",
                                        scramArch = self.scramArch)

        mergeTaskCmsswHelper.setErrorDestinationStep(stepName = mergeTaskLogArch.name())
        mergeTaskCmsswHelper.setGlobalTag(self.globalTag)
        mergeTaskCmsswHelper.setOverrideCatalog(self.overrideCatalog)

        mergeTask.setSplittingAlgorithm(splitAlgo,
                                        max_merge_size = self.maxMergeSize,
                                        min_merge_size = self.minMergeSize,
                                        max_merge_events = self.maxMergeEvents,
                                        max_wait_time = self.maxWaitTime,
                                        siteWhitelist = self.siteWhitelist,
                                        siteBlacklist = self.siteBlacklist,
                                        initial_lfn_counter = lfn_counter)

        if getattr(parentOutputModule, "dataTier") == "DQMROOT":
            mergeTaskCmsswHelper.setDataProcessingConfig("do_not_use", "merge",
                                                         newDQMIO = True)
        else:
            mergeTaskCmsswHelper.setDataProcessingConfig("do_not_use", "merge")

        self.addOutputModule(mergeTask, "Merged",
                             primaryDataset = getattr(parentOutputModule, "primaryDataset"),
                             dataTier = getattr(parentOutputModule, "dataTier"),
                             filterName = getattr(parentOutputModule, "filterName"),
                             forceMerged = True)

        self.addCleanupTask(parentTask, parentOutputModuleName)
        if self.enableHarvesting and getattr(parentOutputModule, "dataTier") in ["DQMROOT", "DQM"]:
            self.addDQMHarvestTask(mergeTask, "Merged",
                                   uploadProxy = self.dqmUploadProxy,
                                   periodic_harvest_interval= self.periodicHarvestingInterval,
                                   doLogCollect = doLogCollect)
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
        return

    def addDQMHarvestTask(self, parentTask, parentOutputModuleName,
                          uploadProxy = None, periodic_harvest_interval = 0,
                          parentStepName = "cmsRun1", doLogCollect = True):
        """
        _addDQMHarvestTask_

        Create a DQM harvest task to harvest the files produces by the parent task.
        """
        harvestTask = parentTask.addTask("%sDQMHarvest%s" % (parentTask.name(), parentOutputModuleName))
        self.addDashboardMonitoring(harvestTask)
        harvestTaskCmssw = harvestTask.makeStep("cmsRun1")
        harvestTaskCmssw.setStepType("CMSSW")

        harvestTaskUpload = harvestTaskCmssw.addStep("upload1")
        harvestTaskUpload.setStepType("DQMUpload")
        harvestTaskLogArch = harvestTaskCmssw.addStep("logArch1")
        harvestTaskLogArch.setStepType("LogArchive")

        harvestTask.setTaskLogBaseLFN(self.unmergedLFNBase)
        if doLogCollect:
            self.addLogCollectTask(harvestTask, taskName = "%s%sDQMHarvestLogCollect" % (parentTask.name(), parentOutputModuleName))

        harvestTask.setTaskType("Harvesting")
        harvestTask.applyTemplates()

        harvestTaskCmsswHelper = harvestTaskCmssw.getTypeHelper()
        harvestTaskCmsswHelper.cmsswSetup(self.frameworkVersion, softwareEnvironment = "",
                                          scramArch = self.scramArch)

        harvestTaskCmsswHelper.setErrorDestinationStep(stepName = harvestTaskLogArch.name())
        harvestTaskCmsswHelper.setGlobalTag(self.globalTag)
        harvestTaskCmsswHelper.setOverrideCatalog(self.overrideCatalog)

        harvestTaskCmsswHelper.setUserLFNBase("/")

        parentTaskCmssw = parentTask.getStep(parentStepName)
        parentOutputModule = parentTaskCmssw.getOutputModule(parentOutputModuleName)

        harvestTask.setInputReference(parentTaskCmssw, outputModule = parentOutputModuleName)

        harvestTask.setSplittingAlgorithm("Harvest",
                                          periodic_harvest_interval = periodic_harvest_interval)

        datasetName = "/%s/%s/%s" % (getattr(parentOutputModule, "primaryDataset"),
                                     getattr(parentOutputModule, "processedDataset"),
                                     getattr(parentOutputModule, "dataTier"))

        if self.dqmConfigCacheID is not None:
            if getattr(self, "configCacheUrl", None) is not None:
                harvestTaskCmsswHelper.setConfigCache(self.configCacheUrl, self.dqmConfigCacheID, self.couchDBName)
            else:
                harvestTaskCmsswHelper.setConfigCache(self.couchURL, self.dqmConfigCacheID, self.couchDBName)
            harvestTaskCmsswHelper.setDatasetName(datasetName)
        else:
            if getattr(parentOutputModule, "dataTier") == "DQMROOT":
                harvestTaskCmsswHelper.setDataProcessingConfig(self.procScenario, "dqmHarvesting",
                                                               globalTag = self.globalTag,
                                                               datasetName = datasetName,
                                                               runNumber = self.runNumber,
                                                               dqmSeq = self.dqmSequences,
                                                               newDQMIO = True)
            else:
                harvestTaskCmsswHelper.setDataProcessingConfig(self.procScenario, "dqmHarvesting",
                                                               globalTag = self.globalTag,
                                                               datasetName = datasetName,
                                                               runNumber = self.runNumber,
                                                               dqmSeq = self.dqmSequences)

        harvestTaskUploadHelper = harvestTaskUpload.getTypeHelper()
        harvestTaskUploadHelper.setProxyFile(uploadProxy)
        harvestTaskUploadHelper.setServerURL(self.dqmUploadUrl)
        return

    def setupPileup(self, task, pileupConfig):
        """
        _setupPileup_

        Setup pileup for every CMSSW step in the task.
        """
        for stepName in task.listAllStepNames():
            step = task.getStep(stepName)
            if step.stepType() != "CMSSW":
                continue
            stepHelper = task.getStepHelper(stepName)
            stepHelper.setupPileup(pileupConfig, self.dbsUrl)

        return

    def validateSchema(self, schema):
        """
        _validateSchema_

        Validate the schema prior to building the workload
        This function should be overridden by individual specs

        If something breaks, raise a WMSpecFactoryException.  A message
        in that excpetion will be transferred to an HTTP Error later on.
        """
        return

    def validateWorkload(self, workload):
        """
        _validateWorkload_

        Just in case you have something that you want to validate
        after the workload gets created, this is where you should
        put it.

        If something breaks, raise a WMSpecFactoryException.  A message
        in that exception will be transferred to an HTTP Error later on.
        """
        pass

    def factoryWorkloadConstruction(self, workloadName, arguments):
        """
        _factoryWorkloadConstruction_

        Master build for ReqMgr - builds the entire workload
        and also performs the proper validation.

        Named this way so that nobody else will try to use this name.
        """
        self.masterValidation(schema = arguments)
        self.validateSchema(schema = arguments)
        workload = self.__call__(workloadName = workloadName, arguments = arguments)
        self.validateWorkload(workload)

        return workload

    def masterValidation(self, schema):
        """
        _masterValidation_

        This is validation for global inputs that have to be implemented for
        multiple types of workflows in the exact same way.

        Usually used for type-checking, etc.
        """

        try:
            procversion(str(schema.get("ProcessingVersion", 0)))
        except AssertionError:
            self.raiseValidationException(msg = "Non-integer castable ProcessingVersion found")

        performanceFields = ['TimePerEvent', 'Memory', 'SizePerEvent']

        for field in performanceFields:
            self._validatePerformanceField(field, schema)

        if schema.get("EnableHarvesting", True):
            # If enableHarvesting requested, then a few conditions must be met
            if "DQMConfigCacheID" not in schema and "ProcScenario" not in schema:
                self.raiseValidationException("Harvesting was requested, but no scenario or config cache ID was given")
            if "DQMConfigCacheID" in schema:
                if "ConfigCacheUrl" not in schema and "CouchURL" not in schema:
                    self.raiseValidationException("Harvesting was requested, but no couch url was given")
                if "CouchDBName" not in schema:
                    self.raiseValidationException("Harvesting was requested, but no couchdb name was given")
                couchUrl = schema.get("ConfigCacheUrl", None) or schema["CouchURL"]
                self.validateConfigCacheExists(configID = schema["DQMConfigCacheID"],
                                                    couchURL = couchUrl,
                                                    couchDBName = schema["CouchDBName"])

        return

    def _validatePerformanceField(self, field, schema):
        """
        __validatePerformanceField_

        Validates an integer field, that is mandatory. So it raises a validation
        exception if the field is not in the schema or is not integer-castable.
        """
        try:
            int(schema[field])
        except KeyError:
            self.raiseValidationException(msg = "The %s must be specified" % field)
        except ValueError:
            self.raiseValidationException(msg = "Please specify a valid %s" % field)

    def requireValidateFields(self, fields, schema, validate = False):
        """
        _requireValidateFields_

        Standard tool for StdBase derived classes to make sure
        that a schema has a certain set of valid fields.  Uses WMCore.Lexicon.identifier

        To be used in validateWorkload()
        """

        for field in fields:
            if not field in schema.keys():
                msg = "Missing required field '%s' in workload validation!" % field
                self.raiseValidationException(msg = msg)
            if schema[field] == None:
                msg = "NULL value for required field %s!" % field
                self.raiseValidationException(msg = msg)
            if validate:
                try:
                    identifier(candidate = schema[field])
                except AssertionError, ex:
                    msg = "Schema value for field '%s' failed Lexicon validation" % field
                    self.raiseValidationException(msg = msg)
        return

    def raiseValidationException(self, msg):
        """
        _raiseValidationException_

        Inbuilt method for raising exception so people don't have
        to import WMSpecFactoryException all over the place.
        """

        logging.error("About to raise exception %s" % msg)
        raise WMSpecFactoryException(message = msg)

    def validateConfigCacheExists(self, configID, couchURL, couchDBName,
                                  getOutputModules = False):
        """
        _validateConfigCacheExists_

        If we have a configCache, we should probably try and load it.
        """

        if configID == '' or configID == ' ':
            self.raiseValidationException(msg = "ConfigCacheID is invalid and cannot be loaded")
            
        configCache = ConfigCache(dbURL = couchURL, couchDBName = couchDBName,
                                  id = configID)
        try:
            configCache.loadByID(configID = configID)
        except ConfigCacheException, ex:
            self.raiseValidationException(msg = "Failure to load ConfigCache while validating workload")

        duplicateCheck = {}
        try:
            outputModuleInfo = configCache.getOutputModuleInfo()
        except Exception, ex:
            # Something's gone wrong with trying to open the configCache
            msg = "Error in getting output modules from ConfigCache during workload validation.  Check ConfigCache formatting!"
            self.raiseValidationException(msg = msg)
        for outputModule in outputModuleInfo.values():
            dataTier   = outputModule.get('dataTier', None)
            filterName = outputModule.get('filterName', None)
            if not dataTier:
                self.raiseValidationException(msg = "No DataTier in output module.")

            # Add dataTier to duplicate dictionary
            if not dataTier in duplicateCheck.keys():
                duplicateCheck[dataTier] = []
            if filterName in duplicateCheck[dataTier]:
                # Then we've seen this combination before
                self.raiseValidationException(msg = "Duplicate dataTier/filterName combination.")
            else:
                duplicateCheck[dataTier].append(filterName)

        if getOutputModules:
            return outputModuleInfo

        return
