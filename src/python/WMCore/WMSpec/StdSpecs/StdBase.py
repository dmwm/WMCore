#!/usr/bin/env python
"""
_StdBase_

Base class with helper functions for standard WMSpec files.
"""
import logging

from WMCore.Cache.WMConfigCache import ConfigCache, ConfigCacheException
from WMCore.Configuration import ConfigSection
from WMCore.Lexicon import lfnBase, identifier, acqname, cmsswversion, cmsname, couchurl
from WMCore.Services.Dashboard.DashboardReporter import DashboardReporter
from WMCore.WMSpec.WMSpecErrors import WMSpecFactoryException
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.WMWorkloadTools import makeList, makeLumiList, strToBool, checkDBSURL, validateArgumentsCreate


analysisTaskTypes = ['Analysis', 'PrivateMC']

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
        These parameters in the init are set all to None, then they wil
        be overwritten in the call.

        These parameters can be changed after the workflow has been created by
        the methods in the WMWorkloadHelper class.
        """
        argumentDefinition = self.getWorkloadArguments()
        for arg in argumentDefinition:
            setattr(self, argumentDefinition[arg]["attr"], None)

        # Internal parameters
        self.workloadName = None
        self.multicoreNCores = None
        self.schema = None
        self.config_cache = {}

        return

    def __call__(self, workloadName, arguments):
        """
        __call__

        Look through the arguments that were passed into the workload's call
        method and pull out any that are setup by this base class.
        """
        self.workloadName = workloadName
        self.schema = {}
        argumentDefinition = self.getWorkloadArguments()
        for arg in argumentDefinition:
            try:
                if arg in arguments:
                    if arguments[arg] is None:
                        setattr(self, argumentDefinition[arg]["attr"], arguments[arg])
                    else:
                        value = argumentDefinition[arg]["type"](arguments[arg])
                        setattr(self, argumentDefinition[arg]["attr"], value)
                        self.schema[arg] = value
                elif argumentDefinition[arg]["optional"]:
                    defaultValue = argumentDefinition[arg]["default"]
                    setattr(self, argumentDefinition[arg]["attr"], defaultValue)
                    self.schema[arg] = defaultValue
            except Exception as ex:
                raise WMSpecFactoryException("parameter %s: %s" % (arg, str(ex)))

        # Definition of parameters that depend on the value of others
        if hasattr(self, "multicore") and self.multicore:
            self.multicoreNCores = int(self.multicore)
            self.multicore = True

        return

    def determineOutputModules(self, scenarioFunc = None, scenarioArgs = None,
                               configDoc = None, couchURL = None,
                               couchDBName = None, configCacheUrl = None):
        """
        _determineOutputModules_

        Determine the output module names and associated metadata for the
        given config.
        """
        # set default scenarioArgs to empty dictionary if it is None.
        scenarioArgs = scenarioArgs or {}

        outputModules = {}
        if configDoc != None and configDoc != "":
            url = configCacheUrl or couchURL
            if  (url, couchDBName) in self.config_cache:
                configCache = self.config_cache[(url, couchDBName)]
            else:
                configCache = ConfigCache(url, couchDBName, True)
                self.config_cache[(url, couchDBName)] = configCache
            #TODO: need to change to DataCache
            #configCache.loadDocument(configDoc)
            configCache.loadByID(configDoc)
            outputModules = configCache.getOutputModuleInfo()
        else:
            if 'outputs' in scenarioArgs and scenarioFunc in [ "promptReco", "expressProcessing", "repack" ]:

                for output in scenarioArgs.get('outputs', []):

                    moduleLabel = output['moduleLabel']
                    outputModules[moduleLabel] = { 'dataTier' : output['dataTier'] }
                    if 'primaryDataset' in output:
                        outputModules[moduleLabel]['primaryDataset'] = output['primaryDataset']
                    if 'filterName' in output:
                        outputModules[moduleLabel]['filterName'] = output['filterName']

                for physicsSkim in scenarioArgs.get('PhysicsSkims',[]):

                    skimToDataTier = { 'LogError' : 'RAW-RECO',
                                       'LogErrorMonitor' : 'USER',
                                       'ZElectron' : 'RAW-RECO',
                                       'ZMu' : 'RAW-RECO',
                                       'MuTau' : 'RAW-RECO',
                                       'TopMuEG' : 'RAW-RECO',
                                       'EcalActivity' : 'RAW-RECO',
                                       'CosmicSP' : 'RAW-RECO',
                                       'CosmicTP' : 'RAW-RECO'
                                       }
                    dataTier = skimToDataTier.get(physicsSkim, 'USER')
                    moduleLabel = "SKIMStream%s" % physicsSkim
                    outputModules[moduleLabel] = { 'dataTier' : dataTier,
                                                   'filterName' : physicsSkim }

            elif scenarioFunc == "alcaSkim":

                for alcaSkim in scenarioArgs.get('skims',[]):
                    moduleLabel = "ALCARECOStream%s" % alcaSkim
                    if alcaSkim.startswith("PromptCalibProd"):
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
        workload.setLumiList(lumiLists = self.lumiList)
        workload.setPriority(self.priority)
        workload.setCampaign(self.campaign)
        workload.setRequestType(self.requestType)
        workload.setPrepID(self.prepID)
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
                            sizePerEvent = None, useMulticore = True, applySiteLists = True,
                            taskConf = {}):
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
        # set default scenarioArgs to empty dictionary if it is None
        scenarioArgs = scenarioArgs or {}

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

        if applySiteLists:
            procTask.setSiteWhitelist(self.siteWhitelist)
            procTask.setSiteBlacklist(self.siteBlacklist)
            procTask.setTrustSitelists(self.trustSitelists)

        newSplitArgs = {}
        for argName in splitArgs.keys():
            newSplitArgs[str(argName)] = splitArgs[argName]

        procTask.setSplittingAlgorithm(splitAlgo, **newSplitArgs)

        if not timePerEvent and self.timePerEvent:
            timePerEvent = self.timePerEvent
        if not sizePerEvent and self.sizePerEvent:
            sizePerEvent = self.sizePerEvent
        if not memoryReq and self.memory:
            memoryReq = self.memory

        procTask.setJobResourceInformation(timePerEvent = timePerEvent,
                                           sizePerEvent = sizePerEvent,
                                           memoryReq = memoryReq)

        procTask.setTaskType(taskType)
        procTask.setProcessingVersion(self.processingVersion)
        procTask.setAcquisitionEra(self.acquisitionEra)
        procTask.setProcessingString(self.processingString)
        procTask.setPerformanceMonitor(taskConf.get("MaxRSS", None),
                                       taskConf.get("MaxVSize", None),
                                       taskConf.get("SoftTimeout", None),
                                       taskConf.get("GracePeriod", None))

        if taskType in ["Production", 'PrivateMC'] and totalEvents != None:
            procTask.addGenerator(seeding)
            procTask.addProduction(totalEvents = totalEvents)
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

        if self.multicore and useMulticore:
            # if multicore, poke in the number of cores setting
            procTaskCmsswHelper.setNumberOfCores(self.multicoreNCores)

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

        if "events_per_lumi" in newSplitArgs:
            eventsPerLumi = newSplitArgs["events_per_lumi"]
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
        mergeTaskStageHelper = mergeTaskStageOut.getTypeHelper()

        mergeTaskCmsswHelper.cmsswSetup(self.frameworkVersion, softwareEnvironment = "",
                                        scramArch = self.scramArch)

        mergeTaskCmsswHelper.setErrorDestinationStep(stepName = mergeTaskLogArch.name())
        mergeTaskCmsswHelper.setGlobalTag(self.globalTag)
        mergeTaskCmsswHelper.setOverrideCatalog(self.overrideCatalog)

        if splitAlgo != "WMBSMergeBySize":
            mergeTaskCmsswHelper.setSkipBadFiles(True)

        mergeTask.setSplittingAlgorithm(splitAlgo,
                                        max_merge_size = self.maxMergeSize,
                                        min_merge_size = self.minMergeSize,
                                        max_merge_events = self.maxMergeEvents,
                                        max_wait_time = self.maxWaitTime,
                                        initial_lfn_counter = lfn_counter)

        if getattr(parentOutputModule, "dataTier") == "DQMIO":
            mergeTaskCmsswHelper.setDataProcessingConfig("do_not_use", "merge",
                                                         newDQMIO = True)
        else:
            mergeTaskCmsswHelper.setDataProcessingConfig("do_not_use", "merge")

        mergeTaskStageHelper.setMinMergeSize(0, 0)

        self.addOutputModule(mergeTask, "Merged",
                             primaryDataset = getattr(parentOutputModule, "primaryDataset"),
                             dataTier = getattr(parentOutputModule, "dataTier"),
                             filterName = getattr(parentOutputModule, "filterName"),
                             forceMerged = True)

        self.addCleanupTask(parentTask, parentOutputModuleName)
        if self.enableHarvesting and getattr(parentOutputModule, "dataTier") in ["DQMIO", "DQM"]:
            self.addDQMHarvestTask(mergeTask, "Merged",
                                   uploadProxy = self.dqmUploadProxy,
                                   periodic_harvest_interval= self.periodicHarvestInterval,
                                   doLogCollect = doLogCollect,
                                   dqmHarvestUnit = self.dqmHarvestUnit)
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

    def addDQMHarvestTask(self, parentTask, parentOutputModuleName, uploadProxy = None,
                          periodic_harvest_interval = 0, periodic_harvest_sibling = False,
                          parentStepName = "cmsRun1", doLogCollect = True, dqmHarvestUnit = "byRun"):
        """
        _addDQMHarvestTask_

        Create a DQM harvest task to harvest the files produces by the parent task.
        """
        if periodic_harvest_interval:
            harvestType = "Periodic"
        else:
            harvestType = "EndOfRun"

        harvestTask = parentTask.addTask("%s%sDQMHarvest%s" % (parentTask.name(),
                                                               harvestType,
                                                               parentOutputModuleName))
        self.addDashboardMonitoring(harvestTask)
        harvestTaskCmssw = harvestTask.makeStep("cmsRun1")
        harvestTaskCmssw.setStepType("CMSSW")

        harvestTaskUpload = harvestTaskCmssw.addStep("upload1")
        harvestTaskUpload.setStepType("DQMUpload")
        harvestTaskLogArch = harvestTaskCmssw.addStep("logArch1")
        harvestTaskLogArch.setStepType("LogArchive")

        harvestTask.setTaskLogBaseLFN(self.unmergedLFNBase)
        if doLogCollect:
            self.addLogCollectTask(harvestTask, taskName = "%s%s%sDQMHarvestLogCollect" % (parentTask.name(),
                                                                                           parentOutputModuleName,
                                                                                           harvestType))

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
                                          periodic_harvest_interval = periodic_harvest_interval,
                                          periodic_harvest_sibling = periodic_harvest_sibling,
                                          dqmHarvestUnit = dqmHarvestUnit)

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
            scenarioArgs = { 'globalTag' : self.globalTag,
                             'datasetName' : datasetName,
                             'runNumber' : self.runNumber,
                             'dqmSeq' : self.dqmSequences }
            if self.globalTagConnect:
                scenarioArgs['globalTagConnect'] = self.globalTagConnect
            if getattr(parentOutputModule, "dataTier") == "DQMIO":
                scenarioArgs['newDQMIO'] = True
            harvestTaskCmsswHelper.setDataProcessingConfig(self.procScenario,
                                                           "dqmHarvesting",
                                                           **scenarioArgs)

        harvestTaskUploadHelper = harvestTaskUpload.getTypeHelper()
        harvestTaskUploadHelper.setProxyFile(uploadProxy)
        harvestTaskUploadHelper.setServerURL(self.dqmUploadUrl)

        # if this was a Periodic harvesting add another for EndOfRun
        if periodic_harvest_interval:
            self.addDQMHarvestTask(parentTask = parentTask, parentOutputModuleName = parentOutputModuleName, uploadProxy = uploadProxy,
                                   periodic_harvest_interval = 0, periodic_harvest_sibling = True,
                                   parentStepName = parentStepName, doLogCollect = doLogCollect,
                                   dqmHarvestUnit = dqmHarvestUnit)

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
        pass

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

    def factoryWorkloadConstruction4docs(self, docs):
        """
        _factoryWorkloadConstruction_

        Build workloads from given list of of request documents.
        Provided list of docs should have similar parameters, such as
        request type, couch url/db, etc.
        """
        if len(set([d['RequestType'] for d in docs])) != 1:
            raise Exception('Provided list of docs has different request type')
        ids = set()
        for doc in docs:
            for key, val in doc.iteritems():
                if  key.endswith('ConfigCacheID'):
                    ids.add(val)
        ids = list(ids)
        couchURL = docs[0]['CouchURL']
        couchDBName = docs[0]['CouchDBName']
        if  (couchURL, couchDBName) in self.config_cache:
            configCache = self.config_cache[(couchURL, couchDBName)]
        else:
            configCache = ConfigCache(dbURL=couchURL, couchDBName=couchDBName)
            self.config_cache[(couchURL, couchDBName)] = configCache
        configCache.docs_cache.prefetch(ids)
        workloads = []
        for doc in docs:
            workloadName = doc['RequestName']
            self.masterValidation(schema=doc)
            self.validateSchema(schema=doc)
            workload = self.__call__(workloadName=workloadName, arguments=doc)
            self.validateWorkload(workload)
            workloads.append(workload)
        configCache.docs_cache.cleanup(ids)
        return workloads

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

        This uses programatically the definitions in getWorkloadArguments
        for type-checking, existence, null tests and the specific validation functions.

        Any spec-specific extras are implemented in the overriden validateSchema
        """
        # Validate the arguments according to the workload arguments definition
        argumentDefinition = self.getWorkloadArguments()
        msg = validateArgumentsCreate(schema, argumentDefinition)
        if msg is not None:
            self.raiseValidationException(msg)
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
                                  getOutputModules = True):
        """
        _validateConfigCacheExists_

        If we have a configCache, we should probably try and load it.
        """

        if configID == '' or configID == ' ':
            self.raiseValidationException(msg = "ConfigCacheID is invalid and cannot be loaded")

        if  (couchURL, couchDBName) in self.config_cache:
            configCache = self.config_cache[(couchURL, couchDBName)]
        else:
            configCache = ConfigCache(dbURL = couchURL, couchDBName = couchDBName, detail = getOutputModules)
            self.config_cache[(couchURL, couchDBName)] = configCache
        
        try:
            # if dtail option is set return outputModules
            return configCache.validate(configID)
        except ConfigCacheException as ex:
            self.raiseValidationException(ex.message())

    
    def getSchema(self):
        return self.schema

    @staticmethod
    def getWorkloadArguments():
        """
        _getWorkloadArguments_

        This represents the authorative list of request arguments that are
        interpreted by the current spec class.
        The list is formatted as a 2-level dictionary, the keys in the first level
        are the identifiers for the arguments processed by the current spec.
        The second level dictionary contains the information about that argument for
        validation:

        - default: Gives a default value if not provided,
                   this default value usually is good enough for a standard workflow. If the argument is not optional
                   and a default value is provided, this is only meant for test purposes.
        - type: A function that verifies the type of the argument, it may also cast it into the appropiate python type.
                If the input is not compatible with the expected type, this method must throw an exception.
        - optional: This boolean value indicates if the value must be provided or not by user 
                    or inherited class can overwrite with default value.
        - assign_optional: This boolean value indicates if the value must be provided when workflow is assinged if False.
                    
        - validate: A function which validates the input after type casting,
                    it returns True if the input is valid, it can throw exceptions on invalid input.
        - attr: This represents the name of the attribute corresponding to the argument in the WMSpec object.
        - null: This indicates if the argument can have None as its value.
        
        If above is not specifyed, automatically set by following default value
        - default: None
        - type: str
        - optional: True
        - assign_optional: True
        - validate: None
        - attr: change first letter to lower case
        - null: False
        Example:
        {
            RequestPriority : {'default' : 0,
                               'type' : int,
                               'optional' : False,
                               'validate' : lambda x : x > 0,
                               'attr' : 'priority',
                               'null' : False}
        }
        This replaces the old syntax in the __call__ of:

        self.priority = arguments.get("RequestPriority", 0)
        """
        # if key is not specified it is set by default value
        
        arguments = {"RequestType" : {"optional" : False}, # this need to be overwritten by inherited class
                     "Requestor": {"default": "unknown", "attr" : "owner"},
                     "RequestorDN" : {"default": "unknown", "attr" : "owner_dn"},
                     "Group" : {"default": "unknown"},
                     "RequestPriority": {"default" : 8000, "type" : int, 
                                         "validate" : lambda x : (x >= 0 and x < 1e6),
                                         "attr" : "priority"},
                     "VoGroup" : {"default" : "unknown", "attr" : "owner_vogroup"},
                     "VoRole" : {"default" : "unknown", "attr" : "owner_vorole"},
                     "Campaign" : {"default" : ""},
                     "AcquisitionEra" : {"default" : "FAKE", "validate" : acqname, "assign_optional": False},
                     "CMSSWVersion" : {"validate" : cmsswversion,
                                       "optional" : False, "attr" : "frameworkVersion"},
                     "ScramArch" : {"default" : "slc5_amd64_gcc462", "optional" : False},
                     "GlobalTag" : {"null" : True},
                     "GlobalTagConnect" : {"null" : True},
                     "ProcessingVersion" : {"default" : 1, "type" : int},
                     "ProcessingString" : {"null" : True},
                     "LumiList" : {"default" : {}, "type" : makeLumiList},
                     "SiteBlacklist" : {"default" : [], "type" : makeList,
                                        "validate" : lambda x: all([cmsname(y) for y in x])},
                     "SiteWhitelist" : {"default" : [], "type" : makeList,
                                        "validate" : lambda x: all([cmsname(y) for y in x])},
                     "TrustSitelists" : {"default" : False, "type" : strToBool},
                     "UnmergedLFNBase" : {"default" : "/store/unmerged"},
                     "MergedLFNBase" : {"default" : "/store/data"},
                     "MinMergeSize" : {"default" : 2 * 1024 * 1024 * 1024, "type" : int,
                                       "validate" : lambda x : x > 0},
                     "MaxMergeSize" : {"default" : 4 * 1024 * 1024 * 1024, "type" : int,
                                       "validate" : lambda x : x > 0},
                     "MaxWaitTime" : {"default" : 24 * 3600, "type" : int,
                                      "validate" : lambda x : x > 0},
                     "MaxMergeEvents" : {"default" : 100000, "type" : int,
                                         "validate" : lambda x : x > 0},
                     "ValidStatus" : {"default" : "PRODUCTION"},
                     "DbsUrl" : {"default" : "https://cmsweb.cern.ch/dbs/prod/global/DBSReader",
                                 "null" : True, "validate" : checkDBSURL},
                     "DashboardHost" : {"default" : "cms-wmagent-job.cern.ch"},
                     "DashboardPort" : {"default" : 8884, "type" : int,
                                        "validate" : lambda x : x > 0},
                     "OverrideCatalog" : {"null" : True},
                     "RunNumber" : {"default" : 0, "type" : int},
                     "TimePerEvent" : {"default" : 12.0, "type" : float,
                                       "validate" : lambda x : x > 0},
                     "Memory" : {"default" : 2300.0, "type" : float,
                                 "validate" : lambda x : x > 0},
                     "SizePerEvent" : {"default" : 512.0, "type" : float,
                                        "validate" : lambda x : x > 0},
                     "PeriodicHarvestInterval" : {"default" : 0, "type" : int,
                                                  "validate" : lambda x : x >= 0},
                     "DQMHarvestUnit" : {"default" : "byRun", "type" : str, "attr" : "dqmHarvestUnit"},
                     "DQMUploadProxy" : {"null" : True, "attr" : "dqmUploadProxy"},
                     "DQMUploadUrl" : {"default" : "https://cmsweb.cern.ch/dqm/dev", "attr" : "dqmUploadUrl"},
                     "DQMSequences" : {"default" : [], "type" : makeList, "attr" : "dqmSequences"},
                     "DQMConfigCacheID" : {"null" : True, "attr" : "dqmConfigCacheID"},
                     "EnableHarvesting" : {"default" : False, "type" : strToBool},
                     "EnableNewStageout" : {"default" : False, "type" : strToBool},
                     "IncludeParents" : {"default" : False,  "type" : strToBool},
                     "Multicore" : {"default" : 1, "null" : True,
                                    "validate" : lambda x : x == "auto" or (int(x) > 0)},
                     #from assignment: performance monitoring data
                     "MaxRSS" : {"default" : 2411724, "type" : int, "validate" : lambda x : x > 0},
                     "MaxVSize" : {"default" : 20411724, "type" : int, "validate" : lambda x : x > 0},
                     "SoftTimeout" : {"default" : 129600, "type" : int, "validate" : lambda x : x > 0},
                     "GracePeriod" : {"default" : 300, "type" : int, "validate" : lambda x : x > 0},
                     "UseSiteListAsLocation" : {"default" : False, "type" : bool},
                     
                     # Set phedex subscription information
                     "CustodialSites" : {"default" : [], "type" : makeList, "assign_optional": True,
                                         "validate" : lambda x: all([cmsname(y) for y in x])},
                     "NonCustodialSites" : {"default" : [], "type" : makeList, "assign_optional": True,
                                            "validate" : lambda x: all([cmsname(y) for y in x])},
                     "AutoApproveSubscriptionSites" : {"default" : [], "type" : makeList, "assign_optional": True, 
                                                       "validate" : lambda x: all([cmsname(y) for y in x])},
                     # should be Low, Normal, High
                     "SubscriptionPriority" : {"default" : "Low", "assign_optional": True,
                                               "validate" : lambda x: x in ["Low", "Normal", "High"]},
                     # should be Move Replica  
                     "CustodialSubType" : {"default" : "Replica", "type" : str, "assign_optional": True,
                                           "validate" : lambda x: x in ["Move", "Replica"]},
                     "NonCustodialSubType" : {"default" : "Replica", "type" : str, "assign_optional": True,
                                              "validate" : lambda x: x in ["Move", "Replica"]},
                     
                     # Block closing informaiont
                     "BlockCloseMaxWaitTime" : {"default" : 66400, "type" : int, "validate" : lambda x : x > 0},
                     "BlockCloseMaxFiles" : {"default" : 500, "type" : int, "validate" : lambda x : x > 0},
                     "BlockCloseMaxEvents" : {"default" : 25000000, "type" : int, "validate" : lambda x : x > 0},
                     "BlockCloseMaxSize" : {"default" : 5000000000000, "type" : int, "validate" : lambda x : x > 0},
                     
                     # dashboard activity
                     "Dashboard" : {"default": "", "type" : str},
                     # team name
                     "Team" : {"default": "", "type" : str},
                     "PrepID": {"default" : None, "null" : True}
                     }
       
        # Set defaults for the argument specification
        StdBase.setDefaultArgumentsProperty(arguments)

        return arguments
    
    @staticmethod
    def getWorkloadArgumentsWithReqMgr():
        # arguments need to be defined all the workflows which uses reqmgr
        reqMgrArguments = {"Requestor": {"optional" : False, "attr" : "owner"},
                           "RequestorDN" : {"default" : None, "optional" : False, "attr" : "owner_dn"},
                           "Group" : {"default" : "DATAOPS"},
                           "CouchURL" : {"default" : "https://cmsweb.cern.ch/couchdb",
                                         "validate" : couchurl},
                           "CouchDBName" : {"default" : "reqmgr_config_cache", "type" : str,
                                            "validate" : identifier},
                           "ConfigCacheUrl" : {"default" :"https://cmsweb.cern.ch/couchdb", "validate" : couchurl},
                           "ConfigCacheID" : {"optional" : False, "validate" : None},
                           "CouchWorkloadDBName" : {"default" : "reqmgr_workload_cache", "validate" : identifier},
                         }

        # Set defaults for the argument specification
        StdBase.setDefaultArgumentsProperty(reqMgrArguments)
        return reqMgrArguments

    @staticmethod
    def setDefaultArgumentsProperty(arguments):
        for arg in arguments:
            arguments[arg].setdefault("default", None)
            arguments[arg].setdefault("type", str)
            arguments[arg].setdefault("optional", True)
            arguments[arg].setdefault("assign_optional", True)
            arguments[arg].setdefault("null", False)
            arguments[arg].setdefault("validate", None)
            arguments[arg].setdefault("attr", arg[:1].lower() + arg[1:])
        return

    @classmethod
    def getTestArguments(cls):
        """
        _getTestArguments_

        Using the getWorkloadArguments definition, build a request schema
        that may pass basic validation and create successfully a workload
        of the current spec. Only for testing purposes! Any use of this function
        outside of unit tests and integration tests may put your life in danger.
        Note that in some cases like ConfigCacheID, there is no default that will work
        and tests should specifically provide one.
        """
        workloadDefinition = cls.getWorkloadArguments()
        schema = {}
        for arg in workloadDefinition:
            # Dashboard parameter must be re-defined for test purposes
            if arg == "DashboardHost":
                schema[arg] = "127.0.0.1"
            elif arg == "CouchURL" or arg == "ConfigCacheUrl":
                
                import os
                schema[arg] = os.environ["COUCHURL"]
            elif arg == "CouchDBName":
                schema[arg] = "reqmgr_config_cache_t"
            elif not workloadDefinition[arg]["optional"]:
                if workloadDefinition[arg]["type"] == str:
                    if arg == "InputDataset":
                        schema[arg] = "/MinimumBias/ComissioningHI-v1/RAW"
                    else:
                        schema[arg] = "fake"
                if workloadDefinition[arg]["type"] == int or workloadDefinition[arg]["type"] == float:
                    schema[arg] = 1
            else:
                schema[arg] = workloadDefinition[arg]['default']
        return schema
