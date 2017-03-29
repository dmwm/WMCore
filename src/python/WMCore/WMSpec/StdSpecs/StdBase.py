#!/usr/bin/env python
"""
_StdBase_

Base class with helper functions for standard WMSpec files.
"""
import logging

from Utils.Utilities import makeList, makeNonEmptyList, strToBool, safeStr
from WMCore.Cache.WMConfigCache import ConfigCache, ConfigCacheException
from WMCore.Configuration import ConfigSection
from WMCore.Lexicon import lfnBase, identifier, acqname, cmsname
from WMCore.Lexicon import couchurl, block, procstring, activity, procversion
from WMCore.Services.Dashboard.DashboardReporter import DashboardReporter
from WMCore.WMSpec.WMSpecErrors import WMSpecFactoryException
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.WMWorkloadTools import (makeLumiList, checkDBSURL, validateArgumentsCreate)
from WMCore.ReqMgr.Tools.cms import releases, architectures


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
                        value = arguments[arg]
                        setattr(self, argumentDefinition[arg]["attr"], value)
                        self.schema[arg] = value
                elif argumentDefinition[arg]["optional"]:
                    defaultValue = argumentDefinition[arg]["default"]
                    setattr(self, argumentDefinition[arg]["attr"], defaultValue)
                    self.schema[arg] = defaultValue
            except Exception as ex:
                raise WMSpecFactoryException("parameter %s: %s" % (arg, str(ex)))

        return

    @staticmethod
    def skimToDataTier():
        """
        Map physics skim to a data tier
        """
        skimMap = {'LogError': 'RAW-RECO',
                   'LogErrorMonitor': 'USER',
                   'ZElectron': 'RAW-RECO',
                   'ZMu': 'RAW-RECO',
                   'MuTau': 'RAW-RECO',
                   'TopMuEG': 'RAW-RECO',
                   'EcalActivity': 'RAW-RECO',
                   'CosmicSP': 'RAW-RECO',
                   'CosmicTP': 'RAW-RECO',
                   'ZMM': 'RAW-RECO',
                   'Onia': 'RECO',
                   'HighPtJet': 'RAW-RECO',
                   'D0Meson': 'RECO',
                   'Photon': 'AOD',
                   'ZEE': 'AOD',
                   'BJet': 'AOD',
                   'OniaCentral': 'RECO',
                   'OniaPeripheral': 'RECO',
                   'SingleTrack': 'AOD',
                   'MinBias': 'AOD',
                   'OniaUPC': 'RAW-RECO',
                   'HighMET': 'RECO',
                   'BPHSkim': 'USER',
                   'PAMinBias': 'RAW-RECO',
                   'PAZEE': 'RAW-RECO',
                   'PAZMM': 'RAW-RECO'
                  }
        return skimMap

    def determineOutputModules(self, scenarioFunc=None, scenarioArgs=None,
                               configDoc=None, couchDBName=None, configCacheUrl=None):
        """
        _determineOutputModules_

        Determine the output module names and associated metadata for the
        given config.
        """
        # set default scenarioArgs to empty dictionary if it is None.
        scenarioArgs = scenarioArgs or {}

        outputModules = {}
        if configDoc != None and configDoc != "":
            if (configCacheUrl, couchDBName) in self.config_cache:
                configCache = self.config_cache[(configCacheUrl, couchDBName)]
            else:
                configCache = ConfigCache(configCacheUrl, couchDBName, True)
                self.config_cache[(configCacheUrl, couchDBName)] = configCache
            # TODO: need to change to DataCache
            # configCache.loadDocument(configDoc)
            configCache.loadByID(configDoc)
            outputModules = configCache.getOutputModuleInfo()
        else:
            if 'outputs' in scenarioArgs and scenarioFunc in ["promptReco", "expressProcessing", "repack"]:

                for output in scenarioArgs.get('outputs', []):

                    moduleLabel = output['moduleLabel']
                    outputModules[moduleLabel] = {'dataTier': output['dataTier']}
                    if 'primaryDataset' in output:
                        outputModules[moduleLabel]['primaryDataset'] = output['primaryDataset']
                    if 'filterName' in output:
                        outputModules[moduleLabel]['filterName'] = output['filterName']

                for physicsSkim in scenarioArgs.get('PhysicsSkims', []):
                    dataTier = StdBase.skimToDataTier().get(physicsSkim, 'USER')
                    moduleLabel = "SKIMStream%s" % physicsSkim
                    outputModules[moduleLabel] = {'dataTier': dataTier,
                                                  'filterName': physicsSkim}

            elif scenarioFunc == "alcaSkim":

                for alcaSkim in scenarioArgs.get('skims', []):
                    moduleLabel = "ALCARECOStream%s" % alcaSkim
                    if alcaSkim.startswith("PromptCalibProd"):
                        dataTier = "ALCAPROMPT"
                    else:
                        dataTier = "ALCARECO"
                    outputModules[moduleLabel] = {'dataTier': dataTier,
                                                  'primaryDataset': scenarioArgs.get('primaryDataset'),
                                                  'filterName': alcaSkim}

        return outputModules

    def addDashboardMonitoring(self, task):
        """
        _addDashboardMonitoring_

        Add dashboard monitoring for the given task.
        """
        # A gigabyte defined as 1024^3 (assuming RSS and VSize is in KiByte)
        gb = 1024.0 * 1024.0
        # Default timeout defined in CMS policy
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
            # Create a fake config
            conf = ConfigSection()
            conf.section_('DashboardReporter')
            conf.DashboardReporter.dashboardHost = self.dashboardHost
            conf.DashboardReporter.dashboardPort = self.dashboardPort

            # Create the reporter
            reporter = DashboardReporter(conf)

            # Assemble the info
            workflow = {}
            workflow['name'] = self.workloadName
            workflow['application'] = self.frameworkVersion
            workflow['TaskType'] = dashboardActivity
            # Let's try to build information about the inputDataset
            dataset = 'DoesNotApply'
            if hasattr(self, 'inputDataset'):
                dataset = self.inputDataset
            workflow['datasetFull'] = dataset
            workflow['user'] = 'cmsdataops'

            # Send the workflow info
            reporter.addTask(workflow)
        except Exception:
            # This is not critical, if it fails just leave it be
            logging.error("There was an error with dashboard reporting")

    def createWorkload(self):
        """
        _createWorkload_

        Create a new workload.
        """
        ownerProps = {'dn': self.owner_dn, 'vogroup': self.owner_vogroup, 'vorole': self.owner_vorole}

        workload = newWorkload(self.workloadName)
        workload.setOwnerDetails(self.owner, self.group, ownerProps)
        workload.setStartPolicy("DatasetBlock", SliceType="NumberOfFiles", SliceSize=1)
        workload.setEndPolicy("SingleShot")
        workload.setAcquisitionEra(acquisitionEras=self.acquisitionEra)
        workload.setProcessingVersion(processingVersions=self.processingVersion)
        workload.setProcessingString(processingStrings=self.processingString)
        workload.setValidStatus(validStatus=self.validStatus)
        workload.setLumiList(lumiLists=self.lumiList)
        workload.setPriority(self.priority)
        workload.setCampaign(self.campaign)
        workload.setRequestType(self.requestType)
        workload.setPrepID(self.prepID)
        workload.setAllowOpportunistic(self.allowOpportunistic)
        return workload

    def setupProcessingTask(self, procTask, taskType, inputDataset=None, inputStep=None,
                            inputModule=None, scenarioName=None,
                            scenarioFunc=None, scenarioArgs=None,
                            couchDBName=None, configDoc=None, splitAlgo="LumiBased",
                            splitArgs=None, seeding=None,
                            totalEvents=None, eventsPerLumi=None,
                            userDN=None, asyncDest=None, owner_vogroup="DEFAULT",
                            owner_vorole="DEFAULT", stepType="CMSSW",
                            userSandbox=None, userFiles=None, primarySubType=None,
                            forceMerged=False, forceUnmerged=False,
                            configCacheUrl=None, timePerEvent=None, memoryReq=None,
                            sizePerEvent=None, applySiteLists=True, cmsswVersion=None,
                            scramArch=None, globalTag=None, taskConf=None):
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
          configDoc not empty - Use a ConfigCache config, configCacheUrl and
            couchDBName must not be empty.
          configDoc empty - Use a Configuration.DataProcessing config.  The
            scenarioName, scenarioFunc and scenarioArgs parameters must not be
            empty.

        The seeding and totalEvents parameters are only used for production jobs.

        taskConf is a dictionary with either Task or Step level key/value pairs used to
        bypass the workload level argument, providing flexibility to have different
        settings even inside the same task object.
        """
        # set default values in case it's not passed to this method
        scenarioArgs = scenarioArgs or {}
        taskConf = taskConf or {}
        splitArgs = splitArgs or {'lumis_per_job': 8}
        userFiles = userFiles or []

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
            procTask.setTrustSitelists(self.trustSitelists, self.trustPUSitelists)

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
        if not cmsswVersion and self.frameworkVersion:
            cmsswVersion = self.frameworkVersion
        if not scramArch and self.scramArch:
            scramArch = self.scramArch
        if not globalTag and self.globalTag:
            globalTag = self.globalTag

        procTask.setJobResourceInformation(timePerEvent=timePerEvent,
                                           sizePerEvent=sizePerEvent,
                                           memoryReq=memoryReq)

        procTask.setTaskType(taskType)

        # we better be safe in case the specs set these Task/Step level args to None
        acqEra = taskConf.get("AcquisitionEra") or self.acquisitionEra
        procStr = taskConf.get("ProcessingString") or self.processingString
        procVer = taskConf.get("ProcessingVersion") or self.processingVersion
        procTask.setAcquisitionEra(acqEra)
        procTask.setProcessingString(procStr)
        procTask.setProcessingVersion(procVer)

        procTask.setPerformanceMonitor(taskConf.get("MaxRSS", None),
                                       taskConf.get("MaxVSize", None),
                                       taskConf.get("SoftTimeout", None),
                                       taskConf.get("GracePeriod", None))

        if taskType in ["Production", 'PrivateMC'] and totalEvents != None:
            procTask.addGenerator(seeding)
            procTask.addProduction(totalEvents=totalEvents)
            procTask.setFirstEventAndLumi(firstEvent=self.firstEvent,
                                          firstLumi=self.firstLumi)
        else:
            if inputDataset != None:
                (primary, processed, tier) = self.inputDataset[1:].split("/")
                procTask.addInputDataset(name=self.inputDataset, primary=primary,
                                         processed=processed, tier=tier, dbsurl=self.dbsUrl,
                                         block_blacklist=self.blockBlacklist,
                                         block_whitelist=self.blockWhitelist,
                                         run_blacklist=self.runBlacklist,
                                         run_whitelist=self.runWhitelist)
            elif inputStep != None and inputModule != None:
                procTask.setInputReference(inputStep, outputModule=inputModule)

        if primarySubType:
            procTask.setPrimarySubType(subType=primarySubType)

        procTaskCmsswHelper = procTaskCmssw.getTypeHelper()
        procTaskStageHelper = procTaskStageOut.getTypeHelper()

        # StepChain overrides
        multicore = self.multicore
        eventStreams = self.eventStreams
        if 'Multicore' in taskConf and taskConf['Multicore'] > 0:
            multicore = taskConf['Multicore']
        if 'EventStreams' in taskConf and taskConf['EventStreams'] >= 0:
            eventStreams = taskConf['EventStreams']

        procTaskCmsswHelper.setNumberOfCores(multicore, eventStreams)

        procTaskCmsswHelper.setUserSandbox(userSandbox)
        procTaskCmsswHelper.setUserFiles(userFiles)
        procTaskCmsswHelper.setGlobalTag(globalTag)
        procTaskCmsswHelper.setOverrideCatalog(self.overrideCatalog)
        procTaskCmsswHelper.setErrorDestinationStep(stepName=procTaskLogArch.name())

        if forceMerged:
            procTaskStageHelper.setMinMergeSize(0, 0)
        elif forceUnmerged:
            procTaskStageHelper.disableStraightToMerge()
        else:
            procTaskStageHelper.setMinMergeSize(self.minMergeSize, self.maxMergeEvents)

        procTaskCmsswHelper.cmsswSetup(cmsswVersion,
                                       softwareEnvironment="",
                                       scramArch=scramArch)

        if "events_per_lumi" in newSplitArgs:
            eventsPerLumi = newSplitArgs["events_per_lumi"]
        procTaskCmsswHelper.setEventsPerLumi(eventsPerLumi)

        configOutput = self.determineOutputModules(scenarioFunc, scenarioArgs,
                                                   configDoc, couchDBName,
                                                   configCacheUrl=configCacheUrl)
        outputModules = {}
        for outputModuleName in configOutput.keys():
            outputModule = self.addOutputModule(procTask,
                                                outputModuleName,
                                                configOutput[outputModuleName].get('primaryDataset',
                                                                                   self.inputPrimaryDataset),
                                                configOutput[outputModuleName]['dataTier'],
                                                configOutput[outputModuleName].get('filterName', None),
                                                forceMerged=forceMerged, forceUnmerged=forceUnmerged, taskConf=taskConf)
            outputModules[outputModuleName] = outputModule

        if configDoc != None and configDoc != "":
            procTaskCmsswHelper.setConfigCache(configCacheUrl, configDoc, couchDBName)
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
        # only in the very end, in order to get it in for the children tasks as well
        prepID = taskConf.get("PrepID") or self.prepID
        procTask.setPrepID(prepID)

        return outputModules

    def _getDictionaryParams(self, prop, key, default=None):
        """
        Support dictonary format for property definition.
        acquisitionEra, processingString, processingVersion
        """
        if isinstance(prop, dict):
            return prop.get(key, default)
        else:
            return prop

    def addOutputModule(self, parentTask, outputModuleName,
                        primaryDataset, dataTier, filterName,
                        stepName="cmsRun1", forceMerged=False,
                        forceUnmerged=False, taskConf=None):
        """
        _addOutputModule_

        Add an output module to the given processing task.

        taskConf is used for multi-step tasks where diff output processed
        dataset name is desired
        """
        taskConf = taskConf or {}
        haveFilterName = (filterName != None and filterName != "")
        haveProcString = (self.processingString != None and self.processingString != "")
        haveRunNumber = (self.runNumber != None and self.runNumber > 0)

        taskName = parentTask.name()
        if self.requestType == "StepChain" and "StepName" in taskConf:
            taskName = taskConf["StepName"]
        acqEra = taskConf.get('AcquisitionEra') or self._getDictionaryParams(self.acquisitionEra, taskName)
        procString = taskConf.get('ProcessingString') or self._getDictionaryParams(self.processingString, taskName)
        procVersion = taskConf.get('ProcessingVersion') or self._getDictionaryParams(self.processingVersion, taskName, 1)

        processedDataset = "%s-" % acqEra
        if haveFilterName:
            processedDataset += "%s-" % filterName
        if haveProcString:
            processedDataset += "%s-" % procString
        processedDataset += "v%i" % procVersion

        if haveProcString:
            processingLFN = "%s-v%i" % (procString, procVersion)
        else:
            processingLFN = "v%i" % procVersion

        if haveRunNumber:
            stringRunNumber = str(self.runNumber).zfill(9)
            runSections = [stringRunNumber[i:i + 3] for i in range(0, 9, 3)]
            runLFN = "/".join(runSections)

        unmergedLFN = "%s/%s/%s/%s" % (self.unmergedLFNBase,
                                       acqEra,
                                       primaryDataset, dataTier)
        mergedLFN = "%s/%s/%s/%s" % (self.mergedLFNBase,
                                     acqEra,
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
                                        primaryDataset=primaryDataset,
                                        processedDataset=processedDataset,
                                        dataTier=dataTier,
                                        filterName=filterName,
                                        lfnBase=unmergedLFN,
                                        mergedLFNBase=mergedLFN,
                                        transient=isTransient)

        return {"primaryDataset": primaryDataset,
                "dataTier": dataTier,
                "processedDataset": processedDataset,
                "filterName": filterName}

    def addLogCollectTask(self, parentTask, taskName="LogCollect", filesPerJob=500,
                          cmsswVersion=None, scramArch=None):
        """
        _addLogCollectTask_

        Create a LogCollect task for log archives that are produced by the
        parent task.
        """
        cmsswVersion = cmsswVersion or self.frameworkVersion
        scramArch = scramArch or self.scramArch
        logCollectTask = parentTask.addTask(taskName)
        self.addDashboardMonitoring(logCollectTask)
        logCollectStep = logCollectTask.makeStep("logCollect1")
        logCollectStep.setStepType("LogCollect")
        logCollectStep.setNewStageoutOverride(self.enableNewStageout)
        logCollectTask.applyTemplates()
        logCollectTask.setSplittingAlgorithm("MinFileBased", files_per_job=filesPerJob)
        logCollectTask.setTaskType("LogCollect")

        parentTaskLogArch = parentTask.getStep("logArch1")
        logCollectTask.setInputReference(parentTaskLogArch, outputModule="logArchive")

        logCollectStepHelper = logCollectStep.getTypeHelper()
        logCollectStepHelper.cmsswSetup(cmsswVersion,
                                        softwareEnvironment="",
                                        scramArch=scramArch)

        return logCollectTask

    def addMergeTask(self, parentTask, parentTaskSplitting, parentOutputModuleName, parentStepName="cmsRun1",
                     doLogCollect=True, lfn_counter=0, forceTaskName=None, cmsswVersion=None, scramArch=None,
                     taskConf=None):
        """
        _addMergeTask_

        Create a merge task for files produced by the parent task.

        taskConf is used for multi-step Tasks where different merge settings are desired for
        the same parent production/processing task
        """
        cmsswVersion = cmsswVersion or self.frameworkVersion
        scramArch = scramArch or self.scramArch
        # StepChain use case, to avoid merge task names clashes
        forceTaskName = forceTaskName or parentTask.name()
        taskConf = taskConf or {}

        mergeTask = parentTask.addTask("%sMerge%s" % (forceTaskName, parentOutputModuleName))
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
        # we better be safe in case the specs set these Task/Step level args to None
        acqEra = taskConf.get("AcquisitionEra") or self.acquisitionEra
        procStr = taskConf.get("ProcessingString") or self.processingString
        procVer = taskConf.get("ProcessingVersion") or self.processingVersion
        mergeTask.setAcquisitionEra(acqEra)
        mergeTask.setProcessingString(procStr)
        mergeTask.setProcessingVersion(procVer)

        if doLogCollect:
            taskNameLC = "%s%sMergeLogCollect" % (forceTaskName, parentOutputModuleName)
            self.addLogCollectTask(mergeTask, taskName=taskNameLC,
                                   cmsswVersion=cmsswVersion, scramArch=scramArch)

        mergeTask.setTaskType("Merge")
        mergeTask.applyTemplates()

        if parentTaskSplitting == "EventBased" and parentTask.taskType() != "Production":
            splitAlgo = "WMBSMergeBySize"
        else:
            splitAlgo = "ParentlessMergeBySize"

        parentTaskCmssw = parentTask.getStep(parentStepName)
        parentOutputModule = parentTaskCmssw.getOutputModule(parentOutputModuleName)

        mergeTask.setInputReference(parentTaskCmssw, outputModule=parentOutputModuleName)

        mergeTaskCmsswHelper = mergeTaskCmssw.getTypeHelper()
        mergeTaskStageHelper = mergeTaskStageOut.getTypeHelper()

        mergeTaskCmsswHelper.cmsswSetup(cmsswVersion,
                                        softwareEnvironment="",
                                        scramArch=scramArch)

        mergeTaskCmsswHelper.setErrorDestinationStep(stepName=mergeTaskLogArch.name())
        mergeTaskCmsswHelper.setGlobalTag(self.globalTag)
        mergeTaskCmsswHelper.setOverrideCatalog(self.overrideCatalog)

        if splitAlgo != "WMBSMergeBySize" and self.robustMerge:
            mergeTaskCmsswHelper.setSkipBadFiles(True)

        mergeTask.setSplittingAlgorithm(splitAlgo,
                                        max_merge_size=self.maxMergeSize,
                                        min_merge_size=self.minMergeSize,
                                        max_merge_events=self.maxMergeEvents,
                                        max_wait_time=self.maxWaitTime,
                                        initial_lfn_counter=lfn_counter)

        if getattr(parentOutputModule, "dataTier") == "DQMIO":
            mergeTaskCmsswHelper.setDataProcessingConfig("do_not_use", "merge",
                                                         newDQMIO=True)
        else:
            mergeTaskCmsswHelper.setDataProcessingConfig("do_not_use", "merge")

        mergeTaskStageHelper.setMinMergeSize(0, 0)

        self.addOutputModule(mergeTask, "Merged",
                             primaryDataset=getattr(parentOutputModule, "primaryDataset"),
                             dataTier=getattr(parentOutputModule, "dataTier"),
                             filterName=getattr(parentOutputModule, "filterName"),
                             forceMerged=True, taskConf=taskConf)

        self.addCleanupTask(parentTask, parentOutputModuleName, forceTaskName)
        if self.enableHarvesting and getattr(parentOutputModule, "dataTier") in ["DQMIO", "DQM"]:
            self.addDQMHarvestTask(mergeTask, "Merged",
                                   uploadProxy=self.dqmUploadProxy,
                                   periodic_harvest_interval=self.periodicHarvestInterval,
                                   doLogCollect=doLogCollect,
                                   dqmHarvestUnit=self.dqmHarvestUnit)

        # only in the very end, in order to get it in for the children tasks as well
        prepID = taskConf.get("PrepID") or parentTask.getPrepID()
        mergeTask.setPrepID(prepID)

        return mergeTask

    def addCleanupTask(self, parentTask, parentOutputModuleName, forceTaskName=None):
        """
        _addCleanupTask_

        Create a cleanup task to delete files produces by the parent task.
        """
        if forceTaskName is None:
            forceTaskName = parentTask.name()

        cleanupTask = parentTask.addTask("%sCleanupUnmerged%s" % (forceTaskName, parentOutputModuleName))
        self.addDashboardMonitoring(cleanupTask)
        cleanupTask.setTaskType("Cleanup")

        parentTaskCmssw = parentTask.getStep("cmsRun1")
        cleanupTask.setInputReference(parentTaskCmssw, outputModule=parentOutputModuleName)
        cleanupTask.setSplittingAlgorithm("SiblingProcessingBased", files_per_job=50)

        cleanupStep = cleanupTask.makeStep("cleanupUnmerged%s" % parentOutputModuleName)
        cleanupStep.setStepType("DeleteFiles")
        cleanupTask.applyTemplates()

        # TODO: for StepChain, it will still use the Step1 PrepID. It has to be fixed
        cleanupTask.setPrepID(parentTask.getPrepID())

        return

    def addDQMHarvestTask(self, parentTask, parentOutputModuleName, uploadProxy=None,
                          periodic_harvest_interval=0, periodic_harvest_sibling=False,
                          parentStepName="cmsRun1", doLogCollect=True, dqmHarvestUnit="byRun",
                          cmsswVersion=None, scramArch=None):
        """
        _addDQMHarvestTask_

        Create a DQM harvest task to harvest the files produces by the parent task.
        """
        cmsswVersion = cmsswVersion or self.frameworkVersion
        scramArch = scramArch or self.scramArch

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
            taskNameLC = "%s%s%sDQMHarvestLogCollect" % (parentTask.name(),
                                                         parentOutputModuleName,
                                                         harvestType)
            self.addLogCollectTask(harvestTask, taskName=taskNameLC,
                                   cmsswVersion=cmsswVersion, scramArch=scramArch)

        harvestTask.setTaskType("Harvesting")
        harvestTask.applyTemplates()

        harvestTaskCmsswHelper = harvestTaskCmssw.getTypeHelper()
        harvestTaskCmsswHelper.cmsswSetup(cmsswVersion,
                                          softwareEnvironment="",
                                          scramArch=scramArch)

        harvestTaskCmsswHelper.setErrorDestinationStep(stepName=harvestTaskLogArch.name())
        harvestTaskCmsswHelper.setGlobalTag(self.globalTag)
        harvestTaskCmsswHelper.setOverrideCatalog(self.overrideCatalog)

        harvestTaskCmsswHelper.setUserLFNBase("/")

        parentTaskCmssw = parentTask.getStep(parentStepName)
        parentOutputModule = parentTaskCmssw.getOutputModule(parentOutputModuleName)

        harvestTask.setInputReference(parentTaskCmssw, outputModule=parentOutputModuleName)

        harvestTask.setSplittingAlgorithm("Harvest",
                                          periodic_harvest_interval=periodic_harvest_interval,
                                          periodic_harvest_sibling=periodic_harvest_sibling,
                                          dqmHarvestUnit=dqmHarvestUnit)

        datasetName = "/%s/%s/%s" % (getattr(parentOutputModule, "primaryDataset"),
                                     getattr(parentOutputModule, "processedDataset"),
                                     getattr(parentOutputModule, "dataTier"))

        if self.dqmConfigCacheID is not None:
            harvestTaskCmsswHelper.setConfigCache(self.configCacheUrl, self.dqmConfigCacheID, self.couchDBName)
            harvestTaskCmsswHelper.setDatasetName(datasetName)
        else:
            scenarioArgs = {'globalTag': self.globalTag,
                            'datasetName': datasetName,
                            'runNumber': self.runNumber,
                            'dqmSeq': self.dqmSequences}
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
            self.addDQMHarvestTask(parentTask=parentTask, parentOutputModuleName=parentOutputModuleName,
                                   uploadProxy=uploadProxy,
                                   periodic_harvest_interval=0, periodic_harvest_sibling=True,
                                   parentStepName=parentStepName, doLogCollect=doLogCollect,
                                   dqmHarvestUnit=dqmHarvestUnit)

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
                if key.endswith('ConfigCacheID'):
                    ids.add(val)
        ids = list(ids)
        configCacheUrl = docs[0]['ConfigCacheUrl']
        couchDBName = docs[0]['CouchDBName']
        if (configCacheUrl, couchDBName) in self.config_cache:
            configCache = self.config_cache[(configCacheUrl, couchDBName)]
        else:
            configCache = ConfigCache(dbURL=configCacheUrl, couchDBName=couchDBName)
            self.config_cache[(configCacheUrl, couchDBName)] = configCache
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
        self.masterValidation(schema=arguments)
        self.validateSchema(schema=arguments)
        workload = self.__call__(workloadName=workloadName, arguments=arguments)
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
        try:
            validateArgumentsCreate(schema, argumentDefinition)
        except Exception as ex:
            self.raiseValidationException(str(ex))
        return

    def raiseValidationException(self, msg):
        """
        _raiseValidationException_

        Inbuilt method for raising exception so people don't have
        to import WMSpecFactoryException all over the place.
        """

        logging.error("About to raise exception %s", msg)
        raise WMSpecFactoryException(message=msg)

    def validateConfigCacheExists(self, configID, configCacheUrl, couchDBName,
                                  getOutputModules=True):
        """
        _validateConfigCacheExists_

        If we have a configCache, we should probably try and load it.
        """

        if configID == '' or configID == ' ':
            self.raiseValidationException(msg="ConfigCacheID is invalid and cannot be loaded")

        if (configCacheUrl, couchDBName) in self.config_cache:
            configCache = self.config_cache[(configCacheUrl, couchDBName)]
        else:
            configCache = ConfigCache(dbURL=configCacheUrl, couchDBName=couchDBName, detail=getOutputModules)
            self.config_cache[(configCacheUrl, couchDBName)] = configCache

        try:
            # if detail option is set return outputModules
            return configCache.validate(configID)
        except ConfigCacheException as ex:
            self.raiseValidationException(str(ex))

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

        arguments = {"RequestType": {"optional": False},  # this need to be overwritten by inherited class
                     "Requestor": {"default": "unknown", "attr": "owner", "optional": False},
                     "RequestorDN": {"default": "unknown", "attr": "owner_dn", "optional": False},
                     "Group": {"default": "DATAOPS"},
                     "RequestPriority": {"default": 8000, "type": int,
                                         "validate": lambda x: (x >= 0 and x < 1e6),
                                         "attr": "priority"},
                     "VoGroup": {"default": "unknown", "attr": "owner_vogroup"},
                     "VoRole": {"default": "unknown", "attr": "owner_vorole"},
                     # default value will be AcquisitionEra except when AcquistionEra is dict
                     "Campaign": {"default": "", "optional": True},
                     "AcquisitionEra": {"validate": acqname, "optional": False},
                     "CMSSWVersion": {"validate": lambda x: x in releases(),
                                      "optional": False, "attr": "frameworkVersion"},
                     "ScramArch": {"validate": lambda x: all([y in architectures() for y in x]),
                                   "optional": False, "type": makeNonEmptyList},
                     "GlobalTag": {"optional": False, "null": False},
                     "GlobalTagConnect": {"null": True},
                     "ProcessingVersion": {"default": 1, "type": int, "validate": procversion},
                     "ProcessingString": {"validate": procstring, "optional": False},
                     "LumiList": {"default": {}, "type": makeLumiList},
                     "SiteBlacklist": {"default": [], "type": makeList,
                                       "validate": lambda x: all([cmsname(y) for y in x])},
                     "SiteWhitelist": {"default": [], "type": makeList,
                                       "validate": lambda x: all([cmsname(y) for y in x])},
                     "BlockBlacklist": {"default": [], "type": makeList,
                                        "validate": lambda x: all([block(y) for y in x])},
                     "BlockWhitelist": {"default": [], "type": makeList,
                                        "validate": lambda x: all([block(y) for y in x])},
                     "UnmergedLFNBase": {"default": "/store/unmerged"},
                     "MergedLFNBase": {"default": "/store/data"},
                     "MinMergeSize": {"default": 2 * 1024 * 1024 * 1024, "type": int,
                                      "validate": lambda x: x > 0},
                     "MaxMergeSize": {"default": 4 * 1024 * 1024 * 1024, "type": int,
                                      "validate": lambda x: x > 0},
                     "MaxWaitTime": {"default": 24 * 3600, "type": int,
                                     "validate": lambda x: x > 0},
                     "MaxMergeEvents": {"default": 100000000, "type": int,
                                        "validate": lambda x: x > 0},
                     "ValidStatus": {"default": "PRODUCTION"},
                     "DbsUrl": {"default": "https://cmsweb.cern.ch/dbs/prod/global/DBSReader",
                                "null": True, "validate": checkDBSURL},
                     "DashboardHost": {"default": "cms-jobmon.cern.ch"},
                     "DashboardPort": {"default": 8884, "type": int,
                                       "validate": lambda x: x > 0},
                     "OverrideCatalog": {"null": True},
                     "RunNumber": {"default": 0, "type": int},
                     "TimePerEvent": {"default": 12.0, "type": float,
                                      "validate": lambda x: x > 0},
                     "Memory": {"default": 2300.0, "type": float,
                                "validate": lambda x: x > 0},
                     "SizePerEvent": {"default": 512.0, "type": float,
                                      "validate": lambda x: x > 0},
                     "PeriodicHarvestInterval": {"default": 0, "type": int,
                                                 "validate": lambda x: x >= 0},
                     "DQMHarvestUnit": {"default": "byRun", "type": str, "attr": "dqmHarvestUnit"},
                     "DQMUploadProxy": {"null": True, "attr": "dqmUploadProxy"},
                     "DQMUploadUrl": {"default": "https://cmsweb.cern.ch/dqm/dev", "attr": "dqmUploadUrl"},
                     "DQMSequences": {"default": [], "type": makeList, "attr": "dqmSequences"},
                     "DQMConfigCacheID": {"null": True, "attr": "dqmConfigCacheID"},
                     "EnableHarvesting": {"default": False, "type": strToBool},
                     "EnableNewStageout": {"default": False, "type": strToBool},
                     "IncludeParents": {"default": False, "type": strToBool},
                     "Multicore": {"default": 1, "type": int,
                                   "validate": lambda x: x > 0},
                     "EventStreams": {"type": int, "validate": lambda x: x >= 0, "null": True},
                     # data location management
                     "TrustSitelists": {"default": False, "type": strToBool},
                     "TrustPUSitelists": {"default": False, "type": strToBool},
                     "AllowOpportunistic": {"default": False, "type": strToBool},
                     # from assignment: performance monitoring data
                     "MaxRSS": {"default": 2411724, "type": int, "validate": lambda x: x > 0},
                     "MaxVSize": {"default": 20411724, "type": int, "validate": lambda x: x > 0},
                     "SoftTimeout": {"default": 129600, "type": int, "validate": lambda x: x > 0},
                     "GracePeriod": {"default": 300, "type": int, "validate": lambda x: x > 0},

                     # Set phedex subscription information
                     "CustodialSites": {"default": [], "type": makeList, "assign_optional": True,
                                        "validate": lambda x: all([cmsname(y) for y in x])},
                     "NonCustodialSites": {"default": [], "type": makeList, "assign_optional": True,
                                           "validate": lambda x: all([cmsname(y) for y in x])},
                     "AutoApproveSubscriptionSites": {"default": [], "type": makeList, "assign_optional": True,
                                                      "validate": lambda x: all([cmsname(y) for y in x])},
                     # should be Low, Normal or High
                     "SubscriptionPriority": {"default": "Low", "assign_optional": True,
                                              "validate": lambda x: x in ["Low", "Normal", "High"]},
                     # should be Move Replica
                     "CustodialSubType": {"default": "Replica", "type": str, "assign_optional": True,
                                          "validate": lambda x: x in ["Move", "Replica"]},
                     "NonCustodialSubType": {"default": "Replica", "type": str, "assign_optional": True,
                                             "validate": lambda x: x in ["Move", "Replica"]},

                     # should be a valid PhEDEx group
                     "CustodialGroup": {"default": "DataOps", "type": str, "assign_optional": True},
                     "NonCustodialGroup": {"default": "DataOps", "type": str, "assign_optional": True},

                     # should be True or False
                     "DeleteFromSource": {"default": False, "type": strToBool},

                     # Block closing information
                     "BlockCloseMaxWaitTime": {"default": 66400, "type": int, "validate": lambda x: x > 0},
                     "BlockCloseMaxFiles": {"default": 500, "type": int, "validate": lambda x: x > 0},
                     "BlockCloseMaxEvents": {"default": 25000000, "type": int, "validate": lambda x: x > 0},
                     "BlockCloseMaxSize": {"default": 5000000000000, "type": int, "validate": lambda x: x > 0},

                     # dashboard activity
                     "Dashboard": {"default": "production", "type": str, "validate": activity},
                     # team name
                     "Team": {"default": "", "type": safeStr, "assign_optional": False},
                     "PrepID": {"default": None, "null": True},
                     "RobustMerge": {"default": True, "type": strToBool},
                     "ConfigCacheID": {"optional": False, "validate": None},
                     "ConfigCacheUrl": {"default": "https://cmsweb.cern.ch/couchdb", "validate": couchurl},
                     # these 3 parameters are overwritten by the ReqMgr2 configuration
                     "CouchURL": {"default": "https://cmsweb.cern.ch/couchdb", "validate": couchurl},
                     "CouchDBName": {"default": "reqmgr_config_cache", "type": str, "validate": identifier},
                     "CouchWorkloadDBName": {"default": "reqmgr_workload_cache", "validate": identifier}
                    }

        # Set defaults for the argument specification
        StdBase.setDefaultArgumentsProperty(arguments)

        return arguments

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
            elif arg == "ConfigCacheUrl":
                import os
                #schema[arg] = 'http://localhost:5984'
                schema[arg] = os.environ["COUCHURL"]
            elif arg == "CouchDBName":
                schema[arg] = "reqmgr_config_cache_t"
            elif arg == "ScramArch":
                schema[arg] = "slc6_amd64_gcc491"
            elif arg == "SiteWhitelist":
                schema[arg] = ['T2_XX_SiteA', 'T2_XX_SiteB', 'T2_XX_SiteC']
            elif not workloadDefinition[arg]["optional"]:
                if workloadDefinition[arg]["type"] == str:
                    if arg == "InputDataset":
                        schema[arg] = "/MinimumBias/ComissioningHI-v1/RAW"
                    elif arg == "CMSSWVersion":
                        schema[arg] = "CMSSW_7_6_2"
                    else:
                        schema[arg] = "FAKE"
                elif workloadDefinition[arg]["type"] == int or workloadDefinition[arg]["type"] == float:
                    schema[arg] = 1
                else:  # A non-optional list or similar. Just copy.
                    schema[arg] = workloadDefinition[arg]['default']
            else:
                schema[arg] = workloadDefinition[arg]['default']
        return schema
