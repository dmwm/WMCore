#!/usr/bin/env python
"""
_StdBase_

Base class with helper functions for standard WMSpec files.
"""
from __future__ import division
from future.utils import viewitems
from builtins import range, object

import logging
import json

from Utils.PythonVersion import PY3
from Utils.Utilities import decodeBytesToUnicodeConditional
from Utils.Utilities import makeList, makeNonEmptyList, strToBool, safeStr
from WMCore.WMRuntime.Tools.Scram import isCMSSWSupported
from WMCore.Cache.WMConfigCache import ConfigCache, ConfigCacheException
from WMCore.Lexicon import (couchurl, procstring, activity, procversion, primdataset,
                            gpuParameters, lfnBase, identifier, acqname, cmsname,
                            dataset, block, campaign, subRequestType)
from WMCore.ReqMgr.DataStructs.RequestStatus import REQUEST_START_STATE
from WMCore.ReqMgr.Tools.cms import releases, architectures
from WMCore.Services.Rucio.RucioUtils import RUCIO_RULES_PRIORITY
from WMCore.WMSpec.WMSpecErrors import WMSpecFactoryException
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.WMWorkloadTools import (makeLumiList, checkDBSURL, validateArgumentsCreate)


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
        argumentDefinition = self.getWorkloadCreateArgs()
        for arg in argumentDefinition:
            setattr(self, argumentDefinition[arg]["attr"], None)

        # Internal parameters
        self.workloadName = None
        self.config_cache = {}

        return

    def __call__(self, workloadName, arguments):
        """
        __call__

        Look through the arguments that were passed into the workload's call
        method and pull out any that are setup by this base class.
        """
        self.workloadName = workloadName
        argumentDefinition = self.getWorkloadCreateArgs()
        for arg in argumentDefinition:
            try:
                if arg in arguments:
                    if arguments[arg] is None:
                        setattr(self, argumentDefinition[arg]["attr"], arguments[arg])
                    else:
                        value = arguments[arg]
                        setattr(self, argumentDefinition[arg]["attr"], value)
                elif argumentDefinition[arg]["optional"]:
                    defaultValue = argumentDefinition[arg]["default"]
                    setattr(self, argumentDefinition[arg]["attr"], defaultValue)
            except Exception as ex:
                raise WMSpecFactoryException("parameter %s: %s" % (arg, str(ex)))

        # TODO: this replace can be removed in one year from now, thus March 2022
        if hasattr(self, "dbsUrl"):
            self.dbsUrl = self.dbsUrl.replace("cmsweb.cern.ch", "cmsweb-prod.cern.ch")
            self.dbsUrl = self.dbsUrl.rstrip("/")

        return

    # static copy of the skim mapping
    skimMap = {}

    @staticmethod
    def calcEvtsPerJobLumi(ePerJob, ePerLumi, tPerEvent, requestedEvents=None):
        """
        _calcEvtsPerJobLumi_

        Given RequestNumEvents (for MC from scratch), EventsPerJob,
        EventsPerLumi and TimePerEvent information, calculates the final
        values for EventsPerJob and EventsPerLumi.

        Final result will always be an EventsPerJob multiple of EventsPerLumi,
        no matter whether EventsPerJob was provided or not. In addition to that,
        makes sure EventsPerJob is not greater than RequestNumEvents.
        :param ePerJob: events per job
        :param ePerLumi: events per lumi
        :param tPerEvent: time per event
        """
        # if not set, let's calculate an 8h job and set it for you
        if ePerJob is None:
            ePerJob = int((8.0 * 3600.0) / tPerEvent)
        if requestedEvents and ePerJob > requestedEvents:
            ePerJob = requestedEvents

        if ePerLumi is None:
            ePerLumi = ePerJob
        elif ePerLumi > ePerJob:
            ePerLumi = ePerJob
        else:
            # then make EventsPerJob multiple of EventsPerLumi and still closer to 8h jobs
            multiplier = int(round(ePerJob / ePerLumi))
            # make sure not to have 0 EventsPerJob
            multiplier = max(multiplier, 1)
            ePerJob = ePerLumi * multiplier

        return ePerJob, ePerLumi

    @staticmethod
    def skimToDataTier(cmsswVersion, skim):
        """
        Start subprocess and call CMSSW python code to retrieve data tier for given skim

        Detects a usable scram arch for this CMSSW release on this machine

        Cache the results and use cache for lookup if possible

        """
        if not cmsswVersion:
            return None

        if cmsswVersion in StdBase.skimMap:
            if skim in StdBase.skimMap[cmsswVersion]:
                return StdBase.skimMap[cmsswVersion][skim]
        else:
            StdBase.skimMap[cmsswVersion] = {}

        import glob
        import subprocess

        p = subprocess.Popen("/cvmfs/cms.cern.ch/common/cmsos", stdout=subprocess.PIPE, shell=True)
        cmsos = p.communicate()[0]
        cmsos = decodeBytesToUnicodeConditional(cmsos, condition=PY3).strip()

        scramBaseDirs = glob.glob("/cvmfs/cms.cern.ch/%s*/cms/cmssw/%s" % (cmsos, cmsswVersion))
        if not scramBaseDirs:
            scramBaseDirs = glob.glob("/cvmfs/cms.cern.ch/%s*/cms/cmssw-patch/%s" % (cmsos, cmsswVersion))
            if not scramBaseDirs:
                return None

        command = "source /cvmfs/cms.cern.ch/cmsset_default.sh\n"
        command += "cd %s\n" % scramBaseDirs[0]
        command += "eval `scramv1 runtime -sh`\n"

        if isCMSSWSupported(cmsswVersion, "CMSSW_10_3_0"):
            command += """python3 -c 'from Configuration.StandardSequences.Skims_cff import getSkimDataTier\n"""
        else:
            command += """python -c 'from Configuration.StandardSequences.Skims_cff import getSkimDataTier\n"""

        command += """dataTier = getSkimDataTier("%s")\n""" % skim
        command += """if dataTier:\n\tprint(dataTier.value())'"""

        p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
        dataTier = p.communicate()[0]
        dataTier = decodeBytesToUnicodeConditional(dataTier, condition=PY3).strip()

        if dataTier == "None":
            dataTier = None

        StdBase.skimMap[cmsswVersion][skim] = dataTier

        return dataTier

    def loadCouchID(self, configDoc=None, configCacheUrl=None, couchDBName=None):
        """
        Load a config document from couch db and return the object
        :param configDoc: the config ID document
        :param configCacheUrl: couch url for the config
        :param couchDBName: couch database name
        :return: config document object
        """
        # TODO: Evaluate if we should call validateConfigCacheExists here
        configCache = None
        if configDoc is not None and configDoc != "":
            if (configCacheUrl, couchDBName) in self.config_cache:
                configCache = self.config_cache[(configCacheUrl, couchDBName)]
            else:
                configCache = ConfigCache(configCacheUrl, couchDBName, True)
                self.config_cache[(configCacheUrl, couchDBName)] = configCache

            configCache.loadByID(configDoc)

        return configCache

    def determineOutputModules(self, scenarioFunc=None, scenarioArgs=None,
                               configCache=None, cmsswVersion=None):
        """
        _determineOutputModules_

        Determine the output module names and associated metadata for the
        given config.
        """
        # set default scenarioArgs to empty dictionary if it is None.
        scenarioArgs = scenarioArgs or {}
        outputModules = {}
        if configCache is not None:
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
                    dataTier = StdBase.skimToDataTier(cmsswVersion, physicsSkim)
                    if dataTier:
                        moduleLabel = "SKIMStream%s" % physicsSkim
                        outputModules[moduleLabel] = {'dataTier': dataTier,
                                                      'filterName': physicsSkim}
                    else:
                        self.raiseValidationException("Can't find physics skim %s in %s" % (physicsSkim, cmsswVersion))

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

    def addRuntimeMonitors(self, task):
        """
        _addRuntimeMonitors_

        Add dashboard monitoring for the given task.
        Memory settings are defined in Megabytes and timing in seconds.
        """
        # Default settings defined by CMS policy
        maxpss = 2.3 * 1024  # 2.3 GiB, but in MiB
        softTimeout = 47 * 3600  # 47h
        hardTimeout = 47 * 3600 + 5 * 60  # 47h + 5 minutes

        monitoring = task.data.section_("watchdog")
        monitoring.interval = 300
        monitoring.monitors = ["PerformanceMonitor"]
        monitoring.section_("PerformanceMonitor")
        monitoring.PerformanceMonitor.maxPSS = maxpss
        monitoring.PerformanceMonitor.softTimeout = softTimeout
        monitoring.PerformanceMonitor.hardTimeout = hardTimeout
        return task

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
        workload.setDbsUrl(self.dbsUrl)
        workload.setPrepID(self.prepID)
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

        self.addRuntimeMonitors(procTask)
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

        newSplitArgs = {}
        for argName in splitArgs:
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
        prepID = taskConf.get("PrepID") or self.prepID
        campaignName = taskConf.get("Campaign") or self.campaign
        procTask.setAcquisitionEra(acqEra)
        procTask.setProcessingString(procStr)
        procTask.setProcessingVersion(procVer)
        procTask.setCampaignName(campaignName)

        if taskType in ["Production", 'PrivateMC'] and totalEvents is not None:
            procTask.addGenerator(seeding)
            procTask.addProduction(totalEvents=totalEvents)
            procTask.setFirstEventAndLumi(firstEvent=self.firstEvent,
                                          firstLumi=self.firstLumi)
        else:
            if inputDataset is not None:
                (primary, processed, tier) = self.inputDataset[1:].split("/")
                procTask.addInputDataset(name=self.inputDataset, primary=primary,
                                         processed=processed, tier=tier, dbsurl=self.dbsUrl,
                                         block_blacklist=self.blockBlacklist,
                                         block_whitelist=self.blockWhitelist,
                                         run_blacklist=self.runBlacklist,
                                         run_whitelist=self.runWhitelist)
            elif inputStep is not None and inputModule is not None:
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
        if taskConf.get("EventStreams") is not None and taskConf['EventStreams'] >= 0:
            eventStreams = taskConf['EventStreams']
        procTaskCmsswHelper.setNumberOfCores(multicore, eventStreams)

        gpuRequired = self.requiresGPU
        if taskConf.get('RequiresGPU', None):
            gpuRequired = taskConf['RequiresGPU']
        # Note that GPUParams has already been validated
        if "GPUParams" in taskConf and json.loads(taskConf['GPUParams']):
            gpuParams = json.loads(taskConf['GPUParams'])
        else:
            gpuParams = json.loads(self.gPUParams)
        procTaskCmsswHelper.setGPUSettings(gpuRequired, gpuParams)

        procTaskCmsswHelper.setUserSandbox(userSandbox)
        procTaskCmsswHelper.setUserFiles(userFiles)
        procTaskCmsswHelper.setGlobalTag(globalTag)
        procTaskCmsswHelper.setPrepId(prepID)
        procTaskCmsswHelper.setOverrideCatalog(self.overrideCatalog)
        procTaskCmsswHelper.setErrorDestinationStep(stepName=procTaskLogArch.name())

        if forceMerged:
            procTaskStageHelper.setMinMergeSize(0, 0)
        elif forceUnmerged and not isinstance(forceUnmerged, list):
            procTaskStageHelper.disableStraightToMerge()
        else:
            procTaskStageHelper.setMinMergeSize(self.minMergeSize, self.maxMergeEvents)
            if forceUnmerged and isinstance(forceUnmerged, list):
                procTaskStageHelper.disableStraightToMergeForOutputModules(forceUnmerged)

        procTaskCmsswHelper.cmsswSetup(cmsswVersion,
                                       softwareEnvironment="",
                                       scramArch=scramArch)

        if "events_per_lumi" in newSplitArgs:
            eventsPerLumi = newSplitArgs["events_per_lumi"]
        procTaskCmsswHelper.setEventsPerLumi(eventsPerLumi)
        configCache = self.loadCouchID(configDoc, configCacheUrl, couchDBName)
        procTaskCmsswHelper.setPhysicsType(configCache)
        configOutput = self.determineOutputModules(scenarioFunc, scenarioArgs,
                                                   configCache, cmsswVersion)
        outputModules = {}
        for outputModuleName in configOutput:
            outputModule = self.addOutputModule(procTask,
                                                outputModuleName,
                                                configOutput[outputModuleName].get('primaryDataset',
                                                                                   self.inputPrimaryDataset),
                                                configOutput[outputModuleName]['dataTier'],
                                                configOutput[outputModuleName].get('filterName', None),
                                                forceMerged=forceMerged, forceUnmerged=forceUnmerged, taskConf=taskConf)
            outputModules[outputModuleName] = outputModule

        if configDoc is not None and configDoc != "":
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
        procTask.setPrepID(prepID)

        # has to be done in the very end such that child tasks are set too
        procTask.setPerformanceMonitor(softTimeout=taskConf.get("SoftTimeout", None),
                                       gracePeriod=taskConf.get("GracePeriod", None))

        # Set procTask physics task type
        procTask.setPhysicsTaskType()

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
        haveFilterName = (filterName is not None and filterName != "")
        haveProcString = (self.processingString is not None and self.processingString != "")
        haveRunNumber = (self.runNumber is not None and self.runNumber > 0)

        taskName = parentTask.name()
        if self.requestType == "StepChain" and "StepName" in taskConf:
            taskName = taskConf["StepName"]
        acqEra = taskConf.get('AcquisitionEra') or self._getDictionaryParams(self.acquisitionEra, taskName)
        procString = taskConf.get('ProcessingString') or self._getDictionaryParams(self.processingString, taskName)
        procVersion = taskConf.get('ProcessingVersion') or self._getDictionaryParams(self.processingVersion,
                                                                                     taskName, 1)

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
        self.addRuntimeMonitors(logCollectTask)
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
        self.addRuntimeMonitors(mergeTask)
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

        dataTier = getattr(parentOutputModule, "dataTier")
        mergeTask.setInputReference(parentTaskCmssw, outputModule=parentOutputModuleName, dataTier=dataTier)

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

        if dataTier == "DQMIO":
            mergeTaskCmsswHelper.setDataProcessingConfig("do_not_use", "merge",
                                                         newDQMIO=True)
        elif dataTier in ("NANOAOD", "NANOAODSIM"):
            mergeTaskCmsswHelper.setDataProcessingConfig("do_not_use", "merge",
                                                         mergeNANO=True)
        else:
            mergeTaskCmsswHelper.setDataProcessingConfig("do_not_use", "merge")

        mergeTaskStageHelper.setMinMergeSize(0, 0)

        self.addOutputModule(mergeTask, "Merged",
                             primaryDataset=getattr(parentOutputModule, "primaryDataset"),
                             dataTier=getattr(parentOutputModule, "dataTier"),
                             filterName=getattr(parentOutputModule, "filterName"),
                             forceMerged=True, taskConf=taskConf)

        self.addCleanupTask(parentTask, parentOutputModuleName, forceTaskName, dataTier)
        if self.enableHarvesting and getattr(parentOutputModule, "dataTier") in ["DQMIO", "DQM"]:
            self.addDQMHarvestTask(mergeTask, "Merged",
                                   uploadProxy=self.dqmUploadProxy,
                                   periodic_harvest_interval=self.periodicHarvestInterval,
                                   doLogCollect=doLogCollect,
                                   dqmHarvestUnit=self.dqmHarvestUnit,
                                   cmsswVersion=cmsswVersion, scramArch=scramArch)

        # only in the very end, in order to get it in for the children tasks as well
        prepID = taskConf.get("PrepID") or parentTask.getPrepID()
        mergeTask.setPrepID(prepID)

        return mergeTask

    def addCleanupTask(self, parentTask, parentOutputModuleName, forceTaskName=None, dataTier=''):
        """
        _addCleanupTask_

        Create a cleanup task to delete files produces by the parent task.
        """
        if forceTaskName is None:
            forceTaskName = parentTask.name()

        cleanupTask = parentTask.addTask("%sCleanupUnmerged%s" % (forceTaskName, parentOutputModuleName))
        self.addRuntimeMonitors(cleanupTask)
        cleanupTask.setTaskType("Cleanup")

        parentTaskCmssw = parentTask.getStep("cmsRun1")
        cleanupTask.setInputReference(parentTaskCmssw, outputModule=parentOutputModuleName, dataTier=dataTier)
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
        self.addRuntimeMonitors(harvestTask)
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
                                   dqmHarvestUnit=dqmHarvestUnit,
                                   cmsswVersion=cmsswVersion, scramArch=scramArch)

        return

    def setupPileup(self, task, pileupConfig, stepName=None):
        """
        _setupPileup_

        Setup pileup for every CMSSW step in the task, unless a stepName
        is given - StepChain case - then only setup pileup for that specific
        step (cmsRun1, cmsRun2, etc).
        pileupConfig has the following data structure:
            {'mc': ['/mc_pd/procds/tier'], 'data': ['/data_pd/procds/tier']}
        """
        for puType, puList in viewitems(pileupConfig):
            task.setInputPileupDatasets(puList)

        if stepName:
            stepHelper = task.getStepHelper(stepName)
            stepHelper.setupPileup(pileupConfig, self.dbsUrl)
            return

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

    def factoryWorkloadConstruction(self, workloadName, arguments):
        """
        _factoryWorkloadConstruction_

        Master build for ReqMgr - builds the entire workload
        and also performs the proper validation.

        Named this way so that nobody else will try to use this name.
        """
        if arguments.get('RequestType') == 'Resubmission':
            self.validateSchema(schema=arguments)
        else:
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

        This uses programatically the definitions in getWorkloadCreateArgs
        for type-checking, existence, null tests and the specific validation functions.

        Any spec-specific extras are implemented in the overriden validateSchema
        """
        # Validate the arguments according to the workload arguments definition
        argumentDefinition = self.getWorkloadCreateArgs()
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

    @staticmethod
    def getWorkloadCreateArgs():
        """
        _getWorkloadCreateArgs_

        This represents the authoritative list of request arguments that are
        allowed by the current spec class during request creation
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
        - assign_optional: This boolean value indicates if the value must be provided when workflow is assigned if False.

        - validate: A function which validates the input after type casting,
                    it returns True if the input is valid, it can throw exceptions on invalid input.
        - attr: This represents the name of the attribute corresponding to the argument in the WMSpec object.
        - null: This indicates if the argument can have None as its value.

        If above is not specified, automatically set by following default value
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
        # if key is not specified (and argument is optional), then it's set to the default value
        arguments = {"RequestType": {"default": "StdBase", "optional": False},
                     "RequestString": {"optional": False, "null": False},
                     # default value will be AcquisitionEra except when AcquistionEra is dict
                     "Campaign": {"default": "", "optional": True, "validate": campaign},
                     "CMSSWVersion": {"validate": lambda x: x in releases(),
                                      "optional": False, "attr": "frameworkVersion"},
                     "ScramArch": {"validate": lambda x: all([y in architectures() for y in x]),
                                   "optional": False, "type": makeNonEmptyList},
                     "GlobalTag": {"optional": False, "null": False},
                     "ConfigCacheID": {"optional": False},

                     "RequestPriority": {"default": 8000, "type": int, "attr": "priority",
                                         "validate": lambda x: (x >= 0 and x < 1e6)},
                     "Group": {"default": "DATAOPS"},
                     "PrepID": {"default": None, "null": True},
                     "OpenRunningTimeout": {"default": 0, "type": int, "null": False,
                                            "validate": lambda x: x >= 0},
                     "GlobalTagConnect": {"null": True},
                     "LumiList": {"default": {}, "type": makeLumiList},
                     "DbsUrl": {"default": "https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader",
                                "null": True, "validate": checkDBSURL},
                     "DashboardHost": {"default": "cms-jobmon.cern.ch"},
                     "DashboardPort": {"default": 8884, "type": int,
                                       "validate": lambda x: x > 0},
                     "TimePerEvent": {"default": 12.0, "type": float, "validate": lambda x: x > 0},
                     "SizePerEvent": {"default": 512.0, "type": float, "validate": lambda x: x > 0},
                     "PeriodicHarvestInterval": {"default": 0, "type": int, "validate": lambda x: x >= 0},
                     "DQMHarvestUnit": {"default": "byRun", "type": str, "attr": "dqmHarvestUnit"},
                     "DQMUploadProxy": {"null": True, "attr": "dqmUploadProxy"},
                     "DQMUploadUrl": {"default": "https://cmsweb.cern.ch/dqm/dev", "attr": "dqmUploadUrl"},
                     "DQMSequences": {"default": [], "type": makeList, "attr": "dqmSequences"},
                     "DQMConfigCacheID": {"null": True, "attr": "dqmConfigCacheID"},
                     "EnableHarvesting": {"default": False, "type": strToBool},
                     "EnableNewStageout": {"default": False, "type": strToBool},
                     "IncludeParents": {"default": False, "type": strToBool},
                     "ConfigCacheUrl": {"default": "https://cmsweb.cern.ch/couchdb", "validate": couchurl},
                     "VoGroup": {"default": "unknown", "attr": "owner_vogroup"},
                     "VoRole": {"default": "unknown", "attr": "owner_vorole"},
                     "ValidStatus": {"default": "PRODUCTION",
                                     "validate": lambda x: x in ("PRODUCTION", "VALID")},
                     "OverrideCatalog": {"null": True},
                     "RunNumber": {"default": 0, "type": int},
                     "RobustMerge": {"default": True, "type": strToBool},
                     "Comments": {"default": ""},
                     "SubRequestType": {"default": "", "validate": subRequestType},
                     "RequiresGPU": {"default": "forbidden",
                                     "validate": lambda x: x in ("forbidden", "optional", "required")},
                     "GPUParams": {"default": json.dumps(None), "validate": gpuParameters},


                     # FIXME (Alan on 27/Mar/017): maybe used by T0 during creation???
                     "MinMergeSize": {"default": 2 * 1024 * 1024 * 1024, "type": int,
                                      "validate": lambda x: x > 0},
                     "MaxMergeSize": {"default": 4 * 1024 * 1024 * 1024, "type": int,
                                      "validate": lambda x: x > 0},
                     "MaxWaitTime": {"default": 24 * 3600, "type": int,
                                     "validate": lambda x: x > 0},
                     "MaxMergeEvents": {"default": 100000000, "type": int,
                                        "validate": lambda x: x > 0},

                     # parameters that can be overwritten during assignment
                     "AcquisitionEra": {"validate": acqname, "optional": False},
                     "ProcessingString": {"validate": procstring, "optional": False},
                     "ProcessingVersion": {"default": 1, "type": int, "validate": procversion},
                     "Memory": {"default": 2300.0, "type": float, "validate": lambda x: x > 0},
                     "Multicore": {"default": 1, "type": int, "validate": lambda x: x > 0},
                     "EventStreams": {"type": int, "default": 0, "validate": lambda x: x >= 0, "null": True},
                     "MergedLFNBase": {"default": "/store/data"},
                     "UnmergedLFNBase": {"default": "/store/unmerged"},
                     "DeleteFromSource": {"default": False, "type": strToBool},
                     }

        # these arguments are internally set by ReqMgr2 and should not be provided by the user
        reqmgrArgs = {"Requestor": {"attr": "owner", "optional": False},
                      "RequestorDN": {"attr": "owner_dn", "optional": False, "null": False},
                      "RequestName": {"optional": False, "null": False, "validate": identifier},
                      "RequestStatus": {"optional": False, "validate": lambda x: x == REQUEST_START_STATE},
                      "RequestTransition": {"optional": False, "type": list},
                      "PriorityTransition": {"optional": False, "type": list},
                      "RequestDate": {"optional": False, "type": list},
                      "CouchURL": {"default": "https://cmsweb.cern.ch/couchdb", "validate": couchurl},
                      "CouchDBName": {"default": "reqmgr_config_cache", "type": str, "validate": identifier},
                      "CouchWorkloadDBName": {"default": "reqmgr_workload_cache", "validate": identifier}
                      }

        arguments.update(reqmgrArgs)
        # Set defaults for the argument specification
        StdBase.setDefaultArgumentsProperty(arguments)

        return arguments

    @staticmethod
    def getWorkloadAssignArgs():
        """
        _getWorkloadAssignArgs_

        This represents the authoritative list of request arguments that are
        allowed by the current spec class during request assignment.

        For more information on how these arguments are built, please have a look
        at the docstring for getWorkloadCreateArgs.
        """
        # if key is not specified (and argument is optional), then it's set to the default value
        arguments = {"RequestPriority": {"type": int, "attr": "priority",
                                         "validate": lambda x: (x >= 0 and x < 1e6)},
                     "RequestStatus": {"assign_optional": False, "validate": lambda x: x == 'assigned'},
                     "Team": {"default": "", "type": safeStr, "assign_optional": False,
                              "validate": lambda x: len(x) > 0},
                     "AcquisitionEra": {"validate": acqname, "assign_optional": True},
                     "ProcessingString": {"validate": procstring, "assign_optional": True},
                     "ProcessingVersion": {"type": int, "validate": procversion, "assign_optional": True},
                     "Memory": {"type": float, "validate": lambda x: x > 0},
                     "Multicore": {"type": int, "validate": lambda x: x > 0},
                     "EventStreams": {"null": True, "type": int, "validate": lambda x: x >= 0},

                     "SiteBlacklist": {"default": [], "type": makeList,
                                       "validate": lambda x: all([cmsname(y) for y in x])},
                     "SiteWhitelist": {"default": [], "type": makeNonEmptyList, "assign_optional": False,
                                       "validate": lambda x: all([cmsname(y) for y in x])},
                     # PhEDEx subscription information
                     "CustodialSites": {"default": [], "type": makeList, "assign_optional": True,
                                        "validate": lambda x: all([cmsname(y) for y in x])},
                     "NonCustodialSites": {"default": [], "type": makeList, "assign_optional": True,
                                           "validate": lambda x: all([cmsname(y) for y in x])},
                     "SubscriptionPriority": {"default": "Low", "assign_optional": True,
                                              "validate": lambda x: x.lower() in RUCIO_RULES_PRIORITY},
                     "DeleteFromSource": {"default": False, "type": strToBool},
                     # merge settings
                     "UnmergedLFNBase": {"assign_optional": True},
                     "MergedLFNBase": {"assign_optional": True},
                     "MinMergeSize": {"default": 2 * 1024 * 1024 * 1024, "type": int,
                                      "validate": lambda x: x > 0},
                     "MaxMergeSize": {"default": 4 * 1024 * 1024 * 1024, "type": int,
                                      "validate": lambda x: x > 0},
                     "MaxWaitTime": {"default": 24 * 3600, "type": int,
                                     "validate": lambda x: x > 0},
                     "MaxMergeEvents": {"default": 100000000, "type": int,
                                        "validate": lambda x: x > 0},
                     # data location management
                     "TrustSitelists": {"default": False, "type": strToBool},
                     "TrustPUSitelists": {"default": False, "type": strToBool},
                     "AllowOpportunistic": {"default": False, "type": strToBool},
                     # from assignment: performance monitoring data
                     "SoftTimeout": {"default": 129600, "type": int, "validate": lambda x: x > 0},
                     "GracePeriod": {"default": 300, "type": int, "validate": lambda x: x > 0},
                     "HardTimeout": {"default": 129600 + 300, "type": int, "validate": lambda x: x > 0},
                     # Block closing information
                     "BlockCloseMaxWaitTime": {"default": 66400, "type": int, "validate": lambda x: x > 0},
                     "BlockCloseMaxFiles": {"default": 500, "type": int, "validate": lambda x: x > 0},
                     "BlockCloseMaxEvents": {"default": 25000000, "type": int, "validate": lambda x: x > 0},
                     "BlockCloseMaxSize": {"default": 5000000000000, "type": int, "validate": lambda x: x > 0},
                     # dashboard activity
                     "Dashboard": {"default": "production", "type": str, "validate": activity},
                     # Override parameters for step (EOS log location, etc
                     # set to "" string or None for eos-lfn-prefix if you don't want to save the log in eos
                     "Override": {"default": {"eos-lfn-prefix": "root://eoscms.cern.ch//eos/cms/store/logs/prod/recent/PRODUCTION"},
                                  "type": dict},
                     # Rucio rule subscription lifetime (used in ContainerRules by T0)
                     "DatasetLifetime": {"default": 0, "type": int, "assign_optional": True, "validate": lambda x: x >= 0},
                     }
        # Set defaults for the argument specification
        StdBase.setDefaultArgumentsProperty(arguments)

        return arguments

    @staticmethod
    def getChainCreateArgs(firstTask=False, generator=False):
        """
        _getChainCreateArgs_

        This represents the authoritative list of request arguments that are
        allowed in each chain (Step/Task) of chained request, during request creation.
        Additional especific arguments must be defined inside each spec class.

        For more information on how these arguments are built, please have a look
        at the docstring for getWorkloadCreateArgs.
        """
        arguments = {'AcquisitionEra': {'optional': True, 'validate': acqname},
                     # Campaign at chain level has no meaning but for CompOps/McM bookkeeping
                     "Campaign": {"optional": True, "validate": campaign},
                     'BlockBlacklist': {'default': [], 'null': False, 'optional': True, 'type': makeList,
                                        'validate': lambda x: all([block(y) for y in x])},
                     'BlockWhitelist': {'default': [], 'null': False, 'optional': True, 'type': makeList,
                                        'validate': lambda x: all([block(y) for y in x])},
                     'CMSSWVersion': {'attr': 'frameworkVersion', 'null': True, 'optional': True,
                                      'validate': lambda x: x in releases()},
                     'ConfigCacheID': {'optional': False, 'type': str},
                     'DataPileup': {'default': None, 'null': False, 'optional': True, 'type': str,
                                    'validate': dataset},
                     # for task/step level, the default EventStreams value (0) comes from the top level
                     'EventStreams': {'null': True, 'type': int, 'validate': lambda x: x >= 0},
                     'EventsPerJob': {'null': True, 'type': int, 'validate': lambda x: x > 0},
                     'EventsPerLumi': {'default': None, 'null': True, 'optional': True, 'type': int,
                                       'validate': lambda x: x > 0},
                     'FilesPerJob': {'default': 1, 'null': False, 'optional': True, 'type': int,
                                     'validate': lambda x: x > 0},
                     'FilterEfficiency': {'default': 1.0, 'null': False, 'optional': True, 'type': float,
                                          'validate': lambda x: x > 0.0},
                     'GlobalTag': {'optional': False, 'type': str},
                     "IncludeParents": {"null": False, "default": False, "type": strToBool},
                     'InputDataset': {'null': False, 'optional': generator or not firstTask, 'validate': dataset},
                     'InputFromOutputModule': {'default': None, 'null': False, 'optional': firstTask},
                     'KeepOutput': {'default': True, 'null': False, 'optional': True, 'type': strToBool},
                     'LheInputFiles': {'default': False, 'null': False, 'optional': True, 'type': strToBool},
                     'LumiList': {'default': {}, 'type': makeLumiList},
                     'LumisPerJob': {'default': 8, 'null': False, 'optional': True, 'type': int,
                                     'validate': lambda x: x > 0},
                     'MCPileup': {'default': None, 'null': False, 'optional': True, 'type': str,
                                  'validate': dataset},
                     'Memory': {'default': None, 'null': True, 'type': float, 'validate': lambda x: x > 0},
                     'Multicore': {'default': 0, 'type': int, 'validate': lambda x: x > 0},
                     'PrepID': {'default': None, 'null': True, 'optional': True, 'type': str},
                     "RequiresGPU": {"default": "forbidden",
                                     "validate": lambda x: x in ("forbidden", "optional", "required")},
                     "GPUParams": {"default": json.dumps(None), "validate": gpuParameters},
                     'PrimaryDataset': {'null': True, 'validate': primdataset, 'attr': 'inputPrimaryDataset'},
                     'ProcessingString': {'optional': True, 'validate': procstring},
                     'ProcessingVersion': {'type': int, 'validate': procversion},
                     'RequestNumEvents': {'null': False, 'optional': not generator, 'type': int,
                                          'validate': lambda x: x > 0},
                     'RunBlacklist': {'default': [], 'null': False, 'optional': True, 'type': makeList,
                                      'validate': lambda x: all([isinstance(y, int) and int(y) > 0 for y in x])},
                     'RunWhitelist': {'default': [], 'null': False, 'optional': True, 'type': makeList,
                                      'validate': lambda x: all([isinstance(y, int) and int(y) > 0 for y in x])},
                     'ScramArch': {'null': True, 'optional': True, 'type': makeNonEmptyList,
                                   'validate': lambda x: all([y in architectures() for y in x])},
                     'Seeding': {'default': 'AutomaticSeeding', 'null': False, 'optional': True, 'type': str,
                                 'validate': lambda x: x in ["ReproducibleSeeding", "AutomaticSeeding"]},
                     'SplittingAlgo': {'default': 'EventAwareLumiBased', 'null': False, 'optional': True,
                                       'validate': lambda x: x in ["EventBased", "LumiBased",
                                                                   "EventAwareLumiBased", "FileBased"]}}

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

        Using the getWorkloadCreateArgs definition, build a request schema
        that may pass basic validation and create successfully a workload
        of the current spec. Only for testing purposes! Any use of this function
        outside of unit tests and integration tests may put your life in danger.
        Note that in some cases like ConfigCacheID, there is no default that will work
        and tests should specifically provide one.
        """
        workloadDefinition = cls.getWorkloadCreateArgs()
        schema = {}
        for arg in workloadDefinition:
            # Dashboard parameter must be re-defined for test purposes
            if arg == "DashboardHost":
                schema[arg] = "127.0.0.1"
            elif arg == "ConfigCacheUrl":
                from os import environ
                schema[arg] = environ["COUCHURL"]
            elif arg == "RequestDate":
                from time import gmtime
                schema[arg] = gmtime()[:6]
            elif arg == "RequestTransition":
                from time import time
                schema[arg] = [{"Status": REQUEST_START_STATE, "UpdateTime": int(time()), "DN": "Fake_DN"}]
            elif arg == "PriorityTransition":
                from time import time
                schema[arg] = [{"Priority": REQUEST_START_STATE, "UpdateTime": int(time()), "DN": "Fake_DN"}]
            elif arg == "RequestStatus":
                schema[arg] = REQUEST_START_STATE
            elif arg == "CouchDBName":
                schema[arg] = "reqmgr_config_cache_t"
            elif arg == "ScramArch":
                schema[arg] = "slc6_amd64_gcc491"
            elif arg == "SiteWhitelist":
                schema[arg] = ['T2_XX_SiteA', 'T2_XX_SiteB', 'T2_XX_SiteC']
            elif arg == "GlobalTag":
                schema[arg] = "GT_DP_V1"
            elif arg == "InputDataset":
                schema[arg] = "/MinimumBias/ComissioningHI-v1/RAW"
            elif not workloadDefinition[arg]["optional"]:
                if workloadDefinition[arg]["type"] == str:
                    if arg == "InputDataset":
                        schema[arg] = "/MinimumBias/ComissioningHI-v1/RAW"
                    elif arg == "CMSSWVersion":
                        schema[arg] = "CMSSW_7_6_2"
                    elif arg == "RequestType":
                        schema[arg] = workloadDefinition[arg]['default']
                    else:
                        schema[arg] = "FAKE"
                elif workloadDefinition[arg]["type"] == int or workloadDefinition[arg]["type"] == float:
                    schema[arg] = 1
                else:  # A non-optional list or similar. Just copy.
                    schema[arg] = workloadDefinition[arg]['default']
            else:
                schema[arg] = workloadDefinition[arg]['default']

        taskStep = {'ConfigCacheID': 'FAKE',
                    'PrimaryDataset': 'FAKE',
                    'EventsPerJob': 100,
                    'GlobalTag': 'GT_DP_V1',
                    'RequestNumEvents': 1000000,
                    'Seeding': 'AutomaticSeeding',
                    'SplittingAlgo': 'EventBased'}
        if schema['RequestType'] == 'TaskChain':
            schema['Task1'] = taskStep
            schema['Task1'].update({'TaskName': 'Task1Name_Test',
                                    'TimePerEvent': 123})
        elif schema['RequestType'] == 'StepChain':
            schema['Step1'] = taskStep
            schema['Step1'].update({'StepName': 'Step1Name_Test'})

        return schema

    @classmethod
    def getAssignTestArguments(cls):
        """
        _getAssignTestArguments_

        Using the getWorkloadAssignArgs definition, build a request schema
        that may pass basic validation and successfully assign a workload
        of the current spec.

        Only for testing purposes! Any use of this function outside of unit
        tests and integration tests may put your life in danger.
        """
        workloadDefinition = cls.getWorkloadAssignArgs()
        schema = {}
        for arg in workloadDefinition:
            if arg == "SiteWhitelist":
                schema[arg] = ["T2_US_TEST_Site"]
            elif arg == "RequestStatus":
                schema[arg] = 'assigned'
            elif not workloadDefinition[arg]["assign_optional"]:
                if workloadDefinition[arg]["type"] == str:
                    if arg == "Team":
                        schema[arg] = "Test-Team"
                    else:
                        schema[arg] = "FAKE"
                elif workloadDefinition[arg]["type"] in (int, float):
                    schema[arg] = 1
                else:
                    schema[arg] = workloadDefinition[arg]['default']
            else:
                schema[arg] = workloadDefinition[arg]['default']
        return schema

    @staticmethod
    def validateGPUSettings(schemaData):
        """
        Method to check whether GPU settings have been provided for
        a workflow (or tasks/step) that requires GPUs (or has it set
        to optional).
        :param schemaData: workflow or task/step dictionary
        :return: nothing if validation is successful, otherwise raises an exception
        """
        if schemaData.get("RequiresGPU") in ("optional", "required"):
            try:
                msg = "Request is set with RequiresGPU={}, ".format(schemaData["RequiresGPU"])
                if not json.loads(schemaData["GPUParams"]):
                    msg += "but GPUParams argument is empty and/or incorrect."
                    raise WMSpecFactoryException(msg)
            except KeyError:
                msg += "but GPUParams argument has not been provided."
                raise WMSpecFactoryException(msg)
        return True
