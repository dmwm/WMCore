#!/usr/bin/env python
#pylint: disable-msg=W0201, W0142, W0102
# W0201: Steve defines all global vars in __call__
#   I don't know why, but I'm not getting blamed for it
# W0142: Dave loves the ** magic
# W0102: Dangerous default values?  I live on danger!
#   Allows us to use a dict as a default
"""
_ReReco_

Standard ReReco workflow.
"""




import os

from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.WMStep import makeWMStep
from WMCore.WMSpec.Steps.StepFactory import getStepTypeHelper

from WMCore.Cache.ConfigCache import WMConfigCache
from WMCore.WMSpec.StdSpecs import SplitAlgoStartPolicyMap
from WMCore.WMSpec.StdSpecs.StdBase import StdBase

def getTestArguments():
    """
    _getTestArguments_

    This should be where the default REQUIRED arguments go
    This serves as documentation for what is currently required 
    by the standard ReReco workload in importable format.

    NOTE: These are test values.  If used in real workflows they
    will cause everything to crash/die/break, and we will be forced
    to hunt you down and kill you.
    """

    arguments = {
        "CmsPath": "/uscmst1/prod/sw/cms",
        "AcquisitionEra": "WMAgentCommissioning10",
        "Requestor": "sfoulkes@fnal.gov",
        "InputDataset": "/MinimumBias/Commissioning10-v4/RAW",
        "CMSSWVersion": "CMSSW_3_5_8",
        "ScramArch": "slc5_ia32_gcc434",
        "ProcessingVersion": "v2scf",
        "SkimInput": "output",
        "GlobalTag": "GR10_P_v4::All",
        
        "ProcessingConfig": "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/rereco_FirstCollisions_MinimumBias_35X.py?revision=1.8",
        "SkimConfig": "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/DataOps/python/prescaleskimmer.py?revision=1.1",
        
        "CouchUrl": os.environ.get("COUCHURL", None),
        "CouchDBName": "wmagent_config_cache",
        "Scenario": ""
        
        #     "Scenario": "cosmics",
        #     "ProcessingConfig": "",
        #     "SkimConfig": ""
        }

    return arguments

class ReRecoWorkloadFactory(StdBase):
    """
    _ReRecoWorkloadFactory_

    Stamp out ReReco workflows.
    """
    def __init__(self):
        StdBase.__init__(self)
        return

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
        workload.setOwner(self.owner)
        workload.data.properties.acquisitionEra = self.acquisitionEra        
        return workload
    
    def setReRecoPolicy(self, workload, splitAlgo, splitAgrs):
        """
        set rereco policy according to the top level task job splitting algorithm
        """
        workload.setStartPolicy("Block",
                                SliceType = SplitAlgoStartPolicyMap.getSliceType(splitAlgo), 
                                SliceSize = SplitAlgoStartPolicyMap.getSliceSize(splitAlgo, splitAgrs))
        
        workload.setEndPolicy("SingleShot")
        
    def setupProcessingTask(self, procTask, taskType, inputDataset = None, inputStep = None,
                            inputModule = None, scenarioName = None,
                            scenarioFunc = None, scenarioArgs = None, couchUrl = None,
                            couchDBName = None, configDoc = None, splitAlgo = "FileBased",
                            splitArgs = {'files_per_job': 1}):
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
          configDoc not empty - Use a ConfigCache config, couchUrl and
            couchDBName must not be empty.
          configDoc empty - Use a Configuration.DataProcessing config.  The
            scenarioName, scenarioFunc and scenarioArgs parameters must not be
            empty.
        """
        #self.addDashboardMonitoring(procTask)
        procTaskCmssw = procTask.makeStep("cmsRun1")
        procTaskCmssw.setStepType("CMSSW")
        procTaskStageOut = procTaskCmssw.addStep("stageOut1")
        procTaskStageOut.setStepType("StageOut")
        procTaskLogArch = procTaskCmssw.addStep("logArch1")
        procTaskLogArch.setStepType("LogArchive")
        procTask.applyTemplates()

        procTask.setTaskLogBaseLFN(self.unmergedLFNBase)

        procTask.setSiteWhitelist(self.siteWhitelist)
        procTask.setSiteBlacklist(self.siteBlacklist)

        newSplitArgs = {}
        for argName in splitArgs.keys():
            newSplitArgs[str(argName)] = splitArgs[argName]
        
        procTask.setSplittingAlgorithm(splitAlgo, **newSplitArgs)
        procTask.addGenerator("BasicNaming")
        procTask.addGenerator("BasicCounter")
        procTask.setTaskType(taskType)
        
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
        procTaskCmsswHelper.setGlobalTag(self.globalTag)
        procTaskStageHelper.setMinMergeSize(self.minMergeSize)
        procTaskCmsswHelper.cmsswSetup(self.frameworkVersion, softwareEnvironment = "",
                                       scramArch = self.scramArch)
        if configDoc != None:
            procTaskCmsswHelper.setConfigCache(couchUrl, configDoc, couchDBName)
        else:
            procTaskCmsswHelper.setDataProcessingConfig(scenarioName, scenarioFunc,
                                                        **scenarioArgs)
        
        return procTask

    def addLogCollectTask(self, parentTask, taskName = "LogCollect"):
        """
        _addLogCollectTask_
        
        Create a LogCollect task for log archives that are produced by the
        parent task.
        """
        logCollectTask = parentTask.addTask(taskName)
        #self.addDashboardMonitoring(logCollectTask)        
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

    def addOutputModule(self, parentTask, parentTaskSplitting, outputModuleName,
                        dataTier, filterName):
        """
        _addOutputModule_
        
        Add an output module to the geven processing task.  This will also
        create merge and cleanup tasks for the output of the output module.
        A handle to the merge task is returned to make it easy to use the merged
        output of the output module as input to another task.
        """
        if filterName != None and filterName != "":
            processedDatasetName = "%s-%s-%s" % (self.acquisitionEra, filterName,
                                                 self.processingVersion)
        else:
            processedDatasetName = "%s-%s" % (self.acquisitionEra,
                                              self.processingVersion)
        
        unmergedLFN = "%s/%s/%s" % (self.unmergedLFNBase, dataTier,
                                    processedDatasetName)
        mergedLFN = "%s/%s/%s" % (self.mergedLFNBase, dataTier,
                                  processedDatasetName)
        cmsswStep = parentTask.getStep("cmsRun1")
        cmsswStepHelper = cmsswStep.getTypeHelper()
        cmsswStepHelper.addOutputModule(outputModuleName,
                                        primaryDataset = self.inputPrimaryDataset,
                                        processedDataset = processedDatasetName,
                                        dataTier = dataTier,
                                        lfnBase = unmergedLFN,
                                        mergedLFNBase = mergedLFN)
        return self.addMergeTask(parentTask, parentTaskSplitting,
                                 outputModuleName, dataTier, processedDatasetName)

    def addMergeTask(self, parentTask, parentTaskSplitting, parentOutputModule,
                     dataTier, processedDatasetName):
        """
        _addMergeTask_
    
        Create a merge task for files produced by the parent task.
        """
        mergeTask = parentTask.addTask("%sMerge%s" % (parentTask.name(), parentOutputModule))
        #self.addDashboardMonitoring(mergeTask)
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

        if parentTaskSplitting == "EventBased":
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

        mergedLFN = "%s/%s/%s" % (self.mergedLFNBase, dataTier, processedDatasetName)    
        mergeTaskCmsswHelper.addOutputModule("Merged",
                                             primaryDataset = self.inputPrimaryDataset,
                                             processedDataset = processedDatasetName,
                                             dataTier = dataTier,
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
        #self.addDashboardMonitoring(cleanupTask)        
        cleanupTask.setTaskType("Cleanup")

        parentTaskCmssw = parentTask.getStep("cmsRun1")
        cleanupTask.setInputReference(parentTaskCmssw, outputModule = parentOutputModuleName)
        cleanupTask.setSplittingAlgorithm("SiblingProcessingBased", files_per_job = 50)
       
        cleanupStep = cleanupTask.makeStep("cleanupUnmerged%s" % parentOutputModuleName)
        cleanupStep.setStepType("DeleteFiles")
        cleanupTask.applyTemplates()
        return

    def __call__(self, workloadName, arguments):
        """
        _call_

        Create a ReReco workload with the given parameters.
        """
        # Required parameters.
        self.acquisitionEra = arguments["AcquisitionEra"]
        self.owner = arguments["Requestor"]
        self.inputDataset = arguments["InputDataset"]
        self.frameworkVersion = arguments["CMSSWVersion"]
        self.scramArch = arguments["ScramArch"]
        self.processingVersion = arguments["ProcessingVersion"]
        self.skimInput = arguments["SkimInput"]
        self.globalTag = arguments["GlobalTag"]        
        self.cmsPath = arguments["CmsPath"]
        self.couchUrl = arguments["CouchUrl"]

        # Required parameters that can be empty.
        self.processingConfig = arguments["ProcessingConfig"]
        self.skimConfig = arguments["SkimConfig"]
        self.scenario = arguments["Scenario"]
        self.couchDBName = arguments.get("CouchDBName", "wmagent_config_cache")        
        
        # Optional arguments.
        self.dbsUrl = arguments.get("DbsUrl", "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet")
        self.blockBlacklist = arguments.get("BlockBlacklist", [])
        self.blockWhitelist = arguments.get("BlockWhitelist", [])
        self.runBlacklist = arguments.get("RunBlacklist", [])
        self.runWhitelist = arguments.get("RunWhitelist", [])
        self.siteBlacklist = arguments.get("SiteBlacklist", [])
        self.siteWhitelist = arguments.get("SiteWhitelist", [])
        self.unmergedLFNBase = arguments.get("UnmergedLFNBase", "/store/temp/WMAgent/unmerged")
        self.mergedLFNBase = arguments.get("MergedLFNBase", "/store/temp/WMAgent/merged")
        self.minMergeSize = arguments.get("MinMergeSize", 500000000)
        self.maxMergeSize = arguments.get("MaxMergeSize", 4294967296)
        self.maxMergeEvents = arguments.get("MaxMergeEvents", 100000)
        self.emulation = arguments.get("Emulation", False)
        self.stdJobSplitAlgo  = arguments.get("StdJobSplitAlgo", 'FileBased')
        self.stdJobSplitArgs  = arguments.get("StdJobSplitArgs", {'files_per_job': 1})
        self.skimJobSplitAlgo = arguments.get("SkimJobSplitAlgo", 'TwoFileBased')
        self.skimJobSplitArgs = arguments.get("SkimJobSplitArgs", {'files_per_job': 1})

        self.workloadName = workloadName
        (self.inputPrimaryDataset, self.inputProcessedDataset, self.inputDataTier) = \
                                   self.inputDataset[1:].split("/")

        procConfigDoc = None
        skimConfigDoc = None
        if self.couchUrl != None and self.couchDBName != None:
            myConfigCache = WMConfigCache(dbname2 = self.couchDBName, dburl = self.couchUrl)
            if self.processingConfig != "":
                (procConfigDoc, rev) = myConfigCache.addConfig(self.processingConfig)
            if self.skimConfig != "":
                (skimConfigDoc, rev) = myConfigCache.addConfig(self.skimConfig)

        workload = self.createWorkload()
        procTask = workload.newTask("ReReco")

        self.setupProcessingTask(procTask, "Processing", self.inputDataset,
                                 scenarioName = self.scenario, scenarioFunc = "promptReco",
                                 scenarioArgs = {"globalTag": self.globalTag, "writeTiers": ["RECO", "ALCARECO"]}, 
                                 couchUrl = self.couchUrl, couchDBName = self.couchDBName,
                                 configDoc = procConfigDoc, splitAlgo = self.stdJobSplitAlgo,
                                 splitArgs = self.stdJobSplitArgs) 
        
        #set the startPolicy according to the to level task
        self.setReRecoPolicy(workload, self.stdJobSplitAlgo, self.stdJobSplitArgs)
        self.addLogCollectTask(procTask)

        procOutput = {}

        processingOutputModules = self.getOutputModuleInfo(self.processingConfig,
                                                           self.scenario, "promptReco",
                                                           scenarioArgs = {"globalTag": self.globalTag, "writeTiers": ["RECO", "ALCARECO"]})
        
        for (outputModuleName, datasetInfo) in processingOutputModules.iteritems():
            mergeTask = self.addOutputModule(procTask, self.stdJobSplitAlgo,
                                             outputModuleName, datasetInfo["dataTier"],
                                             datasetInfo["filterName"])
            procOutput[outputModuleName] = mergeTask

        if skimConfigDoc == None:
            return workload

        if not procOutput.has_key(self.skimInput):
            error = "Processing config does not have the following output module: %s.  " % self.skimInput
            error += "Please change your skim input to be one of the following: %s" % procOutput.keys()
            raise Exception, error
        
        parentMergeTask = procOutput[self.skimInput]
        skimTask = parentMergeTask.addTask("Skims")
        parentCmsswStep = parentMergeTask.getStep("cmsRun1")
        self.setupProcessingTask(skimTask, "Skim", inputStep = parentCmsswStep, inputModule = "Merged",
                                 couchUrl = self.couchUrl, couchDBName = self.couchDBName,
                                 configDoc = skimConfigDoc, splitAlgo = self.skimJobSplitAlgo,
                                 splitArgs = self.skimJobSplitArgs)
        self.addLogCollectTask(skimTask, taskName = "skimLogCollect")

        skimOutputModules = self.getOutputModuleInfo(self.skimConfig,
                                                     self.scenario, "promptReco",
                                                     scenarioArgs = {})

        for (outputModuleName, datasetInfo) in skimOutputModules.iteritems():
            self.addOutputModule(skimTask, self.skimJobSplitAlgo,
                                 outputModuleName, datasetInfo["dataTier"],
                                 datasetInfo["filterName"])
            
        return workload

def rerecoWorkload(workloadName, arguments):
    """
    _rerecoWorkload_

    Instantiate the ReRecoWorkflowFactory and have it generate a workload for
    the given parameters.
    """
    myReRecoFactory = ReRecoWorkloadFactory()
    return myReRecoFactory(workloadName, arguments)
