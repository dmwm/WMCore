#!/usr/bin/env python
"""
_PromptReco_

Standard PromptReco workflow.
"""

from WMCore.Lexicon import dataset, couchurl, identifier, block, procstringT0
from WMCore.WMSpec.StdSpecs.StdBase import StdBase
from WMCore.WMSpec.WMWorkloadTools import makeList, strToBool


class PromptRecoWorkloadFactory(StdBase):
    """
    _PromptRecoWorkloadFactory_

    Stamp out PromptReco workflows.
    """

    def buildWorkload(self):
        """
        _buildWorkload_

        Build the workload given all of the input parameters.

        Not that there will be LogCollect tasks created for each processing
        task and Cleanup tasks created for each merge task.

        """
        (self.inputPrimaryDataset, self.inputProcessedDataset,
         self.inputDataTier) = self.inputDataset[1:].split("/")

        workload = self.createWorkload()
        workload.setDashboardActivity("tier0")
        self.reportWorkflowToDashboard(workload.getDashboardActivity())
        workload.setWorkQueueSplitPolicy("Block", self.procJobSplitAlgo,
                                         self.procJobSplitArgs)

        cmsswStepType = "CMSSW"
        taskType = "Processing"

        recoOutputs = []
        for dataTier in self.writeTiers:
            recoOutputs.append( { 'dataTier' : dataTier,
                                  'eventContent' : dataTier,
                                  'moduleLabel' : "write_%s" % dataTier } )

        recoTask = workload.newTask("Reco")

        scenarioArgs = { 'globalTag' : self.globalTag,
                         'skims' : self.alcaSkims,
                         'PhysicsSkims' : self.physicsSkims,
                         'dqmSeq' : self.dqmSequences,
                         'outputs' : recoOutputs }
        if self.globalTagConnect:
            scenarioArgs['globalTagConnect'] = self.globalTagConnect

        recoOutMods = self.setupProcessingTask(recoTask, taskType, self.inputDataset,
                                               scenarioName = self.procScenario,
                                               scenarioFunc = "promptReco",
                                               scenarioArgs = scenarioArgs,
                                               splitAlgo = self.procJobSplitAlgo,
                                               splitArgs = self.procJobSplitArgs,
                                               stepType = cmsswStepType,
                                               forceUnmerged = ["write_ALCARECO"] if 'ALCARECO' in self.writeTiers else False)

        if self.doLogCollect:
            self.addLogCollectTask(recoTask)

        recoMergeTasks = {}
        for recoOutLabel, recoOutInfo in recoOutMods.items():
            if recoOutInfo['dataTier'] != "ALCARECO":
                mergeTask = self.addMergeTask(recoTask, self.procJobSplitAlgo, recoOutLabel,
                                              doLogCollect = self.doLogCollect)
                recoMergeTasks[recoOutInfo['dataTier']] = mergeTask

            else:
                alcaTask = recoTask.addTask("AlcaSkim")

                scenarioArgs = { 'globalTag' : self.globalTag,
                                 'skims' : self.alcaSkims,
                                 'primaryDataset' : self.inputPrimaryDataset }
                if self.globalTagConnect:
                    scenarioArgs['globalTagConnect'] = self.globalTagConnect

                alcaOutMods = self.setupProcessingTask(alcaTask, taskType,
                                                       inputStep = recoTask.getStep("cmsRun1"),
                                                       inputModule = recoOutLabel,
                                                       scenarioName = self.procScenario,
                                                       scenarioFunc = "alcaSkim",
                                                       scenarioArgs = scenarioArgs,
                                                       splitAlgo = "WMBSMergeBySize",
                                                       splitArgs = {"max_merge_size": self.maxMergeSize,
                                                                    "min_merge_size": self.minMergeSize,
                                                                    "max_merge_events": self.maxMergeEvents},
                                                       stepType = cmsswStepType,
                                                       useMulticore = False)
                if self.doLogCollect:
                    self.addLogCollectTask(alcaTask, taskName = "AlcaSkimLogCollect")
                self.addCleanupTask(recoTask, recoOutLabel)

                for alcaOutLabel in alcaOutMods.keys():
                    self.addMergeTask(alcaTask, self.procJobSplitAlgo, alcaOutLabel,
                                      doLogCollect = self.doLogCollect)

        workload.setBlockCloseSettings(self.blockCloseDelay,
                                       workload.getBlockCloseMaxFiles(),
                                       workload.getBlockCloseMaxEvents(),
                                       workload.getBlockCloseMaxSize())

        # setting the parameters which need to be set for all the tasks
        # sets acquisitionEra, processingVersion, processingString
        workload.setTaskPropertiesFromWorkload()

        # set the LFN bases (normally done by request manager)
        # also pass runNumber (workload evaluates it)
        workload.setLFNBase(self.mergedLFNBase, self.unmergedLFNBase,
                            runNumber = self.runNumber)

        return workload

    def __call__(self, workloadName, arguments):
        """
        _call_

        Create a ReReco workload with the given parameters.
        """
        StdBase.__call__(self, workloadName, arguments)

        # These are mostly place holders because the job splitting algo and
        # parameters will be updated after the workflow has been created.
        self.procJobSplitArgs = {}
        if self.procJobSplitAlgo == "EventBased" or self.procJobSplitAlgo == "EventAwareLumiBased":
            if self.eventsPerJob is None:
                self.eventsPerJob = int((8.0 * 3600.0) / self.timePerEvent)
            self.procJobSplitArgs["events_per_job"] = self.eventsPerJob
            if self.procJobSplitAlgo == "EventAwareLumiBased":
                self.procJobSplitArgs["max_events_per_lumi"] = 100000
        elif self.procJobSplitAlgo == "LumiBased":
            self.procJobSplitArgs["lumis_per_job"] = self.lumisPerJob
        elif self.procJobSplitAlgo == "FileBased":
            self.procJobSplitArgs["files_per_job"] = self.filesPerJob
        self.skimJobSplitArgs = {}
        if self.skimJobSplitAlgo == "EventBased" or self.skimJobSplitAlgo == "EventAwareLumiBased":
            if self.eventsPerJob is None:
                self.eventsPerJob = int((8.0 * 3600.0) / self.timePerEvent)
            self.skimJobSplitArgs["events_per_job"] = self.eventsPerJob
            if self.skimJobSplitAlgo == "EventAwareLumiBased":
                self.skimJobSplitArgs["max_events_per_lumi"] = 20000
        elif self.skimJobSplitAlgo == "LumiBased":
            self.skimJobSplitArgs["lumis_per_job"] = self.lumisPerJob
        elif self.skimJobSplitAlgo == "FileBased":
            self.skimJobSplitArgs["files_per_job"] = self.filesPerJob
        self.skimJobSplitArgs = arguments.get("SkimJobSplitArgs",
                                              {"files_per_job": 1,
                                               "include_parents": True})

        return self.buildWorkload()

    @staticmethod
    def getWorkloadArguments():
        baseArgs = StdBase.getWorkloadArguments()
        specArgs = {"RequestType" : {"default" : "PromptReco", "optional" : True,
                                      "attr" : "requestType"},
                    "Scenario" : {"default" : None, "type" : str,
                                  "optional" : False, "validate" : None,
                                  "attr" : "procScenario", "null" : False},
                    "GlobalTag" : {"default" : None, "type" : str,
                                   "optional" : False, "validate" : None,
                                   "attr" : "globalTag", "null" : False},
                    "ProcessingString": {"default": "", "validate": procstringT0},
                    "WriteTiers" : {"default" : ["RECO", "AOD", "DQM", "ALCARECO"],
                                    "type" : makeList, "optional" : False,
                                    "validate" : None,
                                    "attr" : "writeTiers", "null" : False},
                    "AlcaSkims" : {"default" : ["TkAlCosmics0T","MuAlGlobalCosmics","HcalCalHOCosmics"],
                                   "type" : makeList, "optional" : False,
                                   "validate" : None,
                                   "attr" : "alcaSkims", "null" : False},
                    "InputDataset" : {"default" : "/Cosmics/Run2012A-v1/RAW", "type" : str,
                                      "optional" : False, "validate" : dataset,
                                      "attr" : "inputDataset", "null" : False},
                    "PhysicsSkims" : {"default" : [], "type" : makeList,
                                      "optional" : True, "validate" : None,
                                      "attr" : "physicsSkims", "null" : False},
                    "InitCommand" : {"default" : None, "type" : str,
                                     "optional" : True, "validate" : None,
                                     "attr" : "initCommand", "null" : True},
                    "EnvPath" : {"default" : None, "type" : str,
                                 "optional" : True, "validate" : None,
                                 "attr" : "envPath", "null" : True},
                    "BinPath" : {"default" : None, "type" : str,
                                 "optional" : True, "validate" : None,
                                 "attr" : "binPath", "null" : True},
                    "DoLogCollect" : {"default" : True, "type" : strToBool,
                                      "optional" : True, "validate" : None,
                                      "attr" : "doLogCollect", "null" : False},
                    "BlockBlacklist" : {"default" : [], "type" : makeList,
                                        "optional" : True, "validate" : lambda x: all([block(y) for y in x]),
                                        "attr" : "blockBlacklist", "null" : False},
                    "BlockWhitelist" : {"default" : [], "type" : makeList,
                                        "optional" : True, "validate" : lambda x: all([block(y) for y in x]),
                                        "attr" : "blockWhitelist", "null" : False},
                    "RunBlacklist" : {"default" : [], "type" : makeList,
                                      "optional" : True, "validate" : lambda x: all([int(y) > 0 for y in x]),
                                      "attr" : "runBlacklist", "null" : False},
                    "RunWhitelist" : {"default" : [], "type" : makeList,
                                      "optional" : True, "validate" : lambda x: all([int(y) > 0 for y in x]),
                                      "attr" : "runWhitelist", "null" : False},
                    "SplittingAlgo" : {"default" : "EventBased", "type" : str,
                                       "optional" : True, "validate" : lambda x: x in ["EventBased", "LumiBased",
                                                                                       "EventAwareLumiBased", "FileBased"],
                                       "attr" : "procJobSplitAlgo", "null" : False},
                    "EventsPerJob" : {"default" : 500, "type" : int,
                                      "optional" : True, "validate" : lambda x : x > 0,
                                      "attr" : "eventsPerJob", "null" : False},
                    "LumisPerJob" : {"default" : 8, "type" : int,
                                     "optional" : True, "validate" : lambda x : x > 0,
                                     "attr" : "lumisPerJob", "null" : False},
                    "FilesPerJob" : {"default" : 1, "type" : int,
                                     "optional" : True, "validate" : lambda x : x > 0,
                                     "attr" : "filesPerJob", "null" : False},
                    "SkimSplittingAlgo" : {"default" : "FileBased", "type" : str,
                                           "optional" : True, "validate" : lambda x: x in ["EventBased", "LumiBased",
                                                                                           "EventAwareLumiBased", "FileBased"],
                                           "attr" : "skimJobSplitAlgo", "null" : False},
                    "SkimEventsPerJob" : {"default" : 500, "type" : int,
                                          "optional" : True, "validate" : lambda x : x > 0,
                                          "attr" : "skimEventsPerJob", "null" : False},
                    "SkimLumisPerJob" : {"default" : 8, "type" : int,
                                         "optional" : True, "validate" : lambda x : x > 0,
                                         "attr" : "skimLumisPerJob", "null" : False},
                    "SkimFilesPerJob" : {"default" : 1, "type" : int,
                                         "optional" : True, "validate" : lambda x : x > 0,
                                         "attr" : "skimFilesPerJob", "null" : False},
                    "BlockCloseDelay" : {"default" : 86400, "type" : int,
                                         "optional" : True, "validate" : lambda x : x > 0,
                                         "attr" : "blockCloseDelay", "null" : False}
                    }
        
        baseArgs.update(specArgs)
        # add more optional arguments in case it is created using ReqMgr (not T0 case but should support both)
        reqMgrArguments = {"CouchURL" : {"default" : "https://cmsweb.cern.ch/couchdb",
                                         "validate" : couchurl},
                           "CouchDBName" : {"default" : "reqmgr_config_cache", "type" : str,
                                            "validate" : identifier},
                           "ConfigCacheUrl" : {"default" :"https://cmsweb.cern.ch/couchdb", "validate" : couchurl},
                           "ConfigCacheID" : {"optional": True, "null": True},
                           "CouchWorkloadDBName" : {"default" : "reqmgr_workload_cache", "validate" : identifier},
                         }
        baseArgs.update(reqMgrArguments)
        StdBase.setDefaultArgumentsProperty(baseArgs)
        return baseArgs
