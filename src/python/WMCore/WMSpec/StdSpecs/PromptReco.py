#!/usr/bin/env python
"""
_PromptReco_

Standard PromptReco workflow.
"""

from Utils.Utilities import makeList, strToBool
from WMCore.Lexicon import procstringT0
from WMCore.WMSpec.StdSpecs.DataProcessing import DataProcessing


class PromptRecoWorkloadFactory(DataProcessing):
    """
    _PromptRecoWorkloadFactory_

    Stamp out PromptReco workflows.
    """

    def __init__(self):
        super(PromptRecoWorkloadFactory, self).__init__()
        self.procJobSplitArgs = {}
        self.skimJobSplitArgs = {}
        self.inputProcessedDataset = None
        self.inputPrimaryDataset = None
        self.eventsPerJob = None
        self.inputDataTier = None

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
        workload.setDashboardActivity("t0")
        workload.setWorkQueueSplitPolicy("Block", self.procJobSplitAlgo,
                                         self.procJobSplitArgs)

        cmsswStepType = "CMSSW"
        taskType = "Processing"

        recoOutputs = []
        for dataTier in self.writeTiers:
            recoOutputs.append({'dataTier': dataTier,
                                'eventContent': dataTier,
                                'moduleLabel': "write_%s" % dataTier})

        recoTask = workload.newTask("Reco")

        scenarioArgs = {'globalTag': self.globalTag,
                        'skims': self.alcaSkims,
                        'PhysicsSkims': self.physicsSkims,
                        'dqmSeq': self.dqmSequences,
                        'outputs': recoOutputs}
        if self.globalTagConnect:
            scenarioArgs['globalTagConnect'] = self.globalTagConnect

        recoOutMods = self.setupProcessingTask(recoTask, taskType, self.inputDataset,
                                               scenarioName=self.procScenario,
                                               scenarioFunc="promptReco",
                                               scenarioArgs=scenarioArgs,
                                               splitAlgo=self.procJobSplitAlgo,
                                               splitArgs=self.procJobSplitArgs,
                                               stepType=cmsswStepType,
                                               forceUnmerged=[
                                                   "write_ALCARECO"] if 'ALCARECO' in self.writeTiers else False)
        if self.doLogCollect:
            self.addLogCollectTask(recoTask)

        recoMergeTasks = {}
        for recoOutLabel, recoOutInfo in recoOutMods.items():
            if recoOutInfo['dataTier'] != "ALCARECO":
                mergeTask = self.addMergeTask(recoTask, self.procJobSplitAlgo, recoOutLabel,
                                              doLogCollect=self.doLogCollect)
                recoMergeTasks[recoOutInfo['dataTier']] = mergeTask

            else:
                alcaTask = recoTask.addTask("AlcaSkim")
                alcaTaskConf = {'Multicore': 1, 'EventStreams': 0}
                scenarioArgs = {'globalTag': self.globalTag,
                                'skims': self.alcaSkims,
                                'primaryDataset': self.inputPrimaryDataset}
                if self.globalTagConnect:
                    scenarioArgs['globalTagConnect'] = self.globalTagConnect

                alcaOutMods = self.setupProcessingTask(alcaTask, taskType,
                                                       inputStep=recoTask.getStep("cmsRun1"),
                                                       inputModule=recoOutLabel,
                                                       scenarioName=self.procScenario,
                                                       scenarioFunc="alcaSkim",
                                                       scenarioArgs=scenarioArgs,
                                                       splitAlgo="ParentlessMergeBySize",
                                                       splitArgs={"max_merge_size": self.maxMergeSize,
                                                                  "min_merge_size": self.minMergeSize,
                                                                  "max_merge_events": self.maxMergeEvents},
                                                       stepType=cmsswStepType,
                                                       taskConf=alcaTaskConf)
                if self.doLogCollect:
                    self.addLogCollectTask(alcaTask, taskName="AlcaSkimLogCollect")
                self.addCleanupTask(recoTask, recoOutLabel, dataTier=recoOutInfo['dataTier'])

                for alcaOutLabel in alcaOutMods:
                    self.addMergeTask(alcaTask, self.procJobSplitAlgo, alcaOutLabel,
                                      doLogCollect=self.doLogCollect)

        # setting the parameters which need to be set for all the tasks
        # sets acquisitionEra, processingVersion, processingString
        workload.setTaskPropertiesFromWorkload()

        # set the LFN bases (normally done by request manager)
        # also pass runNumber (workload evaluates it)
        workload.setLFNBase(self.mergedLFNBase, self.unmergedLFNBase,
                            runNumber=self.runNumber)

        return workload

    def __call__(self, workloadName, arguments):
        """
        _call_

        Create a ReReco workload with the given parameters.
        """
        DataProcessing.__call__(self, workloadName, arguments)

        # These are mostly place holders because the job splitting algo and
        # parameters will be updated after the workflow has been created.
        self.procJobSplitArgs = {}
        if self.procJobSplitAlgo in ["EventBased", "EventAwareLumiBased"]:
            if self.eventsPerJob is None:
                self.eventsPerJob = int((8.0 * 3600.0) / self.timePerEvent)
            self.procJobSplitArgs["events_per_job"] = self.eventsPerJob
            if self.procJobSplitAlgo == "EventAwareLumiBased":
                self.procJobSplitArgs["job_time_limit"] = 96 * 3600  # 4 days in seconds
                self.procJobSplitArgs["allowCreationFailure"] = False
        elif self.procJobSplitAlgo == "LumiBased":
            self.procJobSplitArgs["lumis_per_job"] = self.lumisPerJob
        elif self.procJobSplitAlgo == "FileBased":
            self.procJobSplitArgs["files_per_job"] = self.filesPerJob

        self.skimJobSplitArgs = {}
        if self.skimJobSplitAlgo in ["EventBased", "EventAwareLumiBased"]:
            if self.eventsPerJob is None:
                self.eventsPerJob = int((8.0 * 3600.0) / self.timePerEvent)
            if self.skimJobSplitAlgo == "EventAwareLumiBased":
                self.skimJobSplitArgs["job_time_limit"] = 48 * 3600  # 2 days
                self.skimJobSplitArgs["allowCreationFailure"] = False
            self.skimJobSplitArgs["events_per_job"] = self.eventsPerJob
        elif self.skimJobSplitAlgo == "LumiBased":
            self.skimJobSplitArgs["lumis_per_job"] = self.lumisPerJob
        elif self.skimJobSplitAlgo == "FileBased":
            self.skimJobSplitArgs["files_per_job"] = self.filesPerJob
        self.skimJobSplitArgs = arguments.get("SkimJobSplitArgs",
                                              {"files_per_job": 1,
                                               "include_parents": True})

        return self.buildWorkload()

    @staticmethod
    def getWorkloadCreateArgs():
        baseArgs = DataProcessing.getWorkloadCreateArgs()
        specArgs = {"RequestType": {"default": "PromptReco", "optional": True},
                    "ConfigCacheID": {"optional": True, "null": True},
                    "Scenario": {"default": None, "optional": False,
                                 "attr": "procScenario", "null": False},
                    "ProcessingString": {"default": "", "validate": procstringT0},
                    "WriteTiers": {"default": ["RECO", "AOD", "DQM", "ALCARECO"],
                                   "type": makeList, "optional": False, "null": False},
                    "AlcaSkims": {"default": ["TkAlCosmics0T", "MuAlGlobalCosmics", "HcalCalHOCosmics"],
                                  "type": makeList, "optional": False, "null": False},
                    "PhysicsSkims": {"default": [], "type": makeList,
                                     "optional": True, "null": False},
                    "InitCommand": {"default": None, "optional": True, "null": True},
                    "EnvPath": {"default": None, "optional": True, "null": True},
                    "BinPath": {"default": None, "optional": True, "null": True},
                    "DoLogCollect": {"default": True, "type": strToBool,
                                     "optional": True, "null": False},
                    "SplittingAlgo": {"default": "EventAwareLumiBased", "null": False,
                                      "validate": lambda x: x in ["EventBased", "LumiBased",
                                                                  "EventAwareLumiBased", "FileBased"],
                                      "attr": "procJobSplitAlgo"},
                    "EventsPerJob": {"default": 500, "type": int, "validate": lambda x: x > 0,
                                     "null": False},
                    "SkimSplittingAlgo": {"default": "FileBased", "null": False,
                                          "validate": lambda x: x in ["EventBased", "LumiBased",
                                                                      "EventAwareLumiBased", "FileBased"],
                                          "attr": "skimJobSplitAlgo"},
                    "SkimEventsPerJob": {"default": 500, "type": int, "validate": lambda x: x > 0,
                                         "null": False},
                    "SkimLumisPerJob": {"default": 8, "type": int, "validate": lambda x: x > 0,
                                        "null": False},
                    "SkimFilesPerJob": {"default": 1, "type": int, "validate": lambda x: x > 0,
                                        "null": False},
                    }

        baseArgs.update(specArgs)
        DataProcessing.setDefaultArgumentsProperty(baseArgs)
        return baseArgs

    @staticmethod
    def getWorkloadAssignArgs():
        baseArgs = DataProcessing.getWorkloadAssignArgs()
        specArgs = {
            "Override": {"default": {"eos-lfn-prefix": "root://eoscms.cern.ch//eos/cms/store/logs/prod/recent/PromptReco"},
                         "type": dict},
            }
        baseArgs.update(specArgs)
        DataProcessing.setDefaultArgumentsProperty(baseArgs)
        return baseArgs
