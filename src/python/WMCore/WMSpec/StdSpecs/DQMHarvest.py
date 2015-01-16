from WMCore.Lexicon import dataset
from WMCore.WMSpec.StdSpecs.StdBase import StdBase

class DQMHarvestWorkloadFactory(StdBase):
    """
    _DQMHarvestWorkloadFactory_

    """

    def __call__(self, workloadName, arguments):
        """
        _call_

        Create a DQMHarvest workload with the given parameters.
        """
        StdBase.__call__(self, workloadName, arguments)

        (self.inputPrimaryDataset, self.inputProcessedDataset, self.inputDataTier) = self.inputDataset[1:].split("/")

        workload = self.createWorkload()
        
        workload.setLFNBase(self.mergedLFNBase, self.unmergedLFNBase)
        workload.setDashboardActivity("DQMHarvest")
        self.reportWorkflowToDashboard(workload.getDashboardActivity())

        mergeTask = workload.newTask("DQMHarvest")
        self.addDashboardMonitoring(mergeTask)
        mergeTaskCmssw = mergeTask.makeStep("cmsRun1")
        mergeTaskCmssw.setStepType("CMSSW")

        mergeTaskStageOut = mergeTaskCmssw.addStep("stageOut1")
        mergeTaskStageOut.setStepType("StageOut")
        
        mergeTaskLogArch = mergeTaskCmssw.addStep("logArch1")
        mergeTaskLogArch.setStepType("LogArchive")

        mergeTask.setSiteWhitelist(self.siteWhitelist)
        mergeTask.setSiteBlacklist(self.siteBlacklist)

        self.addLogCollectTask(mergeTask, taskName = "DQMHarvestMergeLogCollect")
        
        mergeTask.setTaskType("Merge")
        mergeTask.applyTemplates()
        
        mergeTask.addInputDataset(primary = self.inputPrimaryDataset,
                                  processed = self.inputProcessedDataset,
                                  tier = self.inputDataTier,
                                  dbsurl = self.dbsUrl,
                                  block_blacklist = self.blockBlacklist,
                                  block_whitelist = self.blockWhitelist,
                                  run_blacklist = self.runBlacklist,
                                  run_whitelist = self.runWhitelist)

        splitAlgo = "ParentlessMergeBySize"
        mergeTask.setSplittingAlgorithm(splitAlgo,
                                        max_merge_size = self.maxMergeSize,
                                        min_merge_size = self.minMergeSize,
                                        max_merge_events = self.maxMergeEvents)
        
        mergeTaskCmsswHelper = mergeTaskCmssw.getTypeHelper()
        mergeTaskCmsswHelper.cmsswSetup(self.frameworkVersion, softwareEnvironment = "",
                                        scramArch = self.scramArch)
        mergeTaskCmsswHelper.setGlobalTag(self.globalTag)
        mergeTaskCmsswHelper.setSkipBadFiles(True)
        mergeTaskCmsswHelper.setDataProcessingConfig("do_not_use", "merge")
        
        self.addOutputModule(mergeTask, "Merged",
                             primaryDataset = self.inputPrimaryDataset,
                             dataTier = self.dataTier,
                             filterName = None,
                             forceMerged = True)

        # setting the parameters which need to be set for all the tasks
        # sets acquisitionEra, processingVersion, processingString
        workload.setTaskPropertiesFromWorkload()
        
        return workload

    @staticmethod
    def getWorkloadArguments():
        baseArgs = StdBase.getWorkloadArguments()
        reqMgrArgs = StdBase.getWorkloadArgumentsWithReqMgr()
        baseArgs.update(reqMgrArgs)
        specArgs = {"RequestType" : {"default" : "DQMHarvest"},
                    "InputDataset" : {"default" : None, "optional" : False,
                                      "validate" : dataset}
                    }
        baseArgs.update(specArgs)
        StdBase.setDefaultArgumentsProperty(baseArgs)
        return baseArgs
