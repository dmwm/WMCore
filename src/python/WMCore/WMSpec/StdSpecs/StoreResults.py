#!/usr/bin/env python
# pylint: disable-msg=W0201, W0142, W0102
# W0201: Steve defines all global vars in __call__
#   I don't know why, but I'm not getting blamed for it
# W0142: Dave loves the ** magic
# W0102: Dangerous default values?  I live on danger!
#   Allows us to use a dict as a default
"""
_StoreResults_

Standard StoreResults workflow.
"""

import os

from WMCore.Lexicon import dataset, block
from WMCore.WMSpec.StdSpecs.StdBase import StdBase
from WMCore.WMSpec.WMWorkloadTools import makeList

class StoreResultsWorkloadFactory(StdBase):
    """
    _StoreResultsWorkloadFactory_

    Stamp out StoreResults workloads.
    """

    def __call__(self, workloadName, arguments):
        """
        _call_

        Create a StoreResults workload with the given parameters.
        """
        StdBase.__call__(self, workloadName, arguments)

        (self.inputPrimaryDataset, self.inputProcessedDataset, self.inputDataTier) = \
                                   self.inputDataset[1:].split("/")

        processedDatasetName = "%s-%s" % (self.acquisitionEra,
                                          self.processingVersion)

        workload = self.createWorkload()
        workload.setDashboardActivity("StoreResults")
        self.reportWorkflowToDashboard(workload.getDashboardActivity())

        mergeTask = workload.newTask("StoreResults")
        self.addDashboardMonitoring(mergeTask)
        mergeTaskCmssw = mergeTask.makeStep("cmsRun1")

        mergeTaskCmssw.setStepType("CMSSW")

        mergeTaskStageOut = mergeTaskCmssw.addStep("stageOut1")
        mergeTaskStageOut.setStepType("StageOut")
        mergeTaskLogArch = mergeTaskCmssw.addStep("logArch1")
        mergeTaskLogArch.setStepType("LogArchive")
        self.addLogCollectTask(mergeTask,
                               taskName = "StoreResultsLogCollect")
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
                                        max_merge_events = self.maxMergeEvents,
                                        siteWhitelist = self.siteWhitelist,
                                        siteBlacklist = self.siteBlacklist)

        mergeTaskCmsswHelper = mergeTaskCmssw.getTypeHelper()
        mergeTaskCmsswHelper.cmsswSetup(self.frameworkVersion,
                                        softwareEnvironment = "",
                                        scramArch = self.scramArch)
        mergeTaskCmsswHelper.setDataProcessingConfig("cosmics", "merge")

        mergedLFN = "%s/%s/%s/%s/%s" % (self.mergedLFNBase, self.acquisitionEra,
                                        self.inputPrimaryDataset, self.dataTier,
                                        self.processingVersion)

        mergeTaskCmsswHelper.addOutputModule("Merged",
                                             primaryDataset = self.inputPrimaryDataset,
                                             processedDataset = processedDatasetName,
                                             dataTier = self.dataTier,
                                             lfnBase = mergedLFN)

        return workload

    @staticmethod
    def getWorkloadArguments():
        baseArgs = StdBase.getWorkloadArguments()
        specArgs = {"InputDataset" : {"default" : "/MinimumBias/Run2010A-Dec22ReReco_v1/USER",
                                      "type" : str, "optional" : False,
                                      "validate" : dataset, "attr" : "inputDataset",
                                      "null" : False},
                    "GlobalTag" : {"default" : "GT_SR_V1:All", "type" : str,
                                   "optional" : False, "validate" : None,
                                   "attr" : "globalTag", "null" : False},
                    "CmsPath" : {"default" : "/tmp", "type" : str,
                                 "optional" : False, "validate" : None,
                                 "attr" : "cmsPath", "null" : False},
                    "DataTier" : {"default" : "USER", "type" : str,
                                  "optional" : True, "validate" : None,
                                  "attr" : "dataTier", "null" : False},
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
                                      "attr" : "runWhitelist", "null" : False}}
        baseArgs.update(specArgs)
        return baseArgs
