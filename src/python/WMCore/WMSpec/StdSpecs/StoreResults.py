#!/usr/bin/env python
# pylint: disable=W0201, W0102
# W0201: Steve defines all global vars in __call__
#   I don't know why, but I'm not getting blamed for it
# W0102: Dangerous default values?  I live on danger!
#   Allows us to use a dict as a default
"""
_StoreResults_

Standard StoreResults workflow.
"""

from Utils.Utilities import makeList, makeNonEmptyList
from WMCore.Lexicon import dataset, block, physicsgroup, cmsname
from WMCore.WMSpec.StdSpecs.StdBase import StdBase


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
        # first of all, we update the merged LFN based on the physics group
        arguments['MergedLFNBase'] += "/" + arguments['PhysicsGroup'].lower()
        StdBase.__call__(self, workloadName, arguments)

        (inputPrimaryDataset, inputProcessedDataset, inputDataTier) = self.inputDataset[1:].split("/")

        workload = self.createWorkload()

        mergeTask = workload.newTask("StoreResults")
        self.addRuntimeMonitors(mergeTask)
        mergeTaskCmssw = mergeTask.makeStep("cmsRun1")
        mergeTaskCmssw.setStepType("CMSSW")

        mergeTaskStageOut = mergeTaskCmssw.addStep("stageOut1")
        mergeTaskStageOut.setStepType("StageOut")

        mergeTaskLogArch = mergeTaskCmssw.addStep("logArch1")
        mergeTaskLogArch.setStepType("LogArchive")

        self.addLogCollectTask(mergeTask, taskName="StoreResultsLogCollect")

        mergeTask.setTaskType("Merge")
        mergeTask.applyTemplates()

        mergeTask.addInputDataset(name=self.inputDataset,
                                  primary=inputPrimaryDataset,
                                  processed=inputProcessedDataset,
                                  tier=inputDataTier,
                                  dbsurl=self.dbsUrl,
                                  block_blacklist=self.blockBlacklist,
                                  block_whitelist=self.blockWhitelist,
                                  run_blacklist=self.runBlacklist,
                                  run_whitelist=self.runWhitelist)

        splitAlgo = "ParentlessMergeBySize"
        mergeTask.setSplittingAlgorithm(splitAlgo,
                                        max_merge_size=self.maxMergeSize,
                                        min_merge_size=self.minMergeSize,
                                        max_merge_events=self.maxMergeEvents)

        mergeTaskCmsswHelper = mergeTaskCmssw.getTypeHelper()
        mergeTaskCmsswHelper.cmsswSetup(self.frameworkVersion, softwareEnvironment="",
                                        scramArch=self.scramArch)
        mergeTaskCmsswHelper.setGlobalTag(self.globalTag)
        mergeTaskCmsswHelper.setSkipBadFiles(True)
        mergeTaskCmsswHelper.setDataProcessingConfig("do_not_use", "merge")

        self.addOutputModule(mergeTask, "Merged",
                             primaryDataset=inputPrimaryDataset,
                             dataTier=self.dataTier,
                             filterName=None,
                             forceMerged=True)

        workload.setLFNBase(self.mergedLFNBase, self.unmergedLFNBase)
        workload.setDashboardActivity("StoreResults")

        # setting the parameters which need to be set for all the tasks
        # sets acquisitionEra, processingVersion, processingString
        workload.setTaskPropertiesFromWorkload()

        return workload

    @staticmethod
    def getWorkloadCreateArgs():
        baseArgs = StdBase.getWorkloadCreateArgs()
        specArgs = {"RequestType": {"default": "StoreResults", "optional": False},
                    "InputDataset": {"optional": False, "validate": dataset, "null": False},
                    "ConfigCacheID": {"optional": True, "null": True},
                    "DataTier": {"default": "USER", "type": str,
                                 "optional": True, "validate": None,
                                 "attr": "dataTier", "null": False},
                    "PhysicsGroup": {"default": "", "optional": False,
                                     "null": False, "validate": physicsgroup},
                    "MergedLFNBase": {"default": "/store/results", "type": str,
                                      "optional": True, "validate": None,
                                      "attr": "mergedLFNBase", "null": False},
                    # site whitelist shouldn't be allowed, but let's make an exception for StoreResults
                    "SiteWhitelist": {"default": [], "type": makeNonEmptyList, "assign_optional": False,
                                      "validate": lambda x: all([cmsname(y) for y in x])},
                    "BlockBlacklist": {"default": [], "type": makeList,
                                       "optional": True, "validate": lambda x: all([block(y) for y in x]),
                                       "attr": "blockBlacklist", "null": False},
                    "BlockWhitelist": {"default": [], "type": makeList,
                                       "optional": True, "validate": lambda x: all([block(y) for y in x]),
                                       "attr": "blockWhitelist", "null": False},
                    "RunBlacklist": {"default": [], "type": makeList,
                                     "optional": True, "validate": lambda x: all([int(y) > 0 for y in x]),
                                     "attr": "runBlacklist", "null": False},
                    "RunWhitelist": {"default": [], "type": makeList,
                                     "optional": True, "validate": lambda x: all([int(y) > 0 for y in x]),
                                     "attr": "runWhitelist", "null": False}}
        baseArgs.update(specArgs)
        StdBase.setDefaultArgumentsProperty(baseArgs)
        return baseArgs
