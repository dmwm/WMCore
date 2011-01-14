#!/usr/bin/env python
#pylint: disable-msg=W0201, W0142, W0102
# W0201: Steve defines all global vars in __call__
#   I don't know why, but I'm not getting blamed for it
# W0142: Dave loves the ** magic
# W0102: Dangerous default values?  I live on danger!
#   Allows us to use a dict as a default
"""
_StoreResults_

Standard StoreResults workflow.
"""

import time

from WMCore.WMSpec.StdSpecs.StdBase import StdBase
from WMCore.WMSpec.WMWorkload import newWorkload

def getTestArguments():
    """
    _getTestArguments_

    This should be where the default REQUIRED arguments go
    This serves as documentation for what is currently required
    by the standard StoreResults workload in importable format.

    NOTE: These are test values.  If used in real workflows they
    will cause everything to crash/die/break, and we will be forced
    to hunt you down and kill you.
    """

    arguments = {
        "CmsPath": "/uscmst1/prod/sw/cms",
        "AcquisitionEra": "Blennies",
        "Requestor": "ewv@fnal.gov",
        "InputDataset": "/MinimumBias/Run2010A-PromptReco-v2/RECO",
        "CMSSWVersion": "CMSSW_3_6_1_patch6",
        "ScramArch": "slc5_ia32_gcc434",
        "ProcessingVersion": "v2scf",
        "SkimInput": "output",
        "GlobalTag": "GR10_P_v4::All",
        "CouchURL": "http://dmwmwriter:gutslap!@cmssrv52.fnal.gov:5984",
        "CouchDBName": "wmagent_config_cache",
        "Scenario": ""
        }

    return arguments

class StoreResultsWorkloadFactory(StdBase):
    """
    _StoreResultsWorkloadFactory_

    Stamp out StoreResults workfloads.
    """
    def __init__(self):
        StdBase.__init__(self)
        return

    def createWorkload(self):
        """
        _createWorkload_

        Create a new workload.
        """

        workload = StdBase.createWorkload(self)

        workload.data.properties.acquisitionEra = self.acquisitionEra
        return workload

    def __call__(self, workloadName, arguments):
        """
        _call_

        Create a StoreResults workload with the given parameters.
        """
        StdBase.__call__(self, workloadName, arguments)

        # Required parameters.
        self.inputDataset = arguments["InputDataset"]
        self.frameworkVersion = arguments["CMSSWVersion"]
        self.skimInput = arguments["SkimInput"]
        self.globalTag = arguments["GlobalTag"]
        self.cmsPath = arguments["CmsPath"]

        # Required parameters that can be empty.
        self.scenario = arguments["Scenario"]

        # Optional arguments.
        self.dbsUrl = arguments.get("DbsUrl", "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet")
        self.blockBlackList = arguments.get("BlockBlackList", [])
        self.blockWhiteList = arguments.get("BlockWhiteList", [])
        self.runBlackList = arguments.get("RunBlackList", [])
        self.runWhiteList = arguments.get("RunWhiteList", [])
        self.emulation = arguments.get("Emulation", False)
        self.stdJobSplitAlgo  = arguments.get("StdJobSplitAlgo", 'FileBased')
        self.stdJobSplitArgs  = arguments.get("StdJobSplitArgs", {'files_per_job': 1})
        self.skimJobSplitAlgo = arguments.get("SkimJobSplitAlgo", 'TwoFileBased')
        self.skimJobSplitArgs = arguments.get("SkimJobSplitArgs", {'files_per_job': 1})
        self.dataTier         = arguments.get("DataTier", 'USER')
        dataTier = self.dataTier

        (self.inputPrimaryDataset, self.inputProcessedDataset, self.inputDataTier) = \
                                   self.inputDataset[1:].split("/")

        processedDatasetName = "%s-%s" % (self.acquisitionEra, self.processingVersion)

        workload = self.createWorkload()
        mergeTask = workload.newTask("StoreResults")

        self.addDashboardMonitoring(mergeTask)
        mergeTaskCmssw = mergeTask.makeStep("cmsRun1")

        mergeTaskCmssw.setStepType("CMSSW")

        mergeTaskStageOut = mergeTaskCmssw.addStep("stageOut1")
        mergeTaskStageOut.setStepType("StageOut")
        mergeTaskLogArch = mergeTaskCmssw.addStep("logArch1")
        mergeTaskLogArch.setStepType("LogArchive")
        self.addLogCollectTask(mergeTask, taskName = "StoreResultsLogCollect")
        mergeTask.addGenerator("BasicNaming")
        mergeTask.addGenerator("BasicCounter")
        mergeTask.setTaskType("Merge")
        mergeTask.applyTemplates()
        mergeTask.addInputDataset(primary = self.inputPrimaryDataset, processed = self.inputProcessedDataset,
                                     tier = self.inputDataTier, dbsurl = self.dbsUrl,
                                     block_blacklist = self.blockBlackList,
                                     block_whitelist = self.blockWhiteList,
                                     run_blacklist = self.runBlackList,
                                     run_whitelist = self.runWhiteList)
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

        mergedLFN = "%s/%s/%s/%s/%s" % (self.mergedLFNBase, self.acquisitionEra,
                                        self.inputPrimaryDataset, dataTier,
                                        self.processingVersion)

        mergeTaskCmsswHelper.addOutputModule("Merged",
                                             primaryDataset = self.inputPrimaryDataset,
                                             processedDataset = processedDatasetName,
                                             dataTier = dataTier,
                                             lfnBase = mergedLFN)

        return workload


def storeResultsWorkload(workloadName, arguments):
    """
    _storeResultsWorkload_

    Instantiate the StoreResultsWorkflowFactory and have it generate a workload for
    the given parameters.
    """
    myStoreResultsFactory = StoreResultsWorkloadFactory()
    return myStoreResultsFactory(workloadName, arguments)
