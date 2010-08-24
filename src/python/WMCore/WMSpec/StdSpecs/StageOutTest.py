"""

Workflow to stage a file back and forth to the SE

"""






import subprocess

from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.WMStep import makeWMStep
from WMCore.WMSpec.Steps.StepFactory import getStepTypeHelper
from WMCore.Services.Requests import JSONRequests

from WMCore.Cache.ConfigCache import WMConfigCache
from WMCore.WMSpec.StdSpecs import SplitAlgoStartPolicyMap
from WMCore.WMSpec.StdSpecs.StdBase import StdBase


class StageOutTestWorkloadFactory(StdBase):
    """
    _ReRecoWorkloadFactory_

    Stamp out ReReco workflows.
    """



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
        mergeTask = parentTask.addTask("Merge%s" % parentOutputModule)
        #self.addDashboardMonitoring(mergeTask)
        mergeTaskCmssw = mergeTask.makeStep("cmsRun1")
        mergeTaskCmssw.setStepType("CMSSW")
        
        mergeTaskStageOut = mergeTaskCmssw.addStep("stageOut1")
        mergeTaskStageOut.setStepType("StageOut")
        mergeTaskLogArch = mergeTaskCmssw.addStep("logArch1")
        mergeTaskLogArch.setStepType("LogArchive")

        mergeTask.setTaskLogBaseLFN(self.unmergedLFNBase)        
        self.addLogCollectTask(mergeTask, taskName = "%sMergeLogCollect" % parentOutputModule)
        
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
        cleanupTask = parentTask.addTask("CleanupUnmerged%s" % parentOutputModuleName)
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

        Create a StageOutTest workload with the given parameters.
        """

        self.workloadName = workloadName

        workload = newWorkload(self.workloadName)
        workload.setOwner('meloam@fnal.gov')

        stageOutTask = workload.newTask("StageOut")
        stageOutStepList = []
        stageInStepList  = []
        counter = 1
        
        # do the staging out
        for pfn in arguments.targetPFNs:
            procTaskStageOut = stageOutTask.addStep("stageOut%s" % counter)
            procTaskStageOut.setStepType("StageOut")
            stageOutStepList.extend(procTaskStageOut)
        
        # do the staging in
        counter = 1
        for pfn in arguments.targetPFNs:
            procTaskStageOut = stageOutTask.addStep("stageIn%s" % counter)
            procTaskStageOut.setStepType("StageOut")
            stageOutStepList.extend(procTaskStageOut)
                    
        procTaskLogArch = stageOutTask.addStep("logArch1")
        procTaskLogArch.setStepType("LogArchive")
        
        stageOutTask.applyTemplates()
            
        return workload

def stageOutTestWorkload(workloadName, arguments):
    """
    _rerecoWorkload_

    Instantiate the ReRecoWorkflowFactory and have it generate a workload for
    the given parameters.
    """
    myReRecoFactory = StageOutTestWorkloadFactory()
    return myReRecoFactory(workloadName, arguments)
