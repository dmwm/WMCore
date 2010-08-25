#!/usr/bin/env python
"""
_TestSpec_

Test spec with known output modules used for testing.
"""




from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.WMStep import makeWMStep
from WMCore.WMSpec.Steps.StepFactory import getStepTypeHelper
from WMCore.Services.Requests import JSONRequests

class TestWorkloadFactory(object):
    """
    _TestWorkloadFactory_

    Stamp out test workfloads.
    """
    def createWorkload(self):
        """
        _createWorkload_

        Create a new workload.
        """
        workload = newWorkload("TestWorkload")
        workload.setOwner("sfoulkes@fnal.gov")
        workload.setStartPolicy("DatasetBlock", SliceType = "NumberOfFiles", SliceSize = 1)
        workload.setEndPolicy("SingleShot")
        workload.data.properties.acquisitionEra = "WMAgentCommissioning10"
        return workload

    def setupProcessingTask(self, procTask):
        """
        _setupProcessingTask_

        Given an empty task add cmsRun, stagOut and logArch steps.
        """
        procTaskCmssw = procTask.makeStep("cmsRun1")
        procTaskCmssw.setStepType("CMSSW")
        procTaskStageOut = procTaskCmssw.addStep("stageOut1")
        procTaskStageOut.setStepType("StageOut")
        procTaskLogArch = procTaskCmssw.addStep("logArch1")
        procTaskLogArch.setStepType("LogArchive")
        procTask.applyTemplates()
        splitArgs = {"files_per_job": 1}
        procTask.setSplittingAlgorithm("FileBased", **splitArgs)
        procTask.addGenerator("BasicNaming")
        procTask.addGenerator("BasicCounter")
        procTask.setTaskType("Processing")

        procTask.addInputDataset(primary = "MinimumBias",
                                 processed = "Comissioning10-v4",
                                 tier = "RAW", dbsurl = "dbsbds",
                                 block_blacklist = [],
                                 block_whitelist = [],
                                 run_blacklist = [],
                                 run_whitelist = [])
        procTask.data.constraints.sites.whitelist = []
        procTask.data.constraints.sites.blacklist = []

        procTaskCmsswHelper = procTaskCmssw.getTypeHelper()
        procTaskCmsswHelper.setGlobalTag("TestGlobalTag::All")
        procTaskCmsswHelper.setMinMergeSize(1)
        procTaskCmsswHelper.cmsswSetup("CMSSW_3_5_8_patch3", softwareEnvironment = "",
                                       scramArch = "slc5_amd64_gcc434")
        
        procTaskCmsswHelper.setDataProcessingConfig("cosmics", "PromptReco")

        if self.emulation:
            procTaskStageOutHelper = procTaskStageOut.getTypeHelper()
            procTaskLogArchHelper  = procTaskLogArch.getTypeHelper()
            procTaskCmsswHelper.data.emulator.emulatorName = "CMSSW"
            procTaskStageOutHelper.data.emulator.emulatorName = "StageOut"
            procTaskLogArchHelper.data.emulator.emulatorName = "LogArchive"
            
        return procTask

    def addLogCollectTask(self, parentTask, taskName = "LogCollect"):
        """
        _addLogCollecTask_
        
        Create a LogCollect task for log archives that are produced by the
        parent task.
        """
        logCollectTask = parentTask.addTask(taskName)
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

    def addOutputModule(self, parentTask, outputModuleName, dataTier,
                        filterName):
        """
        _addOutputModule_
        
        Add an output module to the geven processing task.  This will also
        create merge and cleanup tasks for the output of the output module.
        A handle to the merge task is returned to make it easy to use the merged
        output of the output module as input to another task.
        """
        if filterName != None and filterName != "":
            processedDatasetName = "%s-%s-%s" % ("WMAgentCommissioning10", filterName,
                                                 "v1")
        else:
            processedDatasetName = "WMAgentCommissioning10-v1"
        
        unmergedLFN = "%s/%s/%s" % ("/store/temp/WMAgent/unmerged", dataTier,
                                    processedDatasetName)
        mergedLFN = "%s/%s/%s" % ("/store/temp/WMAgent/merged", dataTier,
                                  processedDatasetName)
        cmsswStep = parentTask.getStep("cmsRun1")
        cmsswStepHelper = cmsswStep.getTypeHelper()
        cmsswStepHelper.addOutputModule(outputModuleName,
                                        primaryDataset = "MinimumBias",
                                        processedDataset = "Commissioning10-v4",
                                        dataTier = "RAW",
                                        lfnBase = "/store/temp/WMAgent/unmerged",
                                        mergedLFNBase = "/store/temp/WMAgent/merged")
        return

    def __call__(self, emulation = False):
        """
        _call_

        Create a test workload.
        """
        self.emulation = emulation
        
        workload = self.createWorkload()
        procTask = workload.newTask("ReReco")

        self.setupProcessingTask(procTask)
        self.addLogCollectTask(procTask)

        self.addOutputModule(procTask, "TestOutputModule", "RECO", "SomeFilter")
        return workload

def testWorkload(emulation = False):
    """
    _testWorkflow_

    Instantiate the TestWorkloadFactory and create a workload.
    """
    myTestWorkloadFactory = TestWorkloadFactory()
    return myTestWorkloadFactory(emulation = emulation)

if __name__ == "__main__":
    testWorkload()
    
