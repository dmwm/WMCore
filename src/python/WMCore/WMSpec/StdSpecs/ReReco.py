#!/usr/bin/env python
# encoding: utf-8
"""
ReReco.py

Created by Dave Evans on 2010-02-10.
Copyright (c) 2010 Fermilab. All rights reserved.


Standard Workload creator for ReReco processing

"""

from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.WMStep import makeWMStep
from WMCore.WMSpec.Steps.StepFactory import getStepTypeHelper

parseDataset = lambda x : { "Primary" : x.split("/")[1], 
                            "Processed": x.split("/")[2],
                            "Tier" : x.split("/")[3]}

def outputModule(key):
    d =  {"RECO": "outputRECORECO",
          "AOD":"outputAODRECO",
          "ALCARECO": "outputALCARECOALCARECO"}

    return d.get(key, None)

class ReRecoWorkloadFactory():
    """
    _rerecoWorkload_
    
    Standard way of building a ReReco workload.
    
    arguments is a dictionary containing the same set of keys as RequestSchema for a ReReco request
    
    """
    def addOutputModule(self, parentTask, dataTier):
        """
        _addOutputModule_

        Add an output module and merge task for files produced by the parent
        task.
        """
        lfnBase = "%s/%s/%s" % (self.unmergedLfnBase, dataTier, self.processedDatasetName)
        cmsswStep = parentTask.getStep("cmsRun1")
        cmsswStepHelper = cmsswStep.getTypeHelper()
        cmsswStepHelper.addOutputModule(outputModule(dataTier),
                                        primaryDataset = self.inputPrimaryDataset,
                                        processedDataset = self.unmergedDatasetName,
                                        dataTier = dataTier,
                                        lfnBase = lfnBase)
        self.addMergeTask(parentTask, dataTier)
        return

    def addMergeTask(self, parentTask, dataTier):
        """
        _addMergeTask_

        Create a merge task for files produced by the parent task.
        """
        mergeTask = parentTask.addTask("Merge%s" % dataTier.capitalize())
        mergeTaskCmssw = mergeTask.makeStep("cmsRun1")
        mergeTaskCmssw.setStepType("CMSSW")
        
        mergeTaskStageOut = mergeTaskCmssw.addStep("stageOut1")
        mergeTaskStageOut.setStepType("StageOut")
        mergeTaskLogArch = mergeTaskCmssw.addStep("logArch1")
        mergeTaskLogArch.setStepType("LogArchive")
        mergeTask.addGenerator("BasicNaming")
        mergeTask.addGenerator("BasicCounter")
        mergeTask.setTaskType("Merge")  
        mergeTask.applyTemplates()
        mergeTask.setSplittingAlgorithm("WMBSMergeBySize",
                                        max_merge_size = 4294967296,
                                        min_merge_size = 500000000)

        mergeTaskCmsswHelper = mergeTaskCmssw.getTypeHelper()
        mergeTaskCmsswHelper.cmsswSetup(self.cmsswVersion,
                                        softwareEnvironment = self.softwareInitCommand,
                                        scramArch = self.scramArchitecture)

        mergeTaskCmsswHelper.setDataProcessingConfig(self.scenario, "merge")
        lfnBase = "%s/%s/%s" % (self.commonLfnBase, dataTier, self.processedDatasetName)
        mergeTaskCmsswHelper.addOutputModule("Merged",
                                             primaryDataset = self.inputPrimaryDataset,
                                             processedDataset = self.processedDatasetName,
                                             dataTier = dataTier,
                                             lfnBase = lfnBase)
        parentTaskCmssw = parentTask.getStep("cmsRun1")
        mergeTask.setInputReference(parentTaskCmssw, outputModule = outputModule(dataTier))

        if self.emulationMode:
            mergeTaskStageOutHelper = mergeTaskStageOut.getTypeHelper()
            mergeTaskLogArchHelper  = mergeTaskLogArch.getTypeHelper()
            mergeTaskCmsswHelper.data.emulator.emulatorName = "CMSSW"
            mergeTaskStageOutHelper.data.emulator.emulatorName = "StageOut"
            mergeTaskLogArchHelper.data.emulator.emulatorName = "LogArchive"

        self.addCleanupTask(mergeTask, dataTier)
        return

    def addCleanupTask(self, parentTask, dataTier):
        """
        _addCleanupTask_

        Create a cleanup task to delete files produces by the parent task.
        """
        cleanupTask = parentTask.addTask("CleanupUnmerged%s" % dataTier.capitalize())
        cleanupTask.setTaskType("Cleanup")

        parentTaskCmssw = parentTask.getStep("cmsRun1")
        cleanupTask.setInputReference(parentTaskCmssw, outputModule = outputModule(dataTier))
        cleanupTask.setSplittingAlgorithm("SiblingProcessingBased", files_per_job = 50)
       
        cleanupStep = cleanupTask.makeStep("cleanupUnmerged%s" % dataTier.capitalize())
        cleanupStep.setStepType("DeleteFiles")
        cleanupTask.applyTemplates()
        return

    def addLogCollectTask(self, parentTask):
        """
        _addLogCollecTask_

        Create a LogCollect task for log archives that are produced by the
        parent task.
        """
        logCollectTask = parentTask.addTask("LogCollect")
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

    def __call__(self, workloadName, arguments):
        """
        _call_

        Create a ReReco workload with the given parameters.
        """
        #  //
        # // Processing controls
        #//  TODO: Address defaults etc/exception handling
        writeDataTiers = arguments.get("OutputTiers", ['RECO', 'ALCARECO'])
        owner = arguments.get("Owner", "DataOps")
        acquisitionEra = arguments.get("AcquisitionEra", "Teatime09")
        globalTagSetting = arguments.get("GlobalTag","GR09_P_V7::All")
        lfnCategory = arguments.get("LFNCategory","/store/data")
        processingVersion = arguments.get("ProcessingVersion", "v99")
        self.scenario = arguments.get("Scenario", "cosmics")
        self.cmsswVersion = arguments.get("CMSSWVersion", "CMSSW_3_3_5_patch3")
        self.scramArchitecture = arguments.get("ScramArch", "slc5_ia32_gcc434")
    
        #  //
        # // Input Data selection
        #//
        datasetElements = parseDataset(arguments['InputDatasets'])
        self.inputPrimaryDataset = datasetElements['Primary']
        inputProcessedDataset = datasetElements['Processed']
        inputDataTier = datasetElements['Tier']

        siteWhitelist = arguments.get("SiteWhitelist", [])
        siteBlacklist = arguments.get("SiteBlacklist", [])
        blockBlacklist = arguments.get("BlockBlacklist", [])
        blockWhitelist = arguments.get("BlockWhitelist", [])    
        runWhitelist = arguments.get("RunWhitelist", [])
        runBlacklist = arguments.get("RunBlacklist", [])    
    
        #  //
        # // Enabling Emulation from the Request allows some nice diagnostic tests
        #//
        self.emulationMode = arguments.get("Emulate", False)
    
        #  //
        # // likely to be ~stable
        #//
        dbsUrl = arguments.get("DBSURL","http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet" )
        self.softwareInitCommand = arguments.get("SoftwareInitCommand", " . /uscmst1/prod/sw/cms/shrc prod")
        tempLfnCategory =  arguments.get("TemporaryLFNCategory", "/store/unmerged")
    
        #  //
        # // Set up the basic workload task and step structure
        #//
        workload = newWorkload(workloadName)
        workload.setOwner(owner)
        workload.setStartPolicy('DatasetBlock')
        workload.setEndPolicy('SingleShot')
        workload.data.properties.acquisitionEra = acquisitionEra

        #  //
        # // set up the production task
        #//
        rerecoTask = workload.newTask("ReReco")
        rerecoTaskCmssw = rerecoTask.makeStep("cmsRun1")
        rerecoTaskCmssw.setStepType("CMSSW")
        rerecoTaskStageOut = rerecoTaskCmssw.addStep("stageOut1")
        rerecoTaskStageOut.setStepType("StageOut")
        rerecoTaskLogArch = rerecoTaskCmssw.addStep("logArch1")
        rerecoTaskLogArch.setStepType("LogArchive")
        rerecoTask.applyTemplates()
        rerecoTask.setSplittingAlgorithm("FileBased", files_per_job = 1)
        rerecoTask.addGenerator("BasicNaming")
        rerecoTask.addGenerator("BasicCounter")
        rerecoTask.setTaskType("Processing")
        
        #  //
        # // rereco cmssw step
        #//
        #
        # TODO: Anywhere helper.data is accessed means we need a method added to the
        # type based helper class to provide a clear API.
        rerecoTaskCmsswHelper = rerecoTaskCmssw.getTypeHelper()
        rerecoTask.addInputDataset(
            primary = self.inputPrimaryDataset,
            processed = inputProcessedDataset,
            tier = inputDataTier,
            dbsurl = dbsUrl,
            block_blacklist = blockBlacklist,
            block_whitelist = blockWhitelist,
            run_blacklist = runBlacklist,
            run_whitelist = runWhitelist
            )

        rerecoTask.data.constraints.sites.whitelist = siteWhitelist
        rerecoTask.data.constraints.sites.blacklist = siteBlacklist
        
        rerecoTaskCmsswHelper.cmsswSetup(
            self.cmsswVersion,
            softwareEnvironment = self.softwareInitCommand ,
            scramArch = self.scramArchitecture
            )
        
        rerecoTaskCmsswHelper.setDataProcessingConfig(
            self.scenario, "promptReco", globalTag = globalTagSetting,
            writeTiers = writeDataTiers)
        
        self.processedDatasetName = "rereco_%s_%s" % (globalTagSetting.replace("::","_"), processingVersion)
        self.unmergedDatasetName = "%s" % self.processedDatasetName
        self.commonLfnBase = lfnCategory
        self.commonLfnBase += "/%s" % acquisitionEra
        self.commonLfnBase += "/%s" % self.inputPrimaryDataset
        self.unmergedLfnBase = tempLfnCategory
        self.unmergedLfnBase += "/%s" % acquisitionEra
        self.unmergedLfnBase += "/%s" % self.inputPrimaryDataset
        
        if self.emulationMode:
            rerecoStageOutHelper = rerecoTaskStageOut.getTypeHelper()
            rerecoLogArchHelper  = rerecoTaskLogArch.getTypeHelper()
            rerecoCmsswHelper.data.emulator.emulatorName = "CMSSW"
            rerecoStageOutHelper.data.emulator.emulatorName = "StageOut"
            rerecoLogArchHelper.data.emulator.emulatorName = "LogArchive"
            
        self.addLogCollectTask(rerecoTask)        

        for dataTierName in writeDataTiers:
            self.addOutputModule(rerecoTask, dataTierName)
            
        return workload

def rerecoWorkload(workloadName, arguments):
    """
    _rerecoWorkload_

    Instantiate the ReRecoWorkflowFactory and have it generate a workload for
    the given parameters.
    """
    myReRecoFactory = ReRecoWorkloadFactory()
    return myReRecoFactory(workloadName, arguments)
