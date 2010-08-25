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
                        
                            



def rerecoWorkload(workloadName, arguments):
    """
    _rerecoWorkload_
    
    Standard way of building a ReReco workload.
    
    arguments is a dictionary containing the same set of keys as RequestSchema for a ReReco request
    
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
    scenario = arguments.get("Scenario", "cosmics")
    cmsswVersion = arguments.get("CMSSWVersion", "CMSSW_3_3_5_patch3")
    scramArchitecture = arguments.get("ScramArch", "slc5_ia32_gcc434")
    
    
    #  //
    # // Input Data selection
    #//
    datasetElements = parseDataset(arguments['InputDatasets'])
    inputPrimaryDataset = datasetElements['Primary']
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
    emulationMode = arguments.get("Emulate", False)
    
    #  //
    # // likely to be ~stable
    #//
    dbsUrl = arguments.get("DBSURL","http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet" )
    softwareInitCommand = arguments.get("SoftwareInitCommand", " . /uscmst1/prod/sw/cms/shrc prod")
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
    rereco = workload.newTask("ReReco")
    rerecoCmssw = rereco.makeStep("cmsRun1")
    rerecoCmssw.setStepType("CMSSW")
    rerecoStageOut = rerecoCmssw.addStep("stageOut1")
    rerecoStageOut.setStepType("StageOut")
    rerecoLogArch = rerecoCmssw.addStep("logArch1")
    rerecoLogArch.setStepType("LogArchive")
    rereco.applyTemplates()
    rereco.setSplittingAlgorithm("FileBased", files_per_job = 1)
    rereco.addGenerator("BasicNaming")
    rereco.addGenerator("BasicCounter")
    rereco.setTaskType("Processing")



    #  //
    # // rereco cmssw step
    #//
    #
    # TODO: Anywhere helper.data is accessed means we need a method added to the
    # type based helper class to provide a clear API.
    rerecoCmsswHelper = rerecoCmssw.getTypeHelper()
    rereco.addInputDataset(
        primary = inputPrimaryDataset,
        processed = inputProcessedDataset,
        tier = inputDataTier,
        dbsurl = dbsUrl,
        block_blacklist = blockBlacklist,
        block_whitelist = blockWhitelist,
        run_blacklist = runBlacklist,
        run_whitelist = runWhitelist
        )
    rereco.data.constraints.sites.whitelist = siteWhitelist
    rereco.data.constraints.sites.blacklist = siteBlacklist
    

    rerecoCmsswHelper.cmsswSetup(
        cmsswVersion,
        softwareEnvironment = softwareInitCommand ,
        scramArch = scramArchitecture
        )

    rerecoCmsswHelper.setDataProcessingConfig(
        scenario, "promptReco", globalTag = globalTagSetting,
        writeTiers = writeDataTiers)


    processedDatasetName = "rereco_%s_%s" % (globalTagSetting.replace("::","_"), processingVersion)
    unmergedDatasetName = "%s" % processedDatasetName
    commonLfnBase = lfnCategory
    commonLfnBase += "/%s" % acquisitionEra
    commonLfnBase += "/%s" % inputPrimaryDataset
    unmergedLfnBase = tempLfnCategory
    unmergedLfnBase += "/%s" % acquisitionEra
    unmergedLfnBase += "/%s" % inputPrimaryDataset


    if "RECO" in writeDataTiers:
        rerecoCmsswHelper.addOutputModule(
            outputModule("RECO"), primaryDataset = inputPrimaryDataset,
            processedDataset = unmergedDatasetName,
            dataTier = "RECO",
            lfnBase = "%s/RECO/%s" % ( unmergedLfnBase, processedDatasetName)
        )   

    if "ALCARECO" in writeDataTiers:
        rerecoCmsswHelper.addOutputModule(
            outputModule("ALCARECO"), primaryDataset = inputPrimaryDataset,
            processedDataset = unmergedDatasetName,
            dataTier = "ALCARECO",
            lfnBase = "%s/ALCARECO/%s" % ( unmergedLfnBase, processedDatasetName)
        )  

    if "AOD" in writeDataTiers:
        rerecoCmsswHelper.addOutputModule(
            outputModule("AOD"), primaryDataset = inputPrimaryDataset,
            processedDataset = unmergedDatasetName,
            dataTier = "AOD",
            lfnBase = "%s/AOD/%s" % ( unmergedLfnBase, processedDatasetName)
        )  



    # manipulate stage out and log archive if needed via type specific helper
    rerecoStageOutHelper = rerecoStageOut.getTypeHelper()
    rerecoLogArchHelper  = rerecoLogArch.getTypeHelper()

    # Emulation
    if emulationMode:
        rerecoCmsswHelper.data.emulator.emulatorName = "CMSSW"
        rerecoStageOutHelper.data.emulator.emulatorName = "StageOut"
        rerecoLogArchHelper.data.emulator.emulatorName = "LogArchive"



    #  //
    # // Merges for each output module
    #//
    if "RECO" in writeDataTiers:
        mergeReco = rereco.addTask("MergeReco")
        mergeRecoCmssw = mergeReco.makeStep("mergeReco")    
        mergeRecoCmssw.setStepType("CMSSW")
        mergeRecoStageOut = mergeRecoCmssw.addStep("stageOut1")
        mergeRecoStageOut.setStepType("StageOut")
        mergeRecoLogArch = mergeRecoCmssw.addStep("logArch1")
        mergeRecoLogArch.setStepType("LogArchive")

        mergeReco.applyTemplates()
        mergeReco.setSplittingAlgorithm("WMBSMergeBySize", max_merge_size = 4294967296, min_merge_size = 500000000)  
        mergeReco.addGenerator("BasicNaming")
        mergeReco.addGenerator("BasicCounter")
        mergeReco.setTaskType("Merge")
        mergeRecoCmsswHelper = mergeRecoCmssw.getTypeHelper()
        mergeRecoCmsswHelper.cmsswSetup(
            cmsswVersion,
            softwareEnvironment = softwareInitCommand,
            scramArch = scramArchitecture,
        )

        mergeRecoCmsswHelper.setDataProcessingConfig(scenario, "merge")
        mergeRecoCmsswHelper.addOutputModule(
            "Merged", primaryDataset = inputPrimaryDataset,
            processedDataset = processedDatasetName,
            dataTier = "RECO",
            lfnBase = "%s/RECO/%s" % ( commonLfnBase, processedDatasetName)
        )


        mergeReco.setInputReference(rerecoCmssw, outputModule = outputModule("RECO"))
        if emulationMode:
            mergeRecoStageOutHelper = mergeRecoStageOut.getTypeHelper()
            mergeRecoLogArchHelper  = mergeRecoLogArch.getTypeHelper()
            mergeRecoCmsswHelper.data.emulator.emulatorName = "CMSSW"
            mergeRecoStageOutHelper.data.emulator.emulatorName = "StageOut"
            mergeRecoLogArchHelper.data.emulator.emulatorName = "LogArchive"

    if "ALCARECO" in writeDataTiers:
        mergeAlca = rereco.addTask("MergeAlcaReco")
        mergeAlcaCmssw = mergeAlca.makeStep("mergeAlcaReco")    
        mergeAlcaCmssw.setStepType("CMSSW")
        mergeAlcaStageOut = mergeAlcaCmssw.addStep("stageOut1")
        mergeAlcaStageOut.setStepType("StageOut")
        mergeAlcaLogArch = mergeAlcaCmssw.addStep("logArch1")
        mergeAlcaLogArch.setStepType("LogArchive")
        mergeAlca.addGenerator("BasicNaming")
        mergeAlca.addGenerator("BasicCounter")
        mergeAlca.setTaskType("Merge")  
        mergeAlca.applyTemplates()
        mergeAlca.setSplittingAlgorithm("WMBSMergeBySize", max_merge_size = 4294967296, min_merge_size = 500000000)            

        mergeAlcaCmsswHelper = mergeAlcaCmssw.getTypeHelper()
        mergeAlcaCmsswHelper.cmsswSetup(
            cmsswVersion,
            softwareEnvironment = softwareInitCommand,
            scramArch = scramArchitecture,
        )

        mergeAlcaCmsswHelper.setDataProcessingConfig(scenario, "merge")
        mergeAlcaCmsswHelper.addOutputModule(
            "Merged", primaryDataset = inputPrimaryDataset,
            processedDataset = processedDatasetName,
            dataTier = "ALCARECO",
            lfnBase = "%s/ALCARECO/%s" % ( commonLfnBase, processedDatasetName)
        )


        mergeAlca.setInputReference(rerecoCmssw, outputModule = outputModule("ALCARECO"))
        if emulationMode:
            mergeAlcaStageOutHelper = mergeAlcaStageOut.getTypeHelper()
            mergeAlcaLogArchHelper  = mergeAlcaLogArch.getTypeHelper()
            mergeAlcaCmsswHelper.data.emulator.emulatorName = "CMSSW"
            mergeAlcaStageOutHelper.data.emulator.emulatorName = "StageOut"
            mergeAlcaLogArchHelper.data.emulator.emulatorName = "LogArchive"




    if "AOD" in writeDataTiers:
        mergeAod = rereco.addTask("MergeAod")
        mergeAodCmssw = mergeAod.makeStep("mergeAod")    
        mergeAodCmssw.setStepType("CMSSW")
        mergeAodStageOut = mergeAodCmssw.addStep("stageOut1")
        mergeAodStageOut.setStepType("StageOut")
        mergeAodLogArch = mergeAodCmssw.addStep("logArch1")
        mergeAodLogArch.setStepType("LogArchive")
        mergeAod.addGenerator("BasicNaming")
        mergeAod.addGenerator("BasicCounter")
        mergeAod.setTaskType("Merge")
        mergeAod.applyTemplates()
        mergeAod.setSplittingAlgorithm("WMBSMergeBySize", max_merge_size = 4294967296)

        mergeAodCmsswHelper = mergeAodCmssw.getTypeHelper()
        mergeAodCmsswHelper.cmsswSetup(
            cmsswVersion,
            softwareEnvironment = softwareInitCommand,
            scramArch = scramArchitecture,
        )

        mergeAodCmsswHelper.setDataProcessingConfig(scenario, "merge")
        mergeAodCmsswHelper.addOutputModule(
            "Merged", primaryDataset = inputPrimaryDataset,
            processedDataset = processedDatasetName,
            dataTier = "AOD",
            lfnBase = "%s/AOD/%s" % ( commonLfnBase, processedDatasetName)
        )

        mergeAod.setInputReference(rerecoCmssw, outputModule = outputModule("AOD"))
        if emulationMode:
            mergeAodStageOutHelper = mergeAodStageOut.getTypeHelper()
            mergeAodLogArchHelper  = mergeAodLogArch.getTypeHelper()
            mergeAodCmsswHelper.data.emulator.emulatorName = "CMSSW"
            mergeAodStageOutHelper.data.emulator.emulatorName = "StageOut"
            mergeAodLogArchHelper.data.emulator.emulatorName = "LogArchive"


    return workload
