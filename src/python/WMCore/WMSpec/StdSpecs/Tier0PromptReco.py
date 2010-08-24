#!/usr/bin/env python
"""
_Tier0PromptReco_

The Tier0 Prompt Reco workflow with does RECO and ALCA skiming.
"""

from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.WMStep import makeWMStep
from WMCore.WMSpec.Steps.StepFactory import getStepTypeHelper

parseDataset = lambda x : { "Primary" : x.split("/")[1], 
                            "Processed": x.split("/")[2],
                            "Tier" : x.split("/")[3]}
def doOverrides(stepHelper, newStageOut, arguments):
    if newStageOut:
        stepHelper.addOverride('waitTime', arguments.get('waitTime'))
        stepHelper.addOverride('command', arguments.get('stageOutCommand'))
        stepHelper.addOverride('option',arguments.get('stageOutOptions'))
        stepHelper.addOverride('se-name',arguments.get('stageOutSeName'))
        stepHelper.addOverride('lfn-prefix', arguments.get('stageOutLfnPrefix'))
        stepHelper.addOverride('newStageOut',True)
    return stepHelper
        
def tier0PromptRecoWorkload(workloadName, arguments):
    """
    _tier0PromptRecoWorkload_

    The Tier0 Prompt Reco workflow with does RECO and ALCA skiming.    
    """
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
    #workload.setOwner(owner)
    workload.setStartPolicy('DatasetBlock')
    workload.setEndPolicy('SingleShot')
    workload.data.properties.acquisitionEra = acquisitionEra
    
    # configure possibly using new stageout code
    useNewStageOut = False
    if arguments.get('newStageOut', False):
        useNewStageOut = True
        print "***USING NEW STAGEOUT CODE***"
    #  //
    # // set up the production task
    #//
    rereco = workload.newTask("ReReco")
    rerecoCmssw = rereco.makeStep("cmsRun1")
    rerecoCmssw.setStepType("CMSSW")
    rerecoStageOut = rerecoCmssw.addStep("stageOut1")
    rerecoStageOut.setStepType("StageOut")
    rerecoStageOut = doOverrides(rerecoStageOut,    useNewStageOut, arguments )
    rerecoLogArch = rerecoCmssw.addStep("logArch1")
    rerecoLogArch.setStepType("LogArchive")
    doOverrides(rerecoLogArch,    useNewStageOut, arguments )
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

    rereco.setSiteWhitelist(siteWhitelist)
    rereco.setSiteBlacklist(siteBlacklist)

    rerecoCmsswHelper.cmsswSetup(
        cmsswVersion,
        softwareEnvironment = softwareInitCommand ,
        scramArch = scramArchitecture
        )

    rerecoCmsswHelper.setDataProcessingConfig(
        scenario, "promptReco", globalTag = globalTagSetting,
        writeTiers = writeDataTiers)

    commonLfnBase = lfnCategory
    commonLfnBase += "/%s" % acquisitionEra
    commonLfnBase += "/%s" % inputPrimaryDataset
    unmergedLfnBase = tempLfnCategory
    unmergedLfnBase += "/%s" % acquisitionEra
    unmergedLfnBase += "/%s" % inputPrimaryDataset

    if "RECO" in writeDataTiers:
        rerecoCmsswHelper.addOutputModule(
            "outputRECORECO", primaryDataset = inputPrimaryDataset,
            processedDataset = "%s-Tier0PromptReco-%s" % (acquisitionEra, processingVersion),
            dataTier = "RECO",
            lfnBase = "%s/RECO/%s-Tier0PromptReco-%s" % (unmergedLfnBase, acquisitionEra, processingVersion)
        )   

    if "ALCARECO" in writeDataTiers:
        rerecoCmsswHelper.addOutputModule(
            "outputALCARECOALCARECO", primaryDataset = inputPrimaryDataset,
            processedDataset = "%s-Tier0PromptReco-%s" % (acquisitionEra, processingVersion),
            dataTier = "ALCARECO",
            lfnBase = "%s/ALCARECO/%s-Tier0PromptReco-%s" % (unmergedLfnBase, acquisitionEra, processingVersion)
        )  

    if "AOD" in writeDataTiers:
        rerecoCmsswHelper.addOutputModule(
            "outputAODRECO", primaryDataset = inputPrimaryDataset,
            processedDataset = "%s-Tier0PromptReco-%s" % (acquisitionEra, processingVersion),
            dataTier = "AOD",
            lfnBase = "%s/AOD/%s" % (unmergedLfnBase, acquisitionEra, processingVersion)
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
        doOverrides(mergeRecoStageOut,    useNewStageOut, arguments )

        mergeRecoLogArch = mergeRecoCmssw.addStep("logArch1")
        doOverrides(mergeRecoLogArch,    useNewStageOut, arguments )

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
            processedDataset = "%s-Tier0PromptReco-%s" % (acquisitionEra, processingVersion),
            dataTier = "RECO",
            lfnBase = "%s/RECO/%s-Tier0PromptReco-%s" % (commonLfnBase, acquisitionEra, processingVersion)
        )


        mergeReco.setInputReference(rerecoCmssw, outputModule = "outputRECORECO")
        if emulationMode:
            mergeRecoStageOutHelper = mergeRecoStageOut.getTypeHelper()
            mergeRecoLogArchHelper  = mergeRecoLogArch.getTypeHelper()
            mergeRecoCmsswHelper.data.emulator.emulatorName = "CMSSW"
            mergeRecoStageOutHelper.data.emulator.emulatorName = "StageOut"
            mergeRecoLogArchHelper.data.emulator.emulatorName = "LogArchive"

    # We need to setup skims of the ALCA output and merges for the results of
    # the skims.
    if "ALCARECO" in writeDataTiers:
        skimAlca = rereco.addTask("SkimAlcaReco")
        skimAlca.setInputReference(rerecoCmssw, outputModule = "outputALCARECOALCARECO")        
        skimAlcaCmssw = skimAlca.makeStep("skimAlcaReco")
        skimAlcaCmssw.setStepType("CMSSW")
        skimAlcaStageOut = skimAlcaCmssw.addStep("stageOut1")
        skimAlcaStageOut.setStepType("StageOut")
        doOverrides(skimAlcaStageOut,    useNewStageOut, arguments )

        skimAlcaLogArch = skimAlcaCmssw.addStep("logArch1")
        doOverrides(skimAlcaLogArch,    useNewStageOut, arguments )

        skimAlcaLogArch.setStepType("LogArchive")
        skimAlca.addGenerator("BasicNaming")
        skimAlca.addGenerator("BasicCounter")
        skimAlca.setTaskType("Processing")
        skimAlca.applyTemplates()
        skimAlca.setSplittingAlgorithm("SplitFileBased")

        skimAlcaCmsswHelper = skimAlcaCmssw.getTypeHelper()
        skimAlcaCmsswHelper.cmsswSetup(cmsswVersion,
                                       softwareEnvironment = softwareInitCommand,
                                       scramArch = scramArchitecture)


        skims = ["TkAlBeamHalo",
                 "MuAlBeamHaloOverlaps",
                 "MuAlBeamHalo",
                 "TkAlCosmics0T",
                 "MuAlStandAloneCosmics",
                 "MuAlGlobalCosmics",
                 "MuAlCalIsolatedMu",
                 "HcalCalHOCosmics"]

        skimAlcaCmsswHelper.setDataProcessingConfig(scenario, "alcaSkim", skims = skims)
        for skim in skims:
            skimAlcaCmsswHelper.addOutputModule(
                "ALCARECOStream%s" % skim,
                primaryDataset = inputPrimaryDataset,
                processedDataset = "%s-Tier0PromptReco-%s-%s" % (acquisitionEra, skim, processingVersion),
                dataTier = "ALCARECO",
                lfnBase = "%s/ALCARECO/%s-Tier0PromptReco-%s-%s" % (commonLfnBase, acquisitionEra, skim, processingVersion))
            
            mergeAlca = skimAlca.addTask("MergeAlcaReco%s" % skim)
            mergeAlcaCmssw = mergeAlca.makeStep("mergeAlcaReco%s" % skim)
            mergeAlcaCmssw.setStepType("CMSSW")
            mergeAlcaStageOut = mergeAlcaCmssw.addStep("stageOut1")
            mergeAlcaStageOut.setStepType("StageOut")
            doOverrides(mergeAlcaStageOut,    useNewStageOut, arguments )

            mergeAlcaLogArch = mergeAlcaCmssw.addStep("logArch1")
            doOverrides(mergeAlcaLogArch,    useNewStageOut, arguments )

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
                scramArch = scramArchitecture)

            mergeAlcaCmsswHelper.setDataProcessingConfig(scenario, "merge")
            mergeAlcaCmsswHelper.addOutputModule(
                "Merged", primaryDataset = inputPrimaryDataset,
                processedDataset = "%s-Tier0PromptReco-%s-%s" % (acquisitionEra, skim, processingVersion),
                dataTier = "ALCARECO",
                lfnBase = "%s/ALCARECO/%s-Tier0PromptReco-%s-%s" % (commonLfnBase, acquisitionEra, skim, processingVersion)
                )

            mergeAlca.setInputReference(skimAlcaCmssw, outputModule = "ALCARECOStream%s" % skim)

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
        doOverrides(mergeAodStageOut,    useNewStageOut, arguments )

        mergeAodLogArch = mergeAodCmssw.addStep("logArch1")
        doOverrides(mergeAodLogArch,    useNewStageOut, arguments )

        mergeAodLogArch.addOverride('newStageOut',useNewStageOut)
        mergeAod.addGenerator("BasicNaming")
        mergeAod.addGenerator("BasicCounter")
        mergeAod.setTaskType("Merge")
        mergeAod.applyTemplates()
        mergeAod.setSplittingAlgorithm("WMBSMergeBySize", max_merge_size = 4294967296, min_merge_size = 500000000)
        
        mergeAodCmsswHelper = mergeAodCmssw.getTypeHelper()
        mergeAodCmsswHelper.cmsswSetup(
            cmsswVersion,
            softwareEnvironment = softwareInitCommand,
            scramArch = scramArchitecture,
        )

        mergeAodCmsswHelper.setDataProcessingConfig(scenario, "merge")
        mergeAodCmsswHelper.addOutputModule(
            "Merged", primaryDataset = inputPrimaryDataset,
            processedDataset = "%s-Tier0PromptReco-%s" % (acquisitionEra, processingVersion),
            dataTier = "AOD",
            lfnBase = "%s/AOD/%s-Tier0PromptReco-%s" % (commonLfnBase, acquisitionEra, processingVersion)
        )

        mergeAod.setInputReference(rerecoCmssw, outputModule = "outputAODRECO")
        if emulationMode:
            mergeAodStageOutHelper = mergeAodStageOut.getTypeHelper()
            mergeAodLogArchHelper  = mergeAodLogArch.getTypeHelper()
            mergeAodCmsswHelper.data.emulator.emulatorName = "CMSSW"
            mergeAodStageOutHelper.data.emulator.emulatorName = "StageOut"
            mergeAodLogArchHelper.data.emulator.emulatorName = "LogArchive"


    return workload

if __name__ == '__main__':
    arguments = {
        "InputDatasets" : "/MinBias/Commissioning09-v0/RAW",
        }

    workload = tier0PromptRecoWorkload("Test", arguments)
            
    for tt in  workload.taskIterator():
        print "Have task %s, printing children:" % tt.name()
        for t in tt.childTaskIterator():
            print "  %s" % t.name()
