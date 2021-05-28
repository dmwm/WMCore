#!/usr/bin/env python
"""
_Tier1ReRecoWorkload_



"""
from WMCore.WMSpec.WMWorkload import newWorkload

DBSURL = "https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader"

#  //
# // Set up the basic workload task and step structure
#//
workload = newWorkload("MultiTaskReReco")
workload.setOwner("WMTest")
workload.setStartPolicy('DatasetBlock')
workload.setEndPolicy('SingleShot')

#  //
# // set up the production task
#//
rereco = workload.newTask("ReReco1")
rerecoCmssw = rereco.makeStep("cmsRun1")
rerecoCmssw.setStepType("CMSSW")
skimStageOut = rerecoCmssw.addStep("stageOut1")
skimStageOut.setStepType("StageOut")
skimLogArch = rerecoCmssw.addStep("logArch1")
skimLogArch.setStepType("LogArchive")
rereco.applyTemplates()
rereco.setSplittingAlgorithm("FileBased", files_per_job = 1)
rereco.addInputDataset(
    name="/Cosmics/ComissioningHI-PromptReco-v1/RECO",
    primary = "Cosmics",
    processed = "ComissioningHI-PromptReco-v1",
    tier = "RECO",
    dbsurl = DBSURL)

#  //
# // rereco cmssw step
#//
#
# TODO: Anywhere helper.data is accessed means we need a method added to the
# type based helper class to provide a clear API.
rerecoCmsswHelper = rerecoCmssw.getTypeHelper()


rerecoCmsswHelper.cmsswSetup(
    "CMSSW_3_1_2",
    softwareEnvironment = " . /uscmst1/prod/sw/cms/bashrc prod"
    )

rerecoCmsswHelper.setDataProcessingConfig(
    "cosmics", "promptReco", globalTag = "GLOBAL::BALLS",
    writeTiers = ['RECO'])

rerecoCmsswHelper.addOutputModule(
    "outputRECO", primaryDataset = "Primary",
    processedDataset = "Processed",
    dataTier = "RECO")

pileupConfig = {"data" : ["/HighPileUp/Run2011A-v1/RAW"]}
rerecoCmsswHelper.setupPileup(pileupConfig, DBSURL)

#Add a stageOut step
skimStageOutHelper = skimStageOut.getTypeHelper()
skimLogArchHelper  = skimLogArch.getTypeHelper()


rereco.addGenerator("BasicNaming")

#  //
# // set up the production task
#//
rereco = workload.newTask("ReReco2")
rerecoCmssw = rereco.makeStep("cmsRun1")
rerecoCmssw.setStepType("CMSSW")
skimStageOut = rerecoCmssw.addStep("stageOut1")
skimStageOut.setStepType("StageOut")
skimLogArch = rerecoCmssw.addStep("logArch1")
skimLogArch.setStepType("LogArchive")
rereco.applyTemplates()
rereco.setSplittingAlgorithm("FileBased", files_per_job = 1)
rereco.addInputDataset(
    name="/Cosmics/ComissioningHI-PromptReco-v1/RECO",
    primary = "Cosmics",
    processed = "ComissioningHI-PromptReco-v1",
    tier = "RECO",
    dbsurl = DBSURL)
#  //
# // rereco cmssw step
#//
#
# TODO: Anywhere helper.data is accessed means we need a method added to the
# type based helper class to provide a clear API.
rerecoCmsswHelper = rerecoCmssw.getTypeHelper()


rerecoCmsswHelper.cmsswSetup(
    "CMSSW_3_1_2",
    softwareEnvironment = " . /uscmst1/prod/sw/cms/bashrc prod"
    )

rerecoCmsswHelper.setDataProcessingConfig(
    "cosmics", "promptReco", globalTag = "GLOBAL::BALLS",
    writeTiers = ['RECO'])

rerecoCmsswHelper.addOutputModule(
    "outputRECO", primaryDataset = "Primary",
    processedDataset = "Processed",
    dataTier = "RECO")

#Add a stageOut step
skimStageOutHelper = skimStageOut.getTypeHelper()
skimLogArchHelper  = skimLogArch.getTypeHelper()


rereco.addGenerator("BasicNaming")

workload.setSiteWhitelist(["T2_XX_SiteA", "T2_XX_SiteB", "T2_XX_SiteC"])
