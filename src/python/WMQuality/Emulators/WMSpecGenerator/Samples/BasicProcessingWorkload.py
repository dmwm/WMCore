#!/usr/bin/env python
"""
_Tier1ReRecoWorkload_
"""

from WMCore.WMSpec.WMWorkload import newWorkload

#  //
# // Set up the basic workload task and step structure
#//
def createWorkload(name="BasicProcessing"):
    workload = newWorkload(name)
    workload.setOwner("DMWMTest")
    workload.setStartPolicy('DatasetBlock')
    workload.setEndPolicy('SingleShot')
    workload.setCampaign("TestCampaign")
    workload.setPrepID("TestPrepID")
    #  //
    # // set up the production task
    #//
    rereco = workload.newTask("ReReco1")
    rereco.setTaskType("Processing")
    rerecoCmssw = rereco.makeStep("cmsRun1")
    rerecoCmssw.setStepType("CMSSW")
    skimStageOut = rerecoCmssw.addStep("stageOut1")
    skimStageOut.setStepType("StageOut")
    skimLogArch = rerecoCmssw.addStep("logArch1")
    skimLogArch.setStepType("LogArchive")
    rereco.applyTemplates()
    rereco.setSplittingAlgorithm("FileBased", files_per_job=1)
    rereco.addInputDataset(
        primary = "Cosmics",
        processed = "CRAFT09-PromptReco-v1",
        tier = "RECO",
        dbsurl = "https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader")

    #  //
    # // rereco cmssw step
    #//
    #
    # TODO: Anywhere helper.data is accessed means we need a method added to the
    # type based helper class to provide a clear API.
    rerecoCmsswHelper = rerecoCmssw.getTypeHelper()


    rerecoCmsswHelper.cmsswSetup(
        "CMSSW_3_1_2",
        softwareEnvironment=" . /uscmst1/prod/sw/cms/bashrc prod"
        )

    rerecoCmsswHelper.setDataProcessingConfig(
        "cosmics", "promptReco", globalTag="GLOBAL::BALLS",
        writeTiers=['RECO'])

    rerecoCmsswHelper.addOutputModule(
        "outputRECO", primaryDataset="Primary",
        processedDataset="Processed",
        dataTier="RECO")

    #Add a stageOut step
    skimStageOutHelper = skimStageOut.getTypeHelper()
    skimLogArchHelper  = skimLogArch.getTypeHelper()


    rereco.addGenerator("BasicNaming")
    return workload
