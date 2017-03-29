#!/usr/bin/env python
"""
_MultiMergeProductionWorkload_

Sample/Tester for a production workflow that writes several datasets
and associated merges for each dataset.

Production task produces three output datasets,
Merge tasks are used to merge those datasets

"""
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.WMStep import makeWMStep
from WMCore.WMSpec.Steps.StepFactory import getStepTypeHelper

#  //
# // Set up the basic workload task and step structure
#//
workload = newWorkload("MultiOutputs")
workload.setOwner("WMTest")
workload.setStartPolicy('MonteCarlo')
workload.setEndPolicy('SingleShot')
workload.setSiteWhitelist(["T2_XX_SiteA", "T2_XX_SiteB", "T2_XX_SiteC"])

#  //
# // set up the production task
#//
production = workload.newTask("Production")
#WARNING: this is arbitrary task type (wmbs schema only supprot "Processing", "Merge", "Harvest") - maybe add "MCProduction"
production.setTaskType("Merge")
production.addProduction(totalEvents = 1000)
prodCmssw = production.makeStep("cmsRun1")
prodCmssw.setStepType("CMSSW")
prodStageOut = prodCmssw.addStep("stageOut1")
prodStageOut.setStepType("StageOut")
production.applyTemplates()
production.setSplittingAlgorithm("EventBased", events_per_job = 100)


#  //
# // populate the details of the production tasks
#//
#  //
# // production cmssw step
#//
#
# TODO: Anywhere helper.data is accessed means we need a method added to the
# type based helper class to provide a clear API.
prodCmsswHelper = prodCmssw.getTypeHelper()

prodCmsswHelper.data.application.setup.cmsswVersion = "CMSSW_X_Y_Z"
prodCmsswHelper.data.application.setup.softwareEnvironment = " . /uscmst1/prod/sw/cms/bashrc prod"

prodCmsswHelper.data.application.configuration.configCacheUrl = "http://whatever"

prodCmsswHelper.addOutputModule("writeOutput1", primaryDataset = "Primary",
                                processedDataset = "Processed1-unmerged",
                                dataTier = "TIERONE")
prodCmsswHelper.addOutputModule("writeOutput2", primaryDataset = "Primary",
                                processedDataset = "Processed2-unmerged",
                                dataTier = "TIERTWO")
prodCmsswHelper.addOutputModule("writeOutput3", primaryDataset = "Primary",
                                processedDataset = "Processed3-unmerged",
                                dataTier = "TIERTHREE")
#print prodCmsswHelper.data



#  //
# // set up the merge task for output dataset 1
#//
merge1 = production.addTask("MergeOutput1")
merge1.setTaskType("Merge")
merge1Cmssw = merge1.makeStep("cmsRun1")
merge1Cmssw.setStepType("CMSSW")
merge1StageOut = merge1Cmssw.addStep("stageOut1")
merge1StageOut.setStepType("StageOut")
merge1.applyTemplates()
merge1.setSplittingAlgorithm("MergeBySize", merge_size = 20000000)

#  //
# // set up the merge task for output dataset 1
#//
merge2 = production.addTask("MergeOutput2")
merge2.setTaskType("Merge")
merge2Cmssw = merge2.makeStep("cmsRun1")
merge2Cmssw.setStepType("CMSSW")
merge2StageOut = merge2Cmssw.addStep("stageOut1")
merge2StageOut.setStepType("StageOut")
merge2.applyTemplates()
merge2.setSplittingAlgorithm("MergeBySize", merge_size = 20000000)

#  //
# // set up the merge task for output dataset 3
#//
merge3 = production.addTask("MergeOutput3")
merge3.setTaskType("Merge")
merge3Cmssw = merge3.makeStep("cmsRun1")
merge3Cmssw.setStepType("CMSSW")
merge3StageOut = merge3Cmssw.addStep("stageOut1")
merge3StageOut.setStepType("StageOut")
merge3.applyTemplates()
merge3.setSplittingAlgorithm("MergeBySize", merge_size = 20000000)



#  //
# // production stage out step
#//
prodStageOutHelper = prodStageOut.getTypeHelper()


#  //
# // merge cmssw step
#//
# point it at the input step from the previous task
merge1.setInputReference(prodCmssw, outputModule = "writeOutput1")
merge2.setInputReference(prodCmssw, outputModule = "writeOutput2")
merge3.setInputReference(prodCmssw, outputModule = "writeOutput3")


#  //
# // populate the details of the merge tasks
#//
merge1CmsswHelper = merge1Cmssw.getTypeHelper()
merge1CmsswHelper.data.application.setup.cmsswVersion = "CMSSW_X_Y_Z"
merge1CmsswHelper.data.application.setup.softwareEnvironment = " . /uscmst1/prod/sw/cms/bashrc prod"
merge1CmsswHelper.data.application.configuration.configCacheUrl = "http://whatever"
merge1CmsswHelper.addOutputModule("mergeData1", primaryDataset = "Primary",
                                 processedDataset = "Processed1",
                                 dataTier = "TIER1")

merge2CmsswHelper = merge2Cmssw.getTypeHelper()
merge2CmsswHelper.data.application.setup.cmsswVersion = "CMSSW_X_Y_Z"
merge2CmsswHelper.data.application.setup.softwareEnvironment = " . /uscmst1/prod/sw/cms/bashrc prod"
merge2CmsswHelper.data.application.configuration.configCacheUrl = "http://whatever"
merge2CmsswHelper.addOutputModule("mergeData2", primaryDataset = "Primary",
                                 processedDataset = "Processed2",
                                 dataTier = "TIER2")


merge3CmsswHelper = merge3Cmssw.getTypeHelper()
merge3CmsswHelper.data.application.setup.cmsswVersion = "CMSSW_X_Y_Z"
merge3CmsswHelper.data.application.setup.softwareEnvironment = " . /uscmst1/prod/sw/cms/bashrc prod"
merge3CmsswHelper.data.application.configuration.configCacheUrl = "http://whatever"
merge3CmsswHelper.addOutputModule("mergeData3", primaryDataset = "Primary",
                                 processedDataset = "Processed3",
                                 dataTier = "TIER3")




#print workload.data
