#!/usr/bin/env python
"""
_BasicProductionWorkload_

Sample/Tester for a simple production workflow and associated merge.

Production task produces a single output dataset,
Merge task is used to merge that dataset

"""
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.WMStep import makeWMStep
from WMCore.WMSpec.Steps.StepFactory import getStepTypeHelper

#  //
# // Set up the basic workload task and step structure
#//
workload = newWorkload("BasicProduction")
workload.setOwner("WMTest")
workload.setStartPolicy('MonteCarlo', **{'SliceType': 'NumberOfEvents', 'SliceSize': 100})
workload.setEndPolicy('SingleShot')
workload.setSiteWhitelist(["T2_XX_SiteA", "T2_XX_SiteB", "T2_XX_SiteC"])
#  //
# // set up the production task
#//
production = workload.newTask("Production")
production.addProduction(totalevents = 1000)
#WARNING: this is arbitrary task type (wmbs schema only supprot "Processing", "Merge", "Harvest") - maybe add "MCProduction"
production.setTaskType("Merge")
production.setSplittingAlgorithm("EventBased", events_per_job = 100)
production.setSiteWhitelist(["T2_XX_SiteA", "T2_XX_SiteB", "T2_XX_SiteC"])
prodCmssw = production.makeStep("cmsRun1")
prodCmssw.setStepType("CMSSW")
prodStageOut = prodCmssw.addStep("stageOut1")
prodStageOut.setStepType("StageOut")
production.applyTemplates()

#  //
# // set up the merge task
#//
merge = production.addTask("Merge")
merge.setTaskType("Merge")
merge.setSplittingAlgorithm("MergeBySize", merge_size = 20000000)
mergeCmssw = merge.makeStep("cmsRun1")
mergeCmssw.setStepType("CMSSW")
mergeStageOut = mergeCmssw.addStep("stageOut1")
mergeStageOut.setStepType("StageOut")
merge.applyTemplates()


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

prodCmsswHelper.addOutputModule("writeData", primaryDataset = "Primary",
                                processedDataset = "Processed",
                                dataTier = "TIER")
#print prodCmsswHelper.data



#  //
# // production stage out step
#//
prodStageOutHelper = prodStageOut.getTypeHelper()


#  //
# // merge cmssw step
#//
# point it at the input step from the previous task
merge.setInputReference(prodCmssw, outputModule = "writeData")


#  //
# // populate the details of the merge tasks
#//



# print workload.data
