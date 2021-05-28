#!/usr/bin/env python
"""
_TestSpec_


Standard Spec to use for development & unit testing

"""

from WMCore.WMSpec.WMWorkload import newWorkload


def oneTaskTwoStep():
    workload = newWorkload("OneTaskTwoStepTest")
    workload.setDbsUrl("https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader")

    task1 = workload.newTask("FirstTask")

    step1 = task1.makeStep("cmsRun1")
    step1.setStepType("CMSSW")

    step2 = step1.addStep("stageOut1")
    step2.setStepType("StageOut")

    task1.applyTemplates()

    return workload


def oneTaskFourStep():
    workload = newWorkload("OneTaskFourStepTest")
    workload.setDbsUrl("https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader")

    task1 = workload.newTask("FirstTask")

    step1 = task1.makeStep("cmsRun1")
    step1.setStepType("CMSSW")

    step2 = step1.addStep("stageOut1")
    step2.setStepType("StageOut")

    step3 = step1.addStep("cmsRun2")
    step3.setStepType("CMSSW")

    step4 = step3.addStep("stageOut2")
    step4.setStepType("StageOut")

    return workload


def twoTaskTree():
    workload = newWorkload("TwoTaskTree")
    workload.setDbsUrl("https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader")

    task1 = workload.newTask("FirstTask")

    task2 = task1.addTask("SecondTask")

    step1 = task1.makeStep("cmsRun1")
    step1.setStepType("CMSSW")

    step2 = step1.addStep("stageOut1")
    step2.setStepType("StageOut")

    step3 = task2.makeStep("cmsRun2")
    step3.setStepType("CMSSW")

    step4 = step3.addStep("stageOut2")
    step4.setStepType("StageOut")

    return workload
