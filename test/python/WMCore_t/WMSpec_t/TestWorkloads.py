#!/usr/bin/env python
"""
_TestSpec_


Standard Spec to use for development & unit testing

"""



from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.WMStep import makeWMStep


def oneTaskTwoStep():
    workload = newWorkload("OneTaskTwoStepTest")


    task1 = workload.newTask("FirstTask")

    step1 = task1.makeStep("cmsRun1")
    step1.setStepType("CMSSW")

    step2 = step1.addStep("stageOut1")
    step2.setStepType("StageOut")

    task1.applyTemplates()

    print str(workload.data)
    return workload



def oneTaskFourStep():

    workload = newWorkload("OneTaskFourStepTest")


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


oneTaskTwoStep()
