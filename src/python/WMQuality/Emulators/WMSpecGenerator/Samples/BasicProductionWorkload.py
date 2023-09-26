#!/usr/bin/env python
"""
_BasicProductionWorkload_

Sample/Tester for a simple production workflow and associated merge.

Production task produces a single output dataset,
Merge task is used to merge that dataset

"""
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.StdSpecs.TaskChain import TaskChainWorkloadFactory

#  //
# // Set up the basic workload task and step structure
#//
def createWorkload(name="BasicProduction"):
    workload = newWorkload(name)
    workload.setOwner("DMWMTest")
    workload.setStartPolicy('MonteCarlo')
    workload.setEndPolicy('SingleShot')

    #  //
    # // set up the production task
    #//
    production = workload.newTask("Production")
    #TODO: Currently WMBS only support Processing, Merge, Harvesting type - may be add production type?
    production.setTaskType("Merge")

    production.addProduction(totalevents = 1000)
    prodCmssw = production.makeStep("cmsRun1")
    prodCmssw.setStepType("CMSSW")
    prodStageOut = prodCmssw.addStep("stageOut1")
    prodStageOut.setStepType("StageOut")
    production.applyTemplates()
    production.setSiteWhitelist(["T2_XX_SiteA"])

    #  //
    # // set up the merge task
    #//
    merge = production.addTask("Merge")
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

    return workload


def getProdArgs():
    mcArgs = TaskChainWorkloadFactory.getTestArguments()
    mcArgs.update({
                   "CouchURL": None,
                   "CouchDBName": None,
                   "ConfigCacheDoc" : None
                   })
    mcArgs.pop('ConfigCacheDoc')

    return mcArgs


class TestTaskChainFactory(TaskChainWorkloadFactory):
    """Override bits that talk to cmsssw"""
    def __call__(self, workflowName, args):
        workload = TaskChainWorkloadFactory.__call__(self, workflowName, args)
        #delattr(workload.taskIterator().next().steps().data.application.configuration,
        #        'configCacheUrl')
        return workload

    def determineOutputModules(self, *args, **kwargs):
        "Don't talk to couch"
        return {}

    def loadCouchID(self, *args, **kwargs):
        "Don't talk to couch"
        return None

def taskChainWorkload(workloadName, arguments):
    """
    _monteCarloWorkload_
    Instantiate the MonteCarloWorkflowFactory and have it generate a workload for
    the given parameters.
    """
    myTaskChainFactory = TestTaskChainFactory()
    return myTaskChainFactory(workloadName, arguments)
