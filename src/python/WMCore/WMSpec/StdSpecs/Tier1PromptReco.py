#!/usr/bin/env python
"""
_Tier1PromptReco_

Standard Tier1PromptReco workflow.
"""

import os
import WMCore.Lexicon

from WMCore.WMSpec.StdSpecs.StdBase import StdBase	

def getTestArguments():
    """
    _getTestArguments_

    This should be where the default REQUIRED arguments go
    This serves as documentation for what is currently required 
    by the standard ReReco workload in importable format.

    NOTE: These are test values.  If used in real workflows they
    will cause everything to crash/die/break, and we will be forced
    to hunt you down and kill you.
    """
    arguments = {
        "AcquisitionEra": "WMAgentCommissioning12",
        "Requestor": "Dirk.Hufnagel@cern.ch",

        "ScramArch": "slc5_amd64_gcc462",

        # these must be overridden
        "CMSSWVersion" : None,
        "ProcessingVersion" : None,
        "ProcScenario" : None,
        "GlobalTag" : None,
        "InputDataset" : None,
        "WriteTiers" : None,
        "AlcaSkims" : None,
        }

    return arguments

class Tier1PromptRecoWorkloadFactory(StdBase):
    """
    _Tier1PromptRecoWorkloadFactory_

    Stamp out Tier1PromptReco workflows.
    """
    def __init__(self):
        StdBase.__init__(self)
        self.multicore = False
        self.multicoreNCores = 1
        return

    def buildWorkload(self):
        """
        _buildWorkload_

        Build the workload given all of the input parameters.

        Not that there will be LogCollect tasks created for each processing
        task and Cleanup tasks created for each merge task.

        """
        (self.inputPrimaryDataset, self.inputProcessedDataset,
         self.inputDataTier) = self.inputDataset[1:].split("/")

        workload = self.createWorkload()
        workload.setDashboardActivity("tier0")
        workload.setWorkQueueSplitPolicy("Block", self.procJobSplitAlgo, self.procJobSplitArgs)

        cmsswStepType = "CMSSW"
        taskType = "Processing"
        if self.multicore:
            taskType = "MultiProcessing"

        recoOutputs = []
        for dataTier in self.writeTiers:
            recoOutputs.append( { 'dataTier' : dataTier,
                                  'eventContent' : dataTier,
                                  'filterName' : "Tier1PromptReco",
                                  'moduleLabel' : "write_%s" % dataTier } )

        recoTask = workload.newTask("Reco")
        recoOutMods = self.setupProcessingTask(recoTask, taskType, self.inputDataset,
                                               scenarioName = self.procScenario,
                                               scenarioFunc = "promptReco",
                                               scenarioArgs = { 'globalTag' : self.globalTag,
                                                                'skims' : self.alcaSkims,
                                                                'outputs' : recoOutputs },
                                               splitAlgo = self.procJobSplitAlgo,
                                               splitArgs = self.procJobSplitArgs,
                                               stepType = cmsswStepType)
        self.addLogCollectTask(recoTask)

        for recoOutLabel, recoOutInfo in recoOutMods.items():
            if recoOutInfo['dataTier'] != "ALCARECO":
                self.addMergeTask(recoTask,
                                  self.procJobSplitAlgo,
                                  recoOutLabel)
            else:
                alcaTask = recoTask.addTask("AlcaSkim")
                alcaOutMods = self.setupProcessingTask(alcaTask, taskType,
                                                       inputStep = recoTask.getStep("cmsRun1"),
                                                       inputModule = recoOutLabel,
                                                       scenarioName = self.procScenario,
                                                       scenarioFunc = "alcaSkim",
                                                       scenarioArgs = { 'globalTag' : self.globalTag,
                                                                        'skims' : self.alcaSkims },
                                                       splitAlgo = self.procJobSplitAlgo,
                                                       splitArgs = self.procJobSplitArgs,
                                                       stepType = cmsswStepType)
                for alcaOutLabel, alcaOutInfo in alcaOutMods.items():
                    self.addMergeTask(alcaTask,
                                      self.procJobSplitAlgo,
                                      alcaOutLabel)

        return workload

    def __call__(self, workloadName, arguments):
        """
        _call_

        Create a ReReco workload with the given parameters.
        """
        StdBase.__call__(self, workloadName, arguments)

        # Required parameters that must be specified by the Requestor.
        self.frameworkVersion = arguments['CMSSWVersion']
        self.globalTag = arguments['GlobalTag']
        self.procScenario = arguments['ProcScenario']
        self.writeTiers = arguments['WriteTiers']
        self.alcaSkims = arguments['AlcaSkims']
	self.inputDataset = arguments['InputDataset']

        if arguments.has_key('Multicore'):
            numCores = arguments.get('Multicore')
            if numCores == None or numCores == "":
                self.multicore = False
            elif numCores == "auto":
                self.multicore = True
                self.multicoreNCores = "auto"
            else:
                self.multicore = True
                self.multicoreNCores = numCores

        # Optional arguments that default to something reasonable.
        self.dbsUrl = arguments.get("DbsUrl", "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet")
        self.blockBlacklist = arguments.get("BlockBlacklist", [])
        self.blockWhitelist = arguments.get("BlockWhitelist", [])
        self.runBlacklist = arguments.get("RunBlacklist", [])
        self.runWhitelist = arguments.get("RunWhitelist", [])
        self.emulation = arguments.get("Emulation", False)

        # These are mostly place holders because the job splitting algo and
        # parameters will be updated after the workflow has been created.
        self.procJobSplitAlgo  = arguments.get("StdJobSplitAlgo", "FileBased")
        self.procJobSplitArgs  = arguments.get("StdJobSplitArgs", {})

        return self.buildWorkload()

    def validateSchema(self, schema):
        """
        _validateSchema_
        
        Check for required fields, and some skim facts
        """
        requiredFields = ["ScramArch", "CMSSWVersion", "ProcessingVersion",
                          "ProcScenario", "GlobalTag", "InputDataset",
                          "WriteTiers", "AlcaSkims"]
        self.requireValidateFields(fields = requiredFields, schema = schema,
                                   validate = False)

        try:
            WMCore.Lexicon.dataset(schema.get('InputDataset', ''))
        except AssertionError:
            self.raiseValidationException(msg = "Invalid input dataset!")

        return


def tier1promptrecoWorkload(workloadName, arguments):
    """
    _tier1promptrecoWorkload_

    Instantiate the Tier1PromptRecoWorkflowFactory and have it generate
    a workload for the given parameters.
    """
    myTier1PromptRecoFactory = Tier1PromptRecoWorkloadFactory()
    return myTier1PromptRecoFactory(workloadName, arguments)
