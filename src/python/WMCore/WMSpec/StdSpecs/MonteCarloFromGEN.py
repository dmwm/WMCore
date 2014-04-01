#!/usr/bin/env python
"""
_MonteCarloFromGEN_

Workflow for processing MonteCarlo GEN files.
"""

from WMCore.Lexicon import primdataset
from WMCore.WMSpec.StdSpecs.DataProcessing import DataProcessing

class MonteCarloFromGENWorkloadFactory(DataProcessing):
    """
    _MonteCarloFromGENWorkloadFactory_

    Stamp out MonteCarloFromGEN workflows.
    """

    def buildWorkload(self):
        """
        _buildWorkload_

        Build a workflow for a MonteCarloFromGEN request.
        This represents the following tree:
        - Processing task
          - Merge task
            - LogCollect task
          - Cleanup task
          - LogCollect task
        """
        (inputPrimaryDataset, self.inputProcessedDataset,
         self.inputDataTier) = self.inputDataset[1:].split("/")

        if self.inputPrimaryDataset is None:
            self.inputPrimaryDataset = inputPrimaryDataset

        workload = self.createWorkload()
        workload.setDashboardActivity("production")
        self.reportWorkflowToDashboard(workload.getDashboardActivity())
        workload.setWorkQueueSplitPolicy("Block", self.procJobSplitAlgo,
                                         self.procJobSplitArgs,
                                         OpenRunningTimeout = self.openRunningTimeout)
        procTask = workload.newTask("MonteCarloFromGEN")

        outputMods = self.setupProcessingTask(procTask, "Processing",
                                              self.inputDataset,
                                              couchURL = self.couchURL,
                                              couchDBName = self.couchDBName,
                                              configCacheUrl = self.configCacheUrl,
                                              configDoc = self.configCacheID,
                                              splitAlgo = self.procJobSplitAlgo,
                                              splitArgs = self.procJobSplitArgs,
                                              stepType = "CMSSW",
                                              primarySubType = "Production",
                                              timePerEvent = self.timePerEvent,
                                              memoryReq = self.memory,
                                              sizePerEvent = self.sizePerEvent)
        self.addLogCollectTask(procTask)

        for outputModuleName in outputMods.keys():
            self.addMergeTask(procTask, self.procJobSplitAlgo,
                              outputModuleName)

        # setting the parameters which need to be set for all the tasks
        # sets acquisitionEra, processingVersion, processingString
        workload.setTaskPropertiesFromWorkload()
        return workload

    def __call__(self, workloadName, arguments):
        """
        _call_

        Create a MonteCarloFromGEN workload with the given parameters.
        """
        DataProcessing.__call__(self, workloadName, arguments)
        return self.buildWorkload()

    def validateSchema(self, schema):
        """
        _validateSchema_

        Standard StdBase schema validation, plus verification
        of the ConfigCacheID
        """
        DataProcessing.validateSchema(self, schema)
        couchUrl = schema.get("ConfigCacheUrl", None) or schema["CouchURL"]
        self.validateConfigCacheExists(configID = schema["ConfigCacheID"],
                                       couchURL = couchUrl,
                                       couchDBName = schema["CouchDBName"])
        return

    @staticmethod
    def getWorkloadArguments():
        baseArgs = DataProcessing.getWorkloadArguments()
        specArgs = {"PrimaryDataset" : {"default" : None, "type" : str,
                                        "optional" : True, "validate" : primdataset,
                                        "attr" : "inputPrimaryDataset", "null" : False},
                    "ConfigCacheID" : {"default" : None, "type" : str,
                                       "optional" : False, "validate" : None,
                                       "attr" : "configCacheID", "null" : False}}
        baseArgs.update(specArgs)
        return baseArgs
