#!/usr/bin/env python
"""
_MonteCarloFromGEN_

Workflow for processing MonteCarlo GEN files.
"""
from Utils.Utilities import strToBool
from WMCore.Lexicon import primdataset, dataset
from WMCore.WMSpec.StdSpecs.DataProcessing import DataProcessing
from WMCore.WMSpec.WMWorkloadTools import parsePileupConfig


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
        workload.setWorkQueueSplitPolicy("Block", self.procJobSplitAlgo,
                                         self.procJobSplitArgs,
                                         OpenRunningTimeout=self.openRunningTimeout)
        procTask = workload.newTask("MonteCarloFromGEN")

        outputMods = self.setupProcessingTask(procTask, "Processing",
                                              self.inputDataset,
                                              couchDBName=self.couchDBName,
                                              configCacheUrl=self.configCacheUrl,
                                              configDoc=self.configCacheID,
                                              splitAlgo=self.procJobSplitAlgo,
                                              splitArgs=self.procJobSplitArgs,
                                              stepType="CMSSW",
                                              primarySubType="Production")
        self.addLogCollectTask(procTask)

        # pile up support
        if self.pileupConfig:
            self.setupPileup(procTask, self.pileupConfig)

        for outputModuleName in outputMods:
            self.addMergeTask(procTask, self.procJobSplitAlgo, outputModuleName)

        # setting the parameters which need to be set for all the tasks
        # sets acquisitionEra, processingVersion, processingString
        workload.setTaskPropertiesFromWorkload()

        # set the LFN bases (normally done by request manager)
        # also pass runNumber (workload evaluates it)
        workload.setLFNBase(self.mergedLFNBase, self.unmergedLFNBase,
                            runNumber=self.runNumber)
        self.reportWorkflowToDashboard(workload.getDashboardActivity())

        return workload

    def __call__(self, workloadName, arguments):
        """
        _call_

        Create a MonteCarloFromGEN workload with the given parameters.
        """
        DataProcessing.__call__(self, workloadName, arguments)

        # Transform the pileup as required by the CMSSW step
        self.pileupConfig = parsePileupConfig(self.mcPileup, self.dataPileup)
        # Adjust the pileup splitting
        self.procJobSplitArgs.setdefault("deterministicPileup", self.deterministicPileup)

        return self.buildWorkload()

    def validateSchema(self, schema):
        """
        _validateSchema_

        Standard StdBase schema validation, plus verification
        of the ConfigCacheID
        """
        DataProcessing.validateSchema(self, schema)
        self.validateConfigCacheExists(configID=schema["ConfigCacheID"],
                                       configCacheUrl=schema['ConfigCacheUrl'],
                                       couchDBName=schema["CouchDBName"],
                                       getOutputModules=False)

        return

    @staticmethod
    def getWorkloadCreateArgs():
        baseArgs = DataProcessing.getWorkloadCreateArgs()
        specArgs = {"RequestType": {"default": "MonteCarloFromGEN", "optional": False},
                    "PrimaryDataset": {"optional": True, "validate": primdataset,
                                       "attr": "inputPrimaryDataset", "null": True},
                    "MCPileup": {"validate": dataset, "attr": "mcPileup", "null": True},
                    "DataPileup": {"validate": dataset, "null": True},
                    "DeterministicPileup": {"default": False, "type": strToBool,
                                            "optional": True, "null": False}
                   }
        baseArgs.update(specArgs)
        DataProcessing.setDefaultArgumentsProperty(baseArgs)
        return baseArgs
