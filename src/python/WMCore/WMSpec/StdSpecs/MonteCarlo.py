#!/usr/bin/env python
"""
_MonteCarlo_

Created by Dave Evans on 2010-08-17.
Copyright (c) 2010 Fermilab. All rights reserved.

Support for pile up:
Normally, the generation task has no input. However, if there is a
pile up section defined in the configuration, the generation task
fetches from DBS the information about pileup input.
"""

import math

from Utils.Utilities import strToBool
from WMCore.Lexicon import primdataset, dataset
from WMCore.WMSpec.StdSpecs.StdBase import StdBase
from WMCore.WMSpec.WMWorkloadTools import parsePileupConfig


class MonteCarloWorkloadFactory(StdBase):
    """
    _MonteCarloWorkloadFactory_

    Stamp out Monte Carlo workflows.
    """

    def buildWorkload(self):
        """
        _buildWorkload_

        Build a workflow for a MonteCarlo request.
        This represents the following tree:
        - Production task
          - Merge task
            - LogCollect task
          - Cleanup task
          - LogCollect task
        """
        workload = self.createWorkload()
        workload.setDashboardActivity("production")
        workload.setWorkQueueSplitPolicy("MonteCarlo", self.prodJobSplitAlgo,
                                         self.prodJobSplitArgs)
        workload.setEndPolicy("SingleShot")
        prodTask = workload.newTask("Production")

        outputMods = self.setupProcessingTask(prodTask, "Production", None,
                                              couchDBName=self.couchDBName,
                                              configDoc=self.configCacheID,
                                              splitAlgo=self.prodJobSplitAlgo,
                                              splitArgs=self.prodJobSplitArgs,
                                              configCacheUrl=self.configCacheUrl,
                                              seeding=self.seeding,
                                              totalEvents=self.totalEvents,
                                              eventsPerLumi=self.eventsPerLumi)
        self.addLogCollectTask(prodTask)

        # pile up support
        if self.pileupConfig:
            self.setupPileup(prodTask, self.pileupConfig)

        for outputModuleName in outputMods:
            self.addMergeTask(prodTask, self.prodJobSplitAlgo,
                              outputModuleName,
                              lfn_counter=self.previousJobCount)

        maxFiles = workload.getBlockCloseMaxFiles()
        if self.eventsPerLumi != self.eventsPerJob:
            # Heuristic protection for blocks with too much lumis
            maxFiles = 5

        # Change some defaults in the DBS block close settings
        workload.setBlockCloseSettings(workload.getBlockCloseMaxWaitTime(),
                                       maxFiles,
                                       workload.getBlockCloseMaxEvents(),
                                       workload.getBlockCloseMaxSize())

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
        Store the arguments in attributes with the proper
        formatting.
        """
        StdBase.__call__(self, workloadName, arguments)

        # Adjust the events by the filter efficiency
        self.totalEvents = int(self.requestNumEvents / self.filterEfficiency)

        # We don't write out every event in MC,
        # adjust the size per event accordingly
        self.sizePerEvent = self.sizePerEvent * self.filterEfficiency

        # Tune the splitting, only EventBased is allowed for MonteCarlo
        # 8h jobs are CMS standard, set the default with that in mind
        self.prodJobSplitAlgo = "EventBased"
        self.eventsPerJob, self.eventsPerLumi = StdBase.calcEvtsPerJobLumi(self.eventsPerJob,
                                                                           self.eventsPerLumi,
                                                                           self.timePerEvent)

        self.prodJobSplitArgs = {"events_per_job": self.eventsPerJob,
                                 "events_per_lumi": self.eventsPerLumi,
                                 "lheInputFiles": self.lheInputFiles}

        # Transform the pileup as required by the CMSSW step
        self.pileupConfig = parsePileupConfig(self.mcPileup, self.dataPileup)
        # Adjust the pileup splitting
        self.prodJobSplitArgs.setdefault("deterministicPileup", self.deterministicPileup)

        # Production can be extending statistics,
        # need to move the initial lfn counter
        self.previousJobCount = 0
        if self.firstLumi > 1:
            self.previousJobCount = int(math.ceil((self.firstEvent - 1) / self.eventsPerJob))
            self.prodJobSplitArgs["initial_lfn_counter"] = self.previousJobCount

        # Feed values back to save in couch
        arguments['EventsPerJob'] = self.eventsPerJob

        return self.buildWorkload()

    def validateSchema(self, schema):
        self.validateConfigCacheExists(configID=schema["ConfigCacheID"],
                                       configCacheUrl=schema['ConfigCacheUrl'],
                                       couchDBName=schema["CouchDBName"],
                                       getOutputModules=False)
        return

    @staticmethod
    def getWorkloadCreateArgs():
        """
        Some default values set for testing purposes
        """
        baseArgs = StdBase.getWorkloadCreateArgs()
        specArgs = {"RequestType": {"default": "MonteCarlo", "optional": False},
                    "PrimaryDataset": {"optional": False, "validate": primdataset,
                                       "attr": "inputPrimaryDataset", "null": False},
                    "Seeding": {"default": "AutomaticSeeding", "null": False,
                                "validate": lambda x: x in ["ReproducibleSeeding", "AutomaticSeeding"]},
                    "FilterEfficiency": {"default": 1.0, "type": float, "null": False,
                                         "validate": lambda x: x > 0.0},
                    "RequestNumEvents": {"type": int, "null": False,
                                         "optional": False, "validate": lambda x: x > 0},
                    "FirstEvent": {"default": 1, "type": int, "validate": lambda x: x > 0,
                                   "null": False},
                    "FirstLumi": {"default": 1, "type": int, "validate": lambda x: x > 0,
                                  "null": False},
                    "MCPileup": {"validate": dataset, "attr": "mcPileup", "null": True},
                    "DataPileup": {"validate": dataset, "attr": "dataPileup", "null": True},
                    "SplittingAlgo": {"default": "EventBased", "null": False,
                                      "validate": lambda x: x in ["EventBased"],
                                      "attr": "prodJobSplitAlgo"},
                    "DeterministicPileup": {"default": False, "type": strToBool, "null": False},
                    "EventsPerJob": {"type": int, "validate": lambda x: x > 0, "null": True},
                    "EventsPerLumi": {"type": int, "validate": lambda x: x > 0, "null": True},
                    "LheInputFiles": {"default": False, "type": strToBool, "null": False}
                    }
        baseArgs.update(specArgs)
        StdBase.setDefaultArgumentsProperty(baseArgs)
        return baseArgs
