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
from WMCore.Lexicon import primdataset, dataset
from WMCore.WMSpec.StdSpecs.StdBase import StdBase
from WMCore.WMSpec.WMWorkloadTools import strToBool, parsePileupConfig


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
        self.reportWorkflowToDashboard(workload.getDashboardActivity())
        workload.setWorkQueueSplitPolicy("MonteCarlo", self.prodJobSplitAlgo,
                                         self.prodJobSplitArgs)
        workload.setEndPolicy("SingleShot")
        prodTask = workload.newTask("Production")

        outputMods = self.setupProcessingTask(prodTask, "Production", None,
                                              couchURL=self.couchURL,
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

        for outputModuleName in outputMods.keys():
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
        if self.eventsPerJob is None:
            self.eventsPerJob = int((8.0 * 3600.0) / self.timePerEvent)
        if self.eventsPerLumi is None:
            self.eventsPerLumi = self.eventsPerJob
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

        return self.buildWorkload()

    def validateSchema(self, schema):
        couchUrl = schema.get("ConfigCacheUrl", None) or schema["CouchURL"]
        self.validateConfigCacheExists(configID=schema["ConfigCacheID"],
                                       couchURL=couchUrl,
                                       couchDBName=schema["CouchDBName"])
        return

    @staticmethod
    def getWorkloadArguments():
        baseArgs = StdBase.getWorkloadArguments()
        reqMgrArgs = StdBase.getWorkloadArgumentsWithReqMgr()
        baseArgs.update(reqMgrArgs)
        specArgs = {"RequestType": {"default": "MonteCarlo", "optional": True,
                                    "attr": "requestType"},
                    "PrimaryDataset": {"default": "BlackHoleTest", "type": str,
                                       "optional": False, "validate": primdataset,
                                       "attr": "inputPrimaryDataset", "null": False},
                    "Seeding": {"default": "AutomaticSeeding", "type": str,
                                "optional": True, "validate": lambda x: x in ["ReproducibleSeeding", "AutomaticSeeding"],
                                "attr": "seeding", "null": False},
                    "GlobalTag": {"default": "GT_MC_V1:All", "type": str,
                                  "optional": False, "validate": None,
                                  "attr": "globalTag", "null": False},
                    "FilterEfficiency": {"default": 1.0, "type": float,
                                         "optional": True, "validate": lambda x: x > 0.0,
                                         "attr": "filterEfficiency", "null": False},
                    "RequestNumEvents": {"default": 1000, "type": int,
                                         "optional": False, "validate": lambda x: x > 0,
                                         "attr": "requestNumEvents", "null": False},
                    "FirstEvent": {"default": 1, "type": int,
                                   "optional": True, "validate": lambda x: x > 0,
                                   "attr": "firstEvent", "null": False},
                    "FirstLumi": {"default": 1, "type": int,
                                  "optional": True, "validate": lambda x: x > 0,
                                  "attr": "firstLumi", "null": False},
                    "MCPileup": {"default": None, "type": str,
                                 "optional": True, "validate": dataset,
                                 "attr": "mcPileup", "null": False},
                    "DataPileup": {"default": None, "type": str,
                                   "optional": True, "validate": dataset,
                                   "attr": "dataPileup", "null": False},
                    "DeterministicPileup": {"default": False, "type": strToBool,
                                            "optional": True, "validate": None,
                                            "attr": "deterministicPileup", "null": False},
                    "EventsPerJob": {"default": None, "type": int,
                                     "optional": True, "validate": lambda x: x > 0,
                                     "attr": "eventsPerJob", "null": True},
                    "EventsPerLumi": {"default": None, "type": int,
                                      "optional": True, "validate": lambda x: x > 0,
                                      "attr": "eventsPerLumi", "null": True},
                    "LheInputFiles": {"default": False, "type": strToBool,
                                      "optional": True, "validate": None,
                                      "attr": "lheInputFiles", "null": False}
                   }
        baseArgs.update(specArgs)
        StdBase.setDefaultArgumentsProperty(baseArgs)
        return baseArgs
