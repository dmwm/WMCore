#!/usr/bin/env python
"""
_PrivateMC_

Created by Eric Vaandering on 2012-03-22
Copyright (c) 2012 Fermilab. All rights reserved.

Support for pile up:
normally, the generation task has no input. However, if there is a
pile up section defined in the configuration, the generation task
fetches from DBS the information about pileup input.

"""

from WMCore.Lexicon import dataset
from WMCore.WMSpec.StdSpecs.Analysis import AnalysisWorkloadFactory
from WMCore.WMSpec.WMWorkloadTools import parsePileupConfig

class PrivateMCWorkloadFactory(AnalysisWorkloadFactory):
    """
    PrivateMCWorkloadFactory

    Generate User (Private) Monte Carlo workflows.
    """

    def buildWorkload(self):
        """
        _buildWorkload_

        Build a workflow for a MonteCarlo request.  This means a production
        config and merge tasks for each output module.

        """
        self.commonWorkload()
        prodTask = self.workload.newTask("PrivateMC")

        self.workload.setWorkQueueSplitPolicy("MonteCarlo",
                                              self.analysisJobSplitAlgo,
                                              self.analysisJobSplitArgs)
        self.workload.setEndPolicy("SingleShot")

        outputMods = self.setupProcessingTask(prodTask, "PrivateMC", None,
                                              couchURL = self.couchURL,
                                              couchDBName = self.couchDBName,
                                              configCacheUrl = self.configCacheUrl,
                                              configDoc = self.configCacheID,
                                              splitAlgo = self.analysisJobSplitAlgo,
                                              splitArgs = self.analysisJobSplitArgs,
                                              seeding = self.seeding,
                                              totalEvents = self.totalEvents,
                                              userSandbox = self.userSandbox,
                                              userFiles = self.userFiles)

        self.setUserOutput(prodTask)

        # Pileup configuration for the first generation task
        self.pileupConfig = parsePileupConfig(self.mcPileup, self.dataPileup)

        # Pile up support
        if self.pileupConfig:
            self.setupPileup(prodTask, self.pileupConfig)
        
        # setting the parameters which need to be set for all the tasks
        # sets acquisitionEra, processingVersion, processingString
        self.workload.setTaskPropertiesFromWorkload()

        # set the LFN bases (normally done by request manager)
        # also pass runNumber (workload evaluates it)
        self.workload.setLFNBase(self.mergedLFNBase, self.unmergedLFNBase,
                            runNumber = self.runNumber)

        return self.workload

    @staticmethod
    def getWorkloadArguments():
        baseArgs = AnalysisWorkloadFactory.getWorkloadArguments()
        specArgs = {"RequestType" : {"default" : "PrivateMC", "optional" : True,
                                      "attr" : "requestType"},
                    "PrimaryDataset" : {"default" : "MonteCarloData", "type" : str,
                                        "optional" : False, "validate" : None,
                                        "attr" : "inputPrimaryDataset", "null" : False},
                    "Seeding" : {"default" : "AutomaticSeeding", "type" : str,
                                 "optional" : True, "validate" : lambda x : x in ["ReproducibleSeeding",
                                                                                  "AutomaticSeeding"],
                                 "attr" : "seeding", "null" : False},
                    "FirstEvent" : {"default" : 1, "type" : int,
                                    "optional" : True, "validate" : lambda x : x > 0,
                                    "attr" : "firstEvent", "null" : False},
                    "FirstLumi" : {"default" : 1, "type" : int,
                                    "optional" : True, "validate" : lambda x : x > 0,
                                    "attr" : "firstLumi", "null" : False},
                    "MCPileup" : {"default" : None, "type" : str,
                                  "optional" : True, "validate" : dataset,
                                  "attr" : "mcPileup", "null" : False},
                    "DataPileup" : {"default" : None, "type" : str,
                                    "optional" : True, "validate" : dataset,
                                    "attr" : "dataPileup", "null" : False},
                    "TotalUnits" : {"default" : None, "type" : int,
                                    "optional" : True, "validate" : lambda x : x > 0,
                                    "attr" : "totalEvents", "null" : False}}
        baseArgs["InputDataset"]["optional"] = True
        baseArgs.update(specArgs)
        AnalysisWorkloadFactory.setDefaultArgumentsProperty(baseArgs)
        return baseArgs
