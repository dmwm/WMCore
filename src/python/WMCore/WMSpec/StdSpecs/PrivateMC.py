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

import logging
import os
import time

from WMCore.WMSpec.StdSpecs.Analysis import AnalysisWorkloadFactory, getCommonTestArgs
from WMCore.WMSpec.StdSpecs.StdBase import StdBase


def getTestArguments():
    """
    _getTestArguments_

    """
    args = getCommonTestArgs()

    args["PrimaryDataset"] = "MonteCarloData"
    args["RequestNumEvents"] = 10
    args["ConfigCacheID"] = "f90fc973b731a37c531f6e60e6c57955"

    args["FirstEvent"] = 1
    args["FirstLumi"] = 1

    return args


class PrivateMCWorkloadFactory(AnalysisWorkloadFactory):
    """
    PrivateMCWorkloadFactory

    Generate User (Private) Monte Carlo workflows.
    """

    def __init__(self):
        super(PrivateMCWorkloadFactory, self).__init__()
        self.requiredFields = ["CMSSWVersion", "AnalysisConfigCacheDoc", "PrimaryDataset",
                               "CouchURL", "CouchDBName", "RequestNumEvents", "ScramArch"]

    def buildWorkload(self):
        """
        _buildWorkload_

        Build a workflow for a MonteCarlo request.  This means a production
        config and merge tasks for each output module.

        """
        self.commonWorkload()
        prodTask = self.workload.newTask("PrivateMC")

        self.workload.setWorkQueueSplitPolicy("MonteCarlo", self.prodJobSplitAlgo, self.prodJobSplitArgs)
        self.workload.setEndPolicy("SingleShot")

        outputMods = self.setupProcessingTask(prodTask, "PrivateMC", None,
                                              couchURL=self.couchURL, couchDBName=self.couchDBName,
                                              configDoc=self.configCacheID, splitAlgo=self.prodJobSplitAlgo,
                                              splitArgs=self.prodJobSplitArgs,
                                              seeding=self.seeding, totalEvents=self.totalEvents,
                                              userSandbox=self.userSandbox, userFiles=self.userFiles)

        self.setUserOutput(prodTask)

        # Pile up support
        if self.pileupConfig:
            self.setupPileup(prodTask, self.pileupConfig)

        return self.workload

    def __call__(self, workloadName, arguments):
        """
        Create a workload instance for a MonteCarlo request

        """

        # Monte Carlo arguments
        self.inputPrimaryDataset = arguments["PrimaryDataset"]
        self.seeding = arguments.get("Seeding", "AutomaticSeeding")
        self.firstEvent = arguments.get("FirstEvent", 1)
        self.firstLumi = arguments.get("FirstLumi", 1)

        # Pileup configuration for the first generation task
        self.pileupConfig = arguments.get("PileupConfig", None)

        # Splitting arguments
        self.totalEvents = int(arguments.get("TotalUnits", 1))
        self.prodJobSplitAlgo  = arguments.get("JobSplitAlgo", "EventBased")
        self.prodJobSplitArgs  = arguments.get("JobSplitArgs", {"events_per_job": self.totalEvents})

        return super(PrivateMCWorkloadFactory, self).__call__(workloadName, arguments)


def privateMCWorkload(workloadName, arguments):
    """
    _privateMCWorkload_

    Instantiate the PrivateMCWorkloadFactory and have it generate a workload for
    the given parameters.

    """
    factory = PrivateMCWorkloadFactory()
    return factory(workloadName, arguments)
