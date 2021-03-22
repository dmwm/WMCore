#!/usr/bin/env python
"""
_DataProcessing_

Base module for workflows with input.
"""
from __future__ import division

from Utils.Utilities import makeList
from WMCore.Lexicon import dataset, block, primdataset
from WMCore.WMSpec.StdSpecs.StdBase import StdBase


class DataProcessing(StdBase):
    """
    _DataProcessing_

    Base class for specs with input, it just defines some of the shared attributes
    for this kind of WMSpec.
    """

    def __call__(self, workloadName, arguments):
        StdBase.__call__(self, workloadName, arguments)

        # Handle the default of the various splitting algorithms
        self.procJobSplitArgs = {"include_parents": self.includeParents}
        if self.procJobSplitAlgo in ["EventBased", "EventAwareLumiBased"]:
            if self.eventsPerJob is None:
                self.eventsPerJob = int((8.0 * 3600.0) / self.timePerEvent)
            if self.procJobSplitAlgo == "EventAwareLumiBased":
                self.procJobSplitArgs["job_time_limit"] = 48 * 3600  # 2 days
            self.procJobSplitArgs["events_per_job"] = self.eventsPerJob
            arguments['EventsPerJob'] = self.eventsPerJob
        elif self.procJobSplitAlgo == "LumiBased":
            self.procJobSplitArgs["lumis_per_job"] = self.lumisPerJob
        elif self.procJobSplitAlgo == "FileBased":
            self.procJobSplitArgs["files_per_job"] = self.filesPerJob

        return

    @staticmethod
    def getWorkloadCreateArgs():
        baseArgs = StdBase.getWorkloadCreateArgs()
        specArgs = {"InputDataset": {"optional": False, "validate": dataset, "null": False},
                    "Scenario": {"optional": True, "null": True, "attr": "procScenario"},
                    "PrimaryDataset": {"optional": True, "validate": primdataset,
                                       "attr": "inputPrimaryDataset", "null": True},
                    "RunBlacklist": {"default": [], "type": makeList, "null": False,
                                     "validate": lambda x: all([int(y) > 0 for y in x])},
                    "RunWhitelist": {"default": [], "type": makeList, "null": False,
                                     "validate": lambda x: all([int(y) > 0 for y in x])},
                    "BlockBlacklist": {"default": [], "type": makeList,
                                       "validate": lambda x: all([block(y) for y in x])},
                    "BlockWhitelist": {"default": [], "type": makeList,
                                       "validate": lambda x: all([block(y) for y in x])},
                    "SplittingAlgo": {"default": "EventAwareLumiBased", "null": False,
                                      "validate": lambda x: x in ["EventBased", "LumiBased",
                                                                  "EventAwareLumiBased", "FileBased"],
                                      "attr": "procJobSplitAlgo"},
                    "EventsPerJob": {"type": int, "validate": lambda x: x > 0, "null": True},
                    "LumisPerJob": {"default": 8, "type": int, "null": False,
                                    "validate": lambda x: x > 0},
                    "FilesPerJob": {"default": 1, "type": int, "null": False,
                                    "validate": lambda x: x > 0}
                    }

        baseArgs.update(specArgs)
        StdBase.setDefaultArgumentsProperty(baseArgs)
        return baseArgs

    @staticmethod
    def getWorkloadAssignArgs():
        baseArgs = StdBase.getWorkloadAssignArgs()
        StdBase.setDefaultArgumentsProperty(baseArgs)
        return baseArgs
