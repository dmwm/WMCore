#!/usr/bin/env python
"""
_DataProcessing_

Base module for workflows with input.
"""

from WMCore.Lexicon import dataset, couchurl, identifier, block
from WMCore.WMSpec.StdSpecs.StdBase import StdBase
from WMCore.WMSpec.WMWorkloadTools import makeList

class DataProcessing(StdBase):
    """
    _DataProcessing_

    Base class for specs with input, it just defines some of the shared attributes
    for this kind of WMSpec.
    """

    def __call__(self, workloadName, arguments):
        StdBase.__call__(self, workloadName, arguments)

        # Handle the default of the various splitting algorithms
        self.procJobSplitArgs = {"include_parents" : self.includeParents}
        if self.procJobSplitAlgo == "EventBased" or self.procJobSplitAlgo == "EventAwareLumiBased":
            if self.eventsPerJob is None:
                self.eventsPerJob = int((8.0 * 3600.0) / self.timePerEvent)
            self.procJobSplitArgs["events_per_job"] = self.eventsPerJob
            if self.procJobSplitAlgo == "EventAwareLumiBased":
                self.procJobSplitArgs["max_events_per_lumi"] = 20000
        elif self.procJobSplitAlgo == "LumiBased":
            self.procJobSplitArgs["lumis_per_job"] = self.lumisPerJob
        elif self.procJobSplitAlgo == "FileBased":
            self.procJobSplitArgs["files_per_job"] = self.filesPerJob

        return

    @staticmethod
    def getWorkloadArguments():
        baseArgs = StdBase.getWorkloadArguments()
        reqMgrArgs = StdBase.getWorkloadArgumentsWithReqMgr()
        baseArgs.update(reqMgrArgs)
        specArgs = {"InputDataset" : {"default" : "/MinimumBias/ComissioningHI-v1/RAW", "type" : str,
                                      "optional" : False, "validate" : dataset,
                                      "attr" : "inputDataset", "null" : False},
                    "GlobalTag" : {"default" : "GT_DP_V1", "type" : str,
                                   "optional" : False, "validate" : None,
                                   "attr" : "globalTag", "null" : False},
                    "OpenRunningTimeout" : {"default" : 0, "type" : int,
                                            "optional" : True, "validate" : lambda x : x >= 0,
                                            "attr" : "openRunningTimeout", "null" : False},
                    "BlockBlacklist" : {"default" : [], "type" : makeList,
                                        "optional" : True, "validate" : lambda x: all([block(y) for y in x]),
                                        "attr" : "blockBlacklist", "null" : False},
                    "BlockWhitelist" : {"default" : [], "type" : makeList,
                                        "optional" : True, "validate" : lambda x: all([block(y) for y in x]),
                                        "attr" : "blockWhitelist", "null" : False},
                    "RunBlacklist" : {"default" : [], "type" : makeList,
                                      "optional" : True, "validate" : lambda x: all([int(y) > 0 for y in x]),
                                      "attr" : "runBlacklist", "null" : False},
                    "RunWhitelist" : {"default" : [], "type" : makeList,
                                      "optional" : True, "validate" : lambda x: all([int(y) > 0 for y in x]),
                                      "attr" : "runWhitelist", "null" : False},
                    "SplittingAlgo" : {"default" : "EventAwareLumiBased", "type" : str,
                                       "optional" : True, "validate" : lambda x: x in ["EventBased", "LumiBased",
                                                                                       "EventAwareLumiBased", "FileBased"],
                                       "attr" : "procJobSplitAlgo", "null" : False},
                    "EventsPerJob" : {"default" : None, "type" : int,
                                      "optional" : True, "validate" : lambda x : x > 0,
                                      "attr" : "eventsPerJob", "null" : False},
                    "LumisPerJob" : {"default" : 8, "type" : int,
                                     "optional" : True, "validate" : lambda x : x > 0,
                                     "attr" : "lumisPerJob", "null" : False},
                    "FilesPerJob" : {"default" : 1, "type" : int,
                                     "optional" : True, "validate" : lambda x : x > 0,
                                     "attr" : "filesPerJob", "null" : False}
                    }

        baseArgs.update(specArgs)
        StdBase.setDefaultArgumentsProperty(baseArgs)
        return baseArgs
