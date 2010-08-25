#!/usr/bin/env python
"""
_PileupFetcher_

Given a pileup dataset, pull the information required to cache the list
of pileup files in the job sandbox for the dataset.

"""


from WMCore.WMSpec.Steps.Fetchers.FetcherInterface import FetcherInterface
import WMCore.WMSpec.WMStep as WMStep

from WMCore.Services.DBS.DBSReader import DBSReader

class PileupFetcher(FetcherInterface):
    """
    _PileupFetcher_

    Pull dataset block/SE : LFN list from DBS for the
    pileup datasets required by the steps in the job.

    Archive these maps as files in the sandbox

    """

    def __call__(self, wmTask):
        """
        Trip through the input pileup datasets required by the
        task and pull the information from DBS.

        Stack the information in a file that maps block/SE to a list
        of LFNs at that site so that the available LFNs can be
        looked up on arrival at a site within the runtime environment

        """
        #1. Pull Pileup datasets from task
        #2. For each PU dataset
        #3.    Call dbsReader.listFileBlocks
        #4.    For each block:
        #5.         Call dbsreader.listFileBlockLocation(block)
        #6.         Call dbsreader.lfnsInBlock(block)
        #7.    Save Block/Location : LFN mapping in file in sandbox
        #8.    Record location of file in sandbox
        #         Save as python module to make it easy to load in job
        #TODO: Implement this
        raise NotImplementedError, "Mail Evans and tell him to write this"
