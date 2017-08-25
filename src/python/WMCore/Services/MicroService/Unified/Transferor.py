"""
File       : UnifiedTransferorManager.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: UnifiedTransferorManager class provides full functionality of the UnifiedTransferor service.
"""

# futures
from __future__ import print_function, division

# system modules
import re
import os
import json

# WMCore modules
from WMCore.Services.MicroService.Unified.Common import dbs_url, phedex_url, \
        reqmgr_url, cert, ckey, teraBytes, workflowMinInfo, \
        dbsInfo, phedexInfo, eventsLumisInfo

class UnifiedTransferorManager(object):
    """
    Initialize UnifiedTransferorManager class
    """
    def __init__(self, config=None):
        self.config = config

    def status(self):
        "Return current status about UnifiedTransferor"
        url = '%s/data/request' % reqmgr_url()
        workflows = getWorkflows(url)
        winfo = workflowsInfo(workflows)
        datasets = [d for row in winfo.values() for d in row['datasets']]
        print("### total number of workflows", len(winfo.keys()))
        print("### total number of datasets", len(datasets))
        # get all information about datasets/blocks we need to deal with
        datasetBlocks, datasetSizes = dbsInfo(datasets)
        blockNodes = phedexInfo(datasets)
        eventsLumis = eventsLumisInfo(workflows)

        for wflow, attrs in winfo.items():
            print("### %s" % wflow)
            ndatasets = len(attrs['datasets'])
            npileups = len(attrs['pileups'])
            nblocks = 0
            size = 0
            for dataset in attrs['datasets']:
                blocks = datasetBlocks[dataset]
                print(dataset)
                for blk in blocks:
                    print(blk, sorted(blockNodes[blk]))
                nblocks += len(blocks)
                size += datasetSizes[dataset]
            print("%s datasets, %s blocks, %s bytes, %s TB" % (ndatasets, nblocks, size, teraBytes(size)))
            print()
        sdict = {}
        return sdict

    def request(self, **kwargs):
        "Process request given to UnifiedTransferor"
        return {}
