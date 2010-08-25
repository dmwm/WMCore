#!/usr/bin/env python
"""
WorkQueue splitting by dataset

"""
__all__ = []
__revision__ = "$Id: Dataset.py,v 1.12 2010/07/14 16:27:09 swakef Exp $"
__version__ = "$Revision: 1.12 $"

from WMCore.WorkQueue.Policy.Start.StartPolicyInterface import StartPolicyInterface
from math import ceil

class Dataset(StartPolicyInterface):
    """Split elements into datasets"""
    def __init__(self, **args):
        StartPolicyInterface.__init__(self, **args)
        self.args.setdefault('SliceType', 'NumberOfFiles')
        self.args.setdefault('SliceSize', 1)
        
    def split(self):
        """Apply policy to spec"""
        dbs = self.dbs()
        work = 0
        validblocks = []
        inputDataset = self.initialTask.inputDataset()
        datasetPath = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        dataset = dbs.getDatasetInfo(datasetPath)

        # apply input dataset restrictions
        blockWhiteList = inputDataset.blocks.whitelist
        blockBlackList = inputDataset.blocks.blacklist
        if blockWhiteList or blockBlackList:
            blocks = dbs.getFileBlocksInfo(datasetPath)
            for block in blocks:
                if blockWhiteList and block['Name'] not in blockWhiteList:
                    continue
                if block['Name'] in blockBlackList:
                    continue

                work += block[self.args['SliceType']]
                validblocks.append(block)
            if not validblocks:
                raise RuntimeError, 'No blocks pass white/blacklist'

        # parentage
        if self.initialTask.parentProcessingFlag():
            parents = dataset['Parents']
            if not parents:
                # Real data lacks dataset parentage - work with block parentage
                if not validblocks:
                    validblocks = dbs.getFileBlocksInfo(datasetPath)
                for block in validblocks:
                    parents.extend(block['Parents'])
            if not parents:
                msg = "Parentage required but no parents found for %s"
                raise RuntimeError, msg % datasetPath
        else:
            parents = []

        if not work:
            work = dataset[self.args['SliceType']]
        self.newQueueElement(Data = dataset['path'],
                             ParentData = parents,
                             Jobs = ceil(float(work) /
                                         float(self.args['SliceSize']))
                             )
                             #Jobs = dataset[self.args['SliceType']])


    def validate(self):
        """Check args and spec work with block splitting"""
        pass
