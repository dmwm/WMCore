#!/usr/bin/env python
"""
WorkQueue splitting by block

"""
__all__ = []
__revision__ = "$Id: Block.py,v 1.13 2010/07/14 16:27:09 swakef Exp $"
__version__ = "$Revision: 1.13 $"

from WMCore.WorkQueue.Policy.Start.StartPolicyInterface import StartPolicyInterface
from copy import deepcopy
from math import ceil

class Block(StartPolicyInterface):
    """Split elements into blocks"""
    def __init__(self, **args):
        StartPolicyInterface.__init__(self, **args)
        self.args.setdefault('SliceType', 'NumberOfFiles')
        self.args.setdefault('SliceSize', 1)

    def split(self):
        """Apply policy to spec"""

        dbs = self.dbs()
        inputDataset = self.initialTask.inputDataset()
        datasetPath = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        for block in dbs.getFileBlocksInfo(datasetPath):

            # apply input dataset restrictions
            blockWhiteList = inputDataset.blocks.whitelist
            blockBlackList = inputDataset.blocks.blacklist
            if blockWhiteList and block['Name'] not in blockWhiteList:
                continue
            if block['Name'] in blockBlackList:
                continue

            parents = []
            if self.initialTask.parentProcessingFlag():
                parents = block['Parents']
                if not parents:
                    msg = "Parentage required but no parents found for %s"
                    raise RuntimeError, msg % block['Name']

            self.newQueueElement(Data = block['Name'],
                                 ParentData = parents,
                                 Jobs = ceil(float(block[self.args['SliceType']]) /
                                             float(self.args['SliceSize']))
                                 )
                                 #Jobs = block[self.args['SliceType']])


    def validate(self):
        """Check args and spec work with block splitting"""
        pass
