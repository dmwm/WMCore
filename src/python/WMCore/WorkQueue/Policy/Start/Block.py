#!/usr/bin/env python
"""
WorkQueue splitting by block

"""
__all__ = []
__revision__ = "$Id: Block.py,v 1.1 2009/12/02 13:52:43 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Policy.Start.StartPolicyInterface import StartPolicyInterface
from copy import copy
from math import ceil

class Block(StartPolicyInterface):
    """Split elements into blocks"""
    def __init__(self, **args):
        StartPolicyInterface.__init__(self, **args)
        self.args.setdefault('SliceType', 'NumFiles')
        self.args.setdefault('SliceSize', 10)


    def split(self):
        """Apply policy to spec"""
        dbs = self.dbs()
        #TODO: Handle block restrictions
        inputDataset = self.initialTask.inputDataset()
        datasetPath = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        #TODO: Handle block restrictions
        for block in dbs.getFileBlocksInfo(datasetPath):
            parents = []
            if self.initialTask.parentProcessingFlag():
                parents = block['Parents']
                if not parents:
                    msg = "Parentage required but no parents found for %s"
                    raise RuntimeError, msg % block['Name']

            # copy spec file restricting block
            spec = copy(self.wmspec)
            spec.taskIterator().next().data.input.dataset.blocks.whitelist = block['Name']
            self.newQueueElement(Data = block['Name'],
                                 ParentData = parents,
                                 WMSpec = spec,
                                 Jobs = ceil(block[self.args['SliceType']] /
                                                float(self.args['SliceSize'])))
                                 #Jobs = block[self.args['SliceType']])


    def validate(self):
        """Check args and spec work with block splitting"""
        pass
