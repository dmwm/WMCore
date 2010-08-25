#!/usr/bin/env python
"""
WorkQueue splitting by block

"""
__all__ = []
__revision__ = "$Id: Block.py,v 1.6 2010/02/11 17:57:00 sryu Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.WorkQueue.Policy.Start.StartPolicyInterface import StartPolicyInterface
from copy import deepcopy
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
        inputDataset = self.initialTask.inputDataset()
        datasetPath = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        for block in dbs.getFileBlocksInfo(datasetPath):
            parents = []
            if self.initialTask.parentProcessingFlag():
                parents = block['Parents']
                if not parents:
                    msg = "Parentage required but no parents found for %s"
                    raise RuntimeError, msg % block['Name']

            self.newQueueElement(Data = block['Name'],
                                 ParentData = parents,
                                 WMSpec = self.wmspec,
                                 Jobs = ceil(block[self.args['SliceType']] /
                                                float(self.args['SliceSize'])))
                                 #Jobs = block[self.args['SliceType']])


    def validate(self):
        """Check args and spec work with block splitting"""
        pass
