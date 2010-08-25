#!/usr/bin/env python
"""
WorkQueue splitting by block

"""
__all__ = []
__revision__ = "$Id: Block.py,v 1.11 2010/06/11 19:38:21 sryu Exp $"
__version__ = "$Revision: 1.11 $"

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
            parents = []
            if self.initialTask.parentProcessingFlag():
                parents = block['Parents']
                if not parents:
                    msg = "Parentage required but no parents found for %s"
                    raise RuntimeError, msg % block['Name']

            self.newQueueElement(Data = block['Name'],
                                 ParentData = parents,
                                 Jobs = ceil(float(block[self.args['SliceType']]) /
                                             float(self.args['SliceSize'])/
                                             float(self.args['Multiplier'])
                                            )
                                 )
                                 #Jobs = block[self.args['SliceType']])


    def validate(self):
        """Check args and spec work with block splitting"""
        pass
