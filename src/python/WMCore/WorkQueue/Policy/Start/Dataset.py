#!/usr/bin/env python
"""
WorkQueue splitting by dataset

"""
__all__ = []
__revision__ = "$Id: Dataset.py,v 1.8 2010/06/11 16:34:07 sryu Exp $"
__version__ = "$Revision: 1.8 $"

from WMCore.WorkQueue.Policy.Start.StartPolicyInterface import StartPolicyInterface
from math import ceil

class Dataset(StartPolicyInterface):
    """Split elements into datasets"""
    def __init__(self, **args):
        StartPolicyInterface.__init__(self, **args)
        self.args.setdefault('SliceType', 'number_of_files')
        self.args.setdefault('SliceSize', 1)
        # define how many more works to retrieve from the queue
        # i.e. if Multiplier is set to 1000 it will pull down 
        # 1000 times more jobs than available slot
        self.args.setdefault('Multiplier', 1000)

    def split(self):
        """Apply policy to spec"""
        dbs = self.dbs()
        #TODO: Handle block restrictions
        inputDataset = self.initialTask.inputDataset()
        datasetPath = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        dataset = dbs.getDatasetInfo(datasetPath)

        # parentage
        if self.initialTask.parentProcessingFlag():
            parents = dataset['Parents']
            if not parents:
                # Real data lacks dataset parentage - work with block parentage
                blocks = dbs.getFileBlocksInfo(datasetPath)
                for block in blocks:
                    parents.extend(block['Parents'])
            if not parents:
                msg = "Parentage required but no parents found for %s"
                raise RuntimeError, msg % datasetPath
        else:
            parents = []

        self.newQueueElement(Data = dataset['path'],
                             ParentData = parents,
                             Jobs = ceil(float(dataset[self.args['SliceType']]) /
                                         float(self.args['SliceSize']) /
                                         float(self.args['Multiplier'])
                                         )
                             )
                             #Jobs = dataset[self.args['SliceType']])


    def validate(self):
        """Check args and spec work with block splitting"""
        pass
