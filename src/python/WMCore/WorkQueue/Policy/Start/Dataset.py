#!/usr/bin/env python
"""
WorkQueue splitting by dataset

"""
__all__ = []
__revision__ = "$Id: Dataset.py,v 1.7 2010/05/13 14:00:24 swakef Exp $"
__version__ = "$Revision: 1.7 $"

from WMCore.WorkQueue.Policy.Start.StartPolicyInterface import StartPolicyInterface
from math import ceil

class Dataset(StartPolicyInterface):
    """Split elements into datasets"""
    def __init__(self, **args):
        StartPolicyInterface.__init__(self, **args)
        self.args.setdefault('SliceType', 'number_of_files')
        self.args.setdefault('SliceSize', 100)


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
                                                float(self.args['SliceSize'])))
                             #Jobs = dataset[self.args['SliceType']])


    def validate(self):
        """Check args and spec work with block splitting"""
        pass
