#!/usr/bin/env python
"""
WorkQueue splitting by dataset

"""
__all__ = []
__revision__ = "$Id: Dataset.py,v 1.2 2009/12/07 14:13:46 swakef Exp $"
__version__ = "$Revision: 1.2 $"

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

        self.newQueueElement(Data = dataset['path'],
                             WMSpec = self.wmspec,
                             Jobs = ceil(dataset[self.args['SliceType']] /
                                                float(self.args['SliceSize'])))
                             #Jobs = dataset[self.args['SliceType']])


    def validate(self):
        """Check args and spec work with block splitting"""
        pass
