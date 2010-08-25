#!/usr/bin/env python
"""
    WorkQueue.Policy.Start.Dataset tests
"""

__revision__ = "$Id: Dataset_t.py,v 1.5 2010/02/12 16:33:55 swakef Exp $"
__version__ = "$Revision: 1.5 $"

import unittest
import shutil
from WMCore.WorkQueue.Policy.Start.Dataset import Dataset
from WMCore_t.WMSpec_t.samples.Tier1ReRecoWorkload import workload as Tier1ReRecoWorkload
from WMCore_t.WMSpec_t.samples.Tier1ReRecoWorkload import workingDir
from WMCore_t.WMSpec_t.samples.MultiTaskProcessingWorkload import workload as MultiTaskProcessingWorkload
from WMCore_t.WorkQueue_t.MockDBSReader import MockDBSReader
shutil.rmtree(workingDir, ignore_errors = True)

class DatasetTestCase(unittest.TestCase):

    splitArgs = dict(SliceType = 'number_of_files', SliceSize = 10)

    def testTier1ReRecoWorkload(self):
        """Tier1 Re-reco workflow"""
        inputDataset = Tier1ReRecoWorkload.taskIterator().next().inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        dbs = {inputDataset.dbsurl : MockDBSReader(inputDataset.dbsurl, dataset)}
        for task in Tier1ReRecoWorkload.taskIterator():
            units = Dataset(**self.splitArgs)(Tier1ReRecoWorkload, task, dbs)
            self.assertEqual(1, len(units))
            for unit in units:
                self.assertEqual(2, unit['Jobs'])
                self.assertEqual(Tier1ReRecoWorkload, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
                self.assertEqual(unit['Data'], dataset)


    def testMultiTaskProcessingWorkload(self):
        """Multi Task Processing Workflow"""
        datasets = []
        tasks, count = 0, 0
        for task in MultiTaskProcessingWorkload.taskIterator():
            tasks += 1
            inputDataset = task.inputDataset()
            datasets.append("/%s/%s/%s" % (inputDataset.primary,
                                           inputDataset.processed,
                                           inputDataset.tier))
        dbs = {inputDataset.dbsurl : MockDBSReader(inputDataset.dbsurl, *datasets)}
        for task in MultiTaskProcessingWorkload.taskIterator():
            units = Dataset(**self.splitArgs)(MultiTaskProcessingWorkload, task, dbs)
            self.assertEqual(1, len(units))
            for unit in units:
                self.assertEqual(2, unit['Jobs'])
                self.assertEqual(MultiTaskProcessingWorkload, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
                self.assertEqual(unit['Data'], datasets[count])
            count += 1
        self.assertEqual(tasks, count)


if __name__ == '__main__':
    unittest.main()
