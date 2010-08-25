#!/usr/bin/env python
"""
    WorkQueue.Policy.Start.Block tests
"""

__revision__ = "$Id: Block_t.py,v 1.3 2010/01/04 16:15:14 swakef Exp $"
__version__ = "$Revision: 1.3 $"

import unittest
import shutil
from WMCore.WorkQueue.Policy.Start.Block import Block
from WMCore_t.WMSpec_t.samples.Tier1ReRecoWorkload import workload as Tier1ReRecoWorkload
from WMCore_t.WMSpec_t.samples.Tier1ReRecoWorkload import workingDir
from WMCore_t.WorkQueue_t.MockDBSReader import MockDBSReader
shutil.rmtree(workingDir, ignore_errors = True)

class BlockTestCase(unittest.TestCase):

    splitArgs = dict(SliceType = 'NumFiles', SliceSize = 10)

    def testTier1ReRecoWorkload(self):
        """Tier1 Re-reco workflow"""
        inputDataset = Tier1ReRecoWorkload.taskIterator().next().inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        dbs = {inputDataset.dbsurl : MockDBSReader(inputDataset.dbsurl, dataset)}
        for task in Tier1ReRecoWorkload.taskIterator():
            units = Block(**self.splitArgs)(Tier1ReRecoWorkload, task, dbs)
            self.assertEqual(2, len(units))
            blocks = [] # fill with blocks as we get work units for them
            for unit in units:
                self.assertEqual(1, unit['Jobs'])
                spec = unit['WMSpec']
                # ensure new spec object created for each work unit
                self.assertNotEqual(id(spec), id(Tier1ReRecoWorkload))
                initialTask = spec.taskIterator().next()
                block = initialTask.inputDataset().blocks.whitelist
                self.assertEqual(1, len(block))
                block = block[0]
                self.assertEqual(block, unit['Data'])
                self.assertTrue(block.find(dataset + '#') > -1)
                self.assertFalse(block in blocks)
                blocks.append(block)
            self.assertEqual(len(blocks),
                             len(dbs[inputDataset.dbsurl].blocks[dataset]))


if __name__ == '__main__':
    unittest.main()
