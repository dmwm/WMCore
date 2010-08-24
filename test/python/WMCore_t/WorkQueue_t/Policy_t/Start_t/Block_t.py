#!/usr/bin/env python
"""
    WorkQueue.Policy.Start.Block tests
"""




import unittest
import shutil
from WMCore.WorkQueue.Policy.Start.Block import Block
#from WMCore.WMSpec.StdSpecs.ReReco import rerecoWorkload
from WMCore_t.WorkQueue_t.WorkQueue_t import TestReRecoFactory, rerecoArgs
from WMCore_t.WorkQueue_t.WorkQueue_t import getFirstTask
from WMCore_t.WMSpec_t.samples.MultiTaskProcessingWorkload import workload as MultiTaskProcessingWorkload
from WMCore_t.WorkQueue_t.MockDBSReader import MockDBSReader

class BlockTestCase(unittest.TestCase):

    splitArgs = dict(SliceType = 'NumberOfFiles', SliceSize = 10)

    def testTier1ReRecoWorkload(self):
        """Tier1 Re-reco workflow"""
        Tier1ReRecoWorkload = TestReRecoFactory()('ReRecoWorkload', rerecoArgs)
        inputDataset = getFirstTask(Tier1ReRecoWorkload).inputDataset()
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
                self.assertEqual(Tier1ReRecoWorkload, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
            self.assertEqual(len(units),
                             len(dbs[inputDataset.dbsurl].blocks[dataset]))


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
            units = Block(**self.splitArgs)(MultiTaskProcessingWorkload, task, dbs)
            self.assertEqual(2, len(units))
            blocks = [] # fill with blocks as we get work units for them
            for unit in units:
                self.assertEqual(1, unit['Jobs'])
                self.assertEqual(MultiTaskProcessingWorkload, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
            self.assertEqual(len(units),
                             len(dbs[inputDataset.dbsurl].blocks[datasets[0]]))
            count += 1
        self.assertEqual(tasks, count)


    def testWhiteBlackLists(self):
        """Block/Run White/Black lists"""
        Tier1ReRecoWorkload = TestReRecoFactory()('ReRecoWorkload', rerecoArgs)
        inputDataset = getFirstTask(Tier1ReRecoWorkload).inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        dbs = {inputDataset.dbsurl : MockDBSReader(inputDataset.dbsurl, dataset)}

        # Block blacklist
        rerecoArgs2 = {'BlockBlacklist' : [dataset + '#1']}
        rerecoArgs2.update(rerecoArgs)
        blacklistBlockWorkload = TestReRecoFactory()('ReRecoWorkload',
                                              rerecoArgs2)
        task = getFirstTask(blacklistBlockWorkload)
        units = Block(**self.splitArgs)(blacklistBlockWorkload, task, dbs)
        self.assertEqual(len(units), 1)
        self.assertNotEqual(units[0]['Data'], rerecoArgs2['BlockBlacklist'][0])

        # Block Whitelist
        rerecoArgs2['BlockWhitelist'] = [dataset + '#1']
        rerecoArgs2['BlockBlacklist'] = []
        blacklistBlockWorkload = TestReRecoFactory()('ReRecoWorkload',
                                                     rerecoArgs2)
        task = getFirstTask(blacklistBlockWorkload)
        units = Block(**self.splitArgs)(blacklistBlockWorkload, task, dbs)
        self.assertEqual(len(units), 1)
        self.assertEqual(units[0]['Data'], rerecoArgs2['BlockWhitelist'][0])

        # Block Mixed Whitelist
        rerecoArgs2['BlockWhitelist'] = [dataset + '#2']
        rerecoArgs2['BlockBlacklist'] = [dataset + '#1']
        blacklistBlockWorkload = TestReRecoFactory()('ReRecoWorkload',
                                                     rerecoArgs2)
        task = getFirstTask(blacklistBlockWorkload)
        units = Block(**self.splitArgs)(blacklistBlockWorkload, task, dbs)
        self.assertEqual(len(units), 1)
        self.assertEqual(units[0]['Data'], rerecoArgs2['BlockWhitelist'][0])

        # Run Whitelist
        rerecoArgs3 = {'RunWhitelist' : [1]}
        rerecoArgs3.update(rerecoArgs)
        blacklistBlockWorkload = TestReRecoFactory()('ReRecoWorkload',
                                                     rerecoArgs3)
        task = getFirstTask(blacklistBlockWorkload)
        units = Block(**self.splitArgs)(blacklistBlockWorkload, task, dbs)
        self.assertEqual(len(units), 1)
        self.assertEqual(units[0]['Data'], dataset + '#1')

        # Run Blacklist
        rerecoArgs3 = {'RunBlacklist' : [2, 3]}
        rerecoArgs3.update(rerecoArgs)
        blacklistBlockWorkload = TestReRecoFactory()('ReRecoWorkload',
                                                    rerecoArgs3)
        task = getFirstTask(blacklistBlockWorkload)
        units = Block(**self.splitArgs)(blacklistBlockWorkload, task, dbs)
        self.assertEqual(len(units), 1)
        self.assertEqual(units[0]['Data'], dataset + '#1')

        # Run Mixed Whitelist
        rerecoArgs3 = {'RunBlacklist' : [1], 'RunWhitelist' : [3]}
        rerecoArgs3.update(rerecoArgs)
        blacklistBlockWorkload = TestReRecoFactory()('ReRecoWorkload',
                                                     rerecoArgs3)
        task = getFirstTask(blacklistBlockWorkload)
        units = Block(**self.splitArgs)(blacklistBlockWorkload, task, dbs)
        self.assertEqual(len(units), 1)
        self.assertEqual(units[0]['Data'], dataset + '#2')


    def testDataDirectiveFromQueue(self):
        """Test data directive from queue"""
        Tier1ReRecoWorkload = TestReRecoFactory()('ReRecoWorkload', rerecoArgs)
        inputDataset = getFirstTask(Tier1ReRecoWorkload).inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        dbs = {inputDataset.dbsurl : MockDBSReader(inputDataset.dbsurl, dataset)}
        for task in Tier1ReRecoWorkload.taskIterator():
            # Take dataset and force to run over only 1 block
            units = Block(**self.splitArgs)(Tier1ReRecoWorkload, task,
                                            dbs, dataset + '#1')
            self.assertEqual(1, len(units))
            blocks = [] # fill with blocks as we get work units for them
            for unit in units:
                self.assertEqual(1, unit['Jobs'])
                self.assertEqual(Tier1ReRecoWorkload, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
            self.assertNotEqual(len(units),
                             len(dbs[inputDataset.dbsurl].blocks[dataset]))

if __name__ == '__main__':
    unittest.main()
