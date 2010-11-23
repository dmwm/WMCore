#!/usr/bin/env python
"""
    WorkQueue.Policy.Start.Dataset tests
"""




import unittest
import shutil
from WMCore.WorkQueue.Policy.Start.Dataset import Dataset
from WMCore.WMSpec.StdSpecs.ReReco import rerecoWorkload, getTestArguments
from WMQuality.Emulators.DBSClient.DBSReader \
    import DBSReader as MockDBSReader
from WMCore_t.WMSpec_t.samples.MultiTaskProcessingWorkload \
    import workload as MultiTaskProcessingWorkload

def getFirstTask(wmspec):
    """Return the 1st top level task"""
    # http://www.logilab.org/ticket/8774
    # pylint: disable-msg=E1101,E1103
    return wmspec.taskIterator().next()

rerecoArgs = getTestArguments()

class DatasetTestCase(unittest.TestCase):

    splitArgs = dict(SliceType = 'NumberOfFiles', SliceSize = 5)

    def testTier1ReRecoWorkload(self):
        """Tier1 Re-reco workflow"""
        Tier1ReRecoWorkload = rerecoWorkload('ReRecoWorkload', rerecoArgs)
        inputDataset = getFirstTask(Tier1ReRecoWorkload).inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        dbs = {inputDataset.dbsurl : MockDBSReader(inputDataset.dbsurl)}
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
        dbs = {inputDataset.dbsurl : MockDBSReader(inputDataset.dbsurl)}
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


    def testWhiteBlackLists(self):
        """Block/Run White/Black lists"""
        Tier1ReRecoWorkload = rerecoWorkload('ReRecoWorkload', rerecoArgs)
        inputDataset = getFirstTask(Tier1ReRecoWorkload).inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        dbs = {inputDataset.dbsurl : MockDBSReader(inputDataset.dbsurl)}

        # Block blacklist
        rerecoArgs2 = {'BlockBlacklist' : [dataset + '#1']}
        rerecoArgs2.update(rerecoArgs)
        blacklistBlockWorkload = rerecoWorkload('ReRecoWorkload',
                                                     rerecoArgs2)
        task = getFirstTask(blacklistBlockWorkload)
        units = Dataset(**self.splitArgs)(blacklistBlockWorkload, task, dbs)
        self.assertEqual(len(units), 1)
        self.assertEqual(units[0]['Jobs'], 1.0)

        # Block Whitelist
        rerecoArgs2['BlockWhitelist'] = [dataset + '#1']
        rerecoArgs2['BlockBlacklist'] = []
        blacklistBlockWorkload = rerecoWorkload('ReRecoWorkload',
                                                     rerecoArgs2)
        task = getFirstTask(blacklistBlockWorkload)
        units = Dataset(**self.splitArgs)(blacklistBlockWorkload, task, dbs)
        self.assertEqual(len(units), 1)
        self.assertEqual(units[0]['Jobs'], 1.0)

        # Block Mixed Whitelist
        rerecoArgs2['BlockWhitelist'] = [dataset + '#2']
        rerecoArgs2['BlockBlacklist'] = [dataset + '#1']
        blacklistBlockWorkload = rerecoWorkload('ReRecoWorkload',
                                                     rerecoArgs2)
        task = getFirstTask(blacklistBlockWorkload)
        units = Dataset(**self.splitArgs)(blacklistBlockWorkload, task, dbs)
        self.assertEqual(len(units), 1)
        self.assertEqual(units[0]['Jobs'], 1.0)

        # Run Whitelist
        rerecoArgs3 = {'RunWhitelist' : [1]}
        rerecoArgs3.update(rerecoArgs)
        blacklistBlockWorkload = rerecoWorkload('ReRecoWorkload',
                                                     rerecoArgs3)
        task = getFirstTask(blacklistBlockWorkload)
        units = Dataset(**self.splitArgs)(blacklistBlockWorkload, task, dbs)
        self.assertEqual(len(units), 1)
        self.assertEqual(units[0]['Data'], dataset)
        self.assertEqual(units[0]['Jobs'], 1.0)

        rerecoArgs3 = {'RunWhitelist' : [1 ,2]}
        rerecoArgs3.update(rerecoArgs)
        blacklistBlockWorkload = rerecoWorkload('ReRecoWorkload',
                                                     rerecoArgs3)
        task = getFirstTask(blacklistBlockWorkload)
        units = Dataset(**self.splitArgs)(blacklistBlockWorkload, task, dbs)
        self.assertEqual(len(units), 1)
        self.assertEqual(units[0]['Data'], dataset)
        self.assertEqual(units[0]['Jobs'], 2.0)

        # Run Blacklist
        rerecoArgs3 = {'RunBlacklist' : [2]}
        rerecoArgs3.update(rerecoArgs)
        blacklistBlockWorkload = rerecoWorkload('ReRecoWorkload',
                                                    rerecoArgs3)
        task = getFirstTask(blacklistBlockWorkload)
        units = Dataset(**self.splitArgs)(blacklistBlockWorkload, task, dbs)
        self.assertEqual(len(units), 1)
        self.assertEqual(units[0]['Data'], dataset)
        self.assertEqual(units[0]['Jobs'], 1.0)

        # Run Mixed Whitelist
        rerecoArgs3 = {'RunBlacklist' : [1], 'RunWhitelist' : [2]}
        rerecoArgs3.update(rerecoArgs)
        blacklistBlockWorkload = rerecoWorkload('ReRecoWorkload',
                                                     rerecoArgs3)
        task = getFirstTask(blacklistBlockWorkload)
        units = Dataset(**self.splitArgs)(blacklistBlockWorkload, task, dbs)
        self.assertEqual(len(units), 1)
        self.assertEqual(units[0]['Data'], dataset)
        self.assertEqual(units[0]['Jobs'], 1.0)


    def testDataDirectiveFromQueue(self):
        """Test data directive from queue"""
        Tier1ReRecoWorkload = rerecoWorkload('ReRecoWorkload', rerecoArgs)
        inputDataset = getFirstTask(Tier1ReRecoWorkload).inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        dbs = {inputDataset.dbsurl : MockDBSReader(inputDataset.dbsurl)}
        for task in Tier1ReRecoWorkload.taskIterator():
            Dataset(**self.splitArgs)(Tier1ReRecoWorkload, task, dbs, dataset)
            self.assertRaises(RuntimeError, Dataset(**self.splitArgs),
                              Tier1ReRecoWorkload, task, dbs, dataset + '1')

    def testLumiSplitTier1ReRecoWorkload(self):
        """Tier1 Re-reco workflow split by Lumi"""
        splitArgs = dict(SliceType = 'NumberOfLumis', SliceSize = 2)

        Tier1ReRecoWorkload = rerecoWorkload('ReRecoWorkload', rerecoArgs)
        inputDataset = getFirstTask(Tier1ReRecoWorkload).inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        dbs = {inputDataset.dbsurl : MockDBSReader(inputDataset.dbsurl)}
        for task in Tier1ReRecoWorkload.taskIterator():
            units = Dataset(**splitArgs)(Tier1ReRecoWorkload, task, dbs)
            self.assertEqual(1, len(units))
            for unit in units:
                self.assertEqual(2, unit['Jobs'])


if __name__ == '__main__':
    unittest.main()
