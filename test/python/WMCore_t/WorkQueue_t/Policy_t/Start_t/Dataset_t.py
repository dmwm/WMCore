#!/usr/bin/env python
"""
    WorkQueue.Policy.Start.Dataset tests
"""

import unittest
from WMCore.WorkQueue.Policy.Start.Dataset import Dataset
from WMCore.WMSpec.StdSpecs.ReReco import ReRecoWorkloadFactory
from WMCore.Services.EmulatorSwitch import EmulatorHelper
from WMCore.Services.DBS.DBSReader import DBSReader
from WMCore_t.WMSpec_t.samples.MultiTaskProcessingWorkload \
    import workload as MultiTaskProcessingWorkload
from WMCore.WorkQueue.WorkQueueExceptions import *
from WMCore_t.WorkQueue_t.WorkQueue_t import getFirstTask
from WMQuality.Emulators.DataBlockGenerator import Globals
from WMQuality.Emulators.WMSpecGenerator.WMSpecGenerator import createConfig

rerecoArgs = ReRecoWorkloadFactory.getTestArguments()
parentProcArgs = ReRecoWorkloadFactory.getTestArguments()
parentProcArgs.update(IncludeParents = "True")

class DatasetTestCase(unittest.TestCase):

    splitArgs = dict(SliceType = 'NumberOfFiles', SliceSize = 5)

    def setUp(self):
        Globals.GlobalParams.resetParams()
        EmulatorHelper.setEmulators(phedex = True, dbs = True,
                            siteDB = True, requestMgr = False)

    def tearDown(self):
        EmulatorHelper.resetEmulators()

    def testTier1ReRecoWorkload(self):
        """Tier1 Re-reco workflow"""
        rerecoArgs["ConfigCacheID"] = createConfig(rerecoArgs["CouchDBName"])
        factory = ReRecoWorkloadFactory()
        Tier1ReRecoWorkload = factory.factoryWorkloadConstruction('ReRecoWorkload', rerecoArgs)
        Tier1ReRecoWorkload.setStartPolicy('Dataset', **self.splitArgs)
        inputDataset = getFirstTask(Tier1ReRecoWorkload).inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        dbs = {inputDataset.dbsurl : DBSReader(inputDataset.dbsurl)}
        for task in Tier1ReRecoWorkload.taskIterator():
            units, _ = Dataset(**self.splitArgs)(Tier1ReRecoWorkload, task)
            self.assertEqual(1, len(units))
            for unit in units:
                self.assertEqual(4, unit['Jobs'])
                self.assertEqual(Tier1ReRecoWorkload, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
                self.assertEqual(unit['Inputs'].keys(), [dataset])
                self.assertEqual(8, unit['NumberOfLumis'])
                self.assertEqual(20, unit['NumberOfFiles'])
                self.assertEqual(20000, unit['NumberOfEvents'])

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
        dbs = {inputDataset.dbsurl : DBSReader(inputDataset.dbsurl)}
        for task in MultiTaskProcessingWorkload.taskIterator():
            units, _ = Dataset(**self.splitArgs)(MultiTaskProcessingWorkload, task)
            self.assertEqual(1, len(units))
            for unit in units:
                self.assertEqual(4, unit['Jobs'])
                self.assertEqual(MultiTaskProcessingWorkload, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
                self.assertEqual(unit['Inputs'].keys(), [datasets[count]])
            count += 1
        self.assertEqual(tasks, count)


    def testWhiteBlackLists(self):
        """Block/Run White/Black lists"""
        rerecoArgs["ConfigCacheID"] = createConfig(rerecoArgs["CouchDBName"])
        factory = ReRecoWorkloadFactory()
        Tier1ReRecoWorkload = factory.factoryWorkloadConstruction('ReRecoWorkload', rerecoArgs)
        Tier1ReRecoWorkload.setStartPolicy('Dataset', **self.splitArgs)
        inputDataset = getFirstTask(Tier1ReRecoWorkload).inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        dbs = {inputDataset.dbsurl : DBSReader(inputDataset.dbsurl)}

        # Block blacklist
        rerecoArgs2 = {'BlockBlacklist' : [dataset + '#1']}
        rerecoArgs2.update(rerecoArgs)
        blacklistBlockWorkload = factory.factoryWorkloadConstruction('ReRecoWorkload',
                                                     rerecoArgs2)
        blacklistBlockWorkload.setStartPolicy('Dataset', **self.splitArgs)
        task = getFirstTask(blacklistBlockWorkload)
        units, _ = Dataset(**self.splitArgs)(blacklistBlockWorkload, task)
        self.assertEqual(len(units), 1)
        self.assertEqual(units[0]['Jobs'], 2.0)
        self.assertEqual(4, units[0]['NumberOfLumis'])
        self.assertEqual(10, units[0]['NumberOfFiles'])
        self.assertEqual(10000, units[0]['NumberOfEvents'])

        # Block Whitelist
        rerecoArgs2['BlockWhitelist'] = [dataset + '#1']
        rerecoArgs2['BlockBlacklist'] = []
        blacklistBlockWorkload = factory.factoryWorkloadConstruction('ReRecoWorkload',
                                                     rerecoArgs2)
        blacklistBlockWorkload.setStartPolicy('Dataset', **self.splitArgs)
        task = getFirstTask(blacklistBlockWorkload)
        units, _ = Dataset(**self.splitArgs)(blacklistBlockWorkload, task)
        self.assertEqual(len(units), 1)
        self.assertEqual(units[0]['Jobs'], 2.0)
        self.assertEqual(4, units[0]['NumberOfLumis'])
        self.assertEqual(10, units[0]['NumberOfFiles'])
        self.assertEqual(10000, units[0]['NumberOfEvents'])

        # Block Mixed Whitelist
        rerecoArgs2['BlockWhitelist'] = [dataset + '#2']
        rerecoArgs2['BlockBlacklist'] = [dataset + '#1']
        blacklistBlockWorkload = factory.factoryWorkloadConstruction('ReRecoWorkload',
                                                     rerecoArgs2)
        blacklistBlockWorkload.setStartPolicy('Dataset', **self.splitArgs)
        task = getFirstTask(blacklistBlockWorkload)
        units, _ = Dataset(**self.splitArgs)(blacklistBlockWorkload, task)
        self.assertEqual(len(units), 1)
        self.assertEqual(units[0]['Jobs'], 2.0)
        self.assertEqual(4, units[0]['NumberOfLumis'])
        self.assertEqual(10, units[0]['NumberOfFiles'])
        self.assertEqual(10000, units[0]['NumberOfEvents'])

        # Run Whitelist
        rerecoArgs3 = {'RunWhitelist' : [1]}
        rerecoArgs3.update(rerecoArgs)
        blacklistBlockWorkload = factory.factoryWorkloadConstruction('ReRecoWorkload',
                                                     rerecoArgs3)
        blacklistBlockWorkload.setStartPolicy('Dataset', **self.splitArgs)
        task = getFirstTask(blacklistBlockWorkload)
        units, _ = Dataset(**self.splitArgs)(blacklistBlockWorkload, task)
        self.assertEqual(len(units), 1)
        self.assertEqual(units[0]['Inputs'].keys(), [dataset])
        self.assertEqual(units[0]['Jobs'], 2.0)
        self.assertEqual(10, units[0]['NumberOfLumis'])
        self.assertEqual(10, units[0]['NumberOfFiles'])
        self.assertEqual(10000, units[0]['NumberOfEvents'])

        rerecoArgs3 = {'RunWhitelist' : [1 ,2]}
        rerecoArgs3.update(rerecoArgs)
        blacklistBlockWorkload = factory.factoryWorkloadConstruction('ReRecoWorkload',
                                                     rerecoArgs3)
        blacklistBlockWorkload.setStartPolicy('Dataset', **self.splitArgs)
        task = getFirstTask(blacklistBlockWorkload)
        units, _ = Dataset(**self.splitArgs)(blacklistBlockWorkload, task)
        self.assertEqual(len(units), 1)
        self.assertEqual(units[0]['Inputs'].keys(), [dataset])
        self.assertEqual(units[0]['Jobs'], 4.0)
        self.assertEqual(20, units[0]['NumberOfLumis'])
        self.assertEqual(20, units[0]['NumberOfFiles'])
        self.assertEqual(20000, units[0]['NumberOfEvents'])

        # Run Blacklist
        rerecoArgs3 = {'RunBlacklist' : [2]}
        rerecoArgs3.update(rerecoArgs)
        blacklistBlockWorkload = factory.factoryWorkloadConstruction('ReRecoWorkload',
                                                    rerecoArgs3)
        blacklistBlockWorkload.setStartPolicy('Dataset', **self.splitArgs)
        task = getFirstTask(blacklistBlockWorkload)
        units, _ = Dataset(**self.splitArgs)(blacklistBlockWorkload, task)
        self.assertEqual(len(units), 1)
        self.assertEqual(units[0]['Inputs'].keys(), [dataset])
        self.assertEqual(units[0]['Jobs'], 2.0)
        self.assertEqual(10, units[0]['NumberOfLumis'])
        self.assertEqual(10, units[0]['NumberOfFiles'])
        self.assertEqual(10000, units[0]['NumberOfEvents'])

        # Run Mixed Whitelist
        rerecoArgs3 = {'RunBlacklist' : [1], 'RunWhitelist' : [2]}
        rerecoArgs3.update(rerecoArgs)
        blacklistBlockWorkload = factory.factoryWorkloadConstruction('ReRecoWorkload',
                                                     rerecoArgs3)
        blacklistBlockWorkload.setStartPolicy('Dataset', **self.splitArgs)
        task = getFirstTask(blacklistBlockWorkload)
        units, _ = Dataset(**self.splitArgs)(blacklistBlockWorkload, task)
        self.assertEqual(len(units), 1)
        self.assertEqual(units[0]['Inputs'].keys(), [dataset])
        self.assertEqual(units[0]['Jobs'], 2.0)
        self.assertEqual(10, units[0]['NumberOfLumis'])
        self.assertEqual(10, units[0]['NumberOfFiles'])
        self.assertEqual(10000, units[0]['NumberOfEvents'])


    def testDataDirectiveFromQueue(self):
        """Test data directive from queue"""
        rerecoArgs["ConfigCacheID"] = createConfig(rerecoArgs["CouchDBName"])
        factory = ReRecoWorkloadFactory()
        Tier1ReRecoWorkload = factory.factoryWorkloadConstruction('ReRecoWorkload', rerecoArgs)
        inputDataset = getFirstTask(Tier1ReRecoWorkload).inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        dbs = {inputDataset.dbsurl : DBSReader(inputDataset.dbsurl)}
        for task in Tier1ReRecoWorkload.taskIterator():
            Dataset(**self.splitArgs)(Tier1ReRecoWorkload, task, {dataset : []})
            self.assertRaises(RuntimeError, Dataset(**self.splitArgs),
                              Tier1ReRecoWorkload, task, dbs, {dataset + '1': []})

    def testLumiSplitTier1ReRecoWorkload(self):
        """Tier1 Re-reco workflow split by Lumi"""
        splitArgs = dict(SliceType = 'NumberOfLumis', SliceSize = 2)
        rerecoArgs["ConfigCacheID"] = createConfig(rerecoArgs["CouchDBName"])
        factory = ReRecoWorkloadFactory()
        Tier1ReRecoWorkload = factory.factoryWorkloadConstruction('ReRecoWorkload', rerecoArgs)
        Tier1ReRecoWorkload.setStartPolicy('Dataset', **splitArgs)
        inputDataset = getFirstTask(Tier1ReRecoWorkload).inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        dbs = {inputDataset.dbsurl : DBSReader(inputDataset.dbsurl)}
        for task in Tier1ReRecoWorkload.taskIterator():
            units, _ = Dataset(**splitArgs)(Tier1ReRecoWorkload, task)
            self.assertEqual(1, len(units))
            for unit in units:
                self.assertEqual(4, unit['Jobs'])

    def testRunWhitelist(self):
        """ReReco lumi split with Run whitelist"""
        # get files with multiple runs
        Globals.GlobalParams.setNumOfRunsPerFile(2)
        # a large number of lumis to ensure we get multiple runs
        Globals.GlobalParams.setNumOfLumisPerBlock(10)
        splitArgs = dict(SliceType = 'NumberOfLumis', SliceSize = 1)
        rerecoArgs["ConfigCacheID"] = createConfig(rerecoArgs["CouchDBName"])
        factory = ReRecoWorkloadFactory()
        Tier1ReRecoWorkload = factory.factoryWorkloadConstruction('ReRecoWorkload', rerecoArgs)
        Tier1ReRecoWorkload.setRunWhitelist([2, 3])
        Tier1ReRecoWorkload.setStartPolicy('Dataset', **splitArgs)
        inputDataset = getFirstTask(Tier1ReRecoWorkload).inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        dbs = {inputDataset.dbsurl : DBSReader(inputDataset.dbsurl)}
        for task in Tier1ReRecoWorkload.taskIterator():
            units, _ = Dataset(**splitArgs)(Tier1ReRecoWorkload, task)
            self.assertEqual(1, len(units))
            # Check number of jobs in element match number for
            # dataset in run whitelist
            jobs = 0
            wq_jobs = 0
            for unit in units:
                wq_jobs += unit['Jobs']
                runLumis = dbs[inputDataset.dbsurl].listRunLumis(dataset = unit['Inputs'].keys()[0])
                for run in runLumis:
                    if run in getFirstTask(Tier1ReRecoWorkload).inputRunWhitelist():
                        jobs += runLumis[run]
            self.assertEqual(int(jobs / splitArgs['SliceSize'] ) , int(wq_jobs))

    def testInvalidSpecs(self):
        """Specs with no work"""
        # no dataset
        rerecoArgs["ConfigCacheID"] = createConfig(rerecoArgs["CouchDBName"])
        factory = ReRecoWorkloadFactory()
        processingSpec = factory.factoryWorkloadConstruction('testProcessingInvalid', rerecoArgs)
        getFirstTask(processingSpec).data.input.dataset = None
        for task in processingSpec.taskIterator():
            self.assertRaises(WorkQueueWMSpecError, Dataset(), processingSpec, task)

        # invalid dbs url
        processingSpec = factory.factoryWorkloadConstruction('testProcessingInvalid', rerecoArgs)
        getFirstTask(processingSpec).data.input.dataset.dbsurl = 'wrongprot://dbs.example.com'
        for task in processingSpec.taskIterator():
            self.assertRaises(WorkQueueWMSpecError, Dataset(), processingSpec, task)

        # invalid dataset name
        processingSpec = factory.factoryWorkloadConstruction('testProcessingInvalid', rerecoArgs)
        getFirstTask(processingSpec).data.input.dataset.primary = Globals.NOT_EXIST_DATASET
        for task in processingSpec.taskIterator():
            self.assertRaises(WorkQueueNoWorkError, Dataset(), processingSpec, task)

        # invalid run whitelist
        processingSpec = factory.factoryWorkloadConstruction('testProcessingInvalid', rerecoArgs)
        processingSpec.setRunWhitelist([666]) # not in this dataset
        for task in processingSpec.taskIterator():
            self.assertRaises(WorkQueueNoWorkError, Dataset(), processingSpec, task)

    def testParentProcessing(self):
        """
        test parent processing: should have the same results as rereco test
        with the parent flag and dataset.
        """
        parentProcArgs["ConfigCacheID"] = createConfig(rerecoArgs["CouchDBName"])
        factory = ReRecoWorkloadFactory()
        parentProcSpec = factory.factoryWorkloadConstruction('testParentProcessing', parentProcArgs)
        parentProcSpec.setStartPolicy('Dataset', **self.splitArgs)
        inputDataset = getFirstTask(parentProcSpec).inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        dbs = {inputDataset.dbsurl : DBSReader(inputDataset.dbsurl)}
        for task in parentProcSpec.taskIterator():
            units, _ = Dataset(**self.splitArgs)(parentProcSpec, task)
            self.assertEqual(1, len(units))
            for unit in units:
                self.assertEqual(4, unit['Jobs'])
                self.assertEqual(parentProcSpec, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
                self.assertEqual(unit['Inputs'].keys(), [dataset])
                self.assertEqual(True, unit['ParentFlag'])
                self.assertEqual(0, len(unit['ParentData']))

if __name__ == '__main__':
    unittest.main()
