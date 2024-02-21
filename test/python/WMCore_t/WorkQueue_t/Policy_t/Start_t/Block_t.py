#!/usr/bin/env python
"""
    WorkQueue.Policy.Start.Block tests
"""

from future.utils import viewitems, listvalues

import unittest

from WMCore_t.WMSpec_t.samples.MultiTaskProcessingWorkload import workload as MultiTaskProcessingWorkload
from WMCore_t.WorkQueue_t.WorkQueue_t import getFirstTask

from Utils.PythonVersion import PY3

from WMCore.DataStructs.LumiList import LumiList
from WMCore.Services.DBS.DBSErrors import DBSReaderError
from WMCore.Services.DBS.DBSReader import DBSReader
from WMCore.WMSpec.StdSpecs.ReReco import ReRecoWorkloadFactory
from WMCore.WorkQueue.Policy.Start.Block import Block
from WMCore.WorkQueue.WorkQueueExceptions import (WorkQueueWMSpecError, WorkQueueNoWorkError)
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMQuality.Emulators.WMSpecGenerator.WMSpecGenerator import createConfig

rerecoArgs = ReRecoWorkloadFactory.getTestArguments()
rerecoArgs["SplittingAlgo"] = "LumiBased"
rerecoArgs["LumisPerJob"] = 8
rerecoArgs["ConfigCacheID"] = createConfig(rerecoArgs["CouchDBName"])

parentProcArgs = ReRecoWorkloadFactory.getTestArguments()
parentProcArgs.update(IncludeParents="True")
parentProcArgs["SplittingAlgo"] = "LumiBased"
parentProcArgs["LumisPerJob"] = 8


def rerecoWorkload(workloadName, arguments, assignArgs=None):
    factory = ReRecoWorkloadFactory()
    wmspec = factory.factoryWorkloadConstruction(workloadName, arguments)
    if assignArgs:
        args = factory.getAssignTestArguments()
        args.update(assignArgs)
        wmspec.updateArguments(args)
    return wmspec


class BlockTestCase(EmulatedUnitTestCase):
    splitArgs = dict(SliceType='NumberOfFiles', SliceSize=10)

    def setUp(self):
        super(BlockTestCase, self).setUp()
        if PY3:
            self.assertItemsEqual = self.assertCountEqual

    def tearDown(self):
        super(BlockTestCase, self).tearDown()

    def testTier1ReRecoWorkload(self):
        """Tier1 Re-reco workflow"""
        Tier1ReRecoWorkload = rerecoWorkload('ReRecoWorkload', rerecoArgs,
                                             assignArgs={'SiteWhitelist': ['T2_XX_SiteA']})
        Tier1ReRecoWorkload.data.request.priority = 69
        inputDataset = getFirstTask(Tier1ReRecoWorkload).inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                 inputDataset.processed,
                                 inputDataset.tier)
        dbs = {inputDataset.dbsurl: DBSReader(inputDataset.dbsurl)}
        for task in Tier1ReRecoWorkload.taskIterator():
            units, _, _ = Block(**self.splitArgs)(Tier1ReRecoWorkload, task)
            self.assertEqual(47, len(units))
            for unit in units:
                self.assertEqual(69, unit['Priority'])
                self.assertTrue(1 <= unit['Jobs'])
                self.assertEqual(Tier1ReRecoWorkload, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
                self.assertTrue(1 <= unit['NumberOfLumis'])
                self.assertTrue(1 <= unit['NumberOfFiles'])
                self.assertTrue(0 <= unit['NumberOfEvents'])
            self.assertEqual(len(units),
                             len(dbs[inputDataset.dbsurl].listFileBlocks(dataset)))

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
        dbs = {inputDataset.dbsurl: DBSReader(inputDataset.dbsurl)}

        for task in MultiTaskProcessingWorkload.taskIterator():
            units, _, _ = Block(**self.splitArgs)(MultiTaskProcessingWorkload, task)
            self.assertEqual(58, len(units))

            for unit in units:
                self.assertTrue(1 <= unit['Jobs'])
                self.assertEqual(MultiTaskProcessingWorkload, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
            self.assertEqual(len(units),
                             len(dbs[inputDataset.dbsurl].listFileBlocks(datasets[0])))
            count += 1
        self.assertEqual(tasks, count)

    def testWhiteBlackLists(self):
        """Block/Run White/Black lists"""
        Tier1ReRecoWorkload = rerecoWorkload('ReRecoWorkload', rerecoArgs,
                                             assignArgs={'SiteWhitelist': ['T2_XX_SiteA']})
        inputDataset = getFirstTask(Tier1ReRecoWorkload).inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary, inputDataset.processed, inputDataset.tier)

        # No white/black lists
        newArgs = {}
        newArgs.update(rerecoArgs)
        workload = rerecoWorkload('ReRecoWorkload', newArgs,
                                  assignArgs={'SiteWhitelist': ['T2_XX_SiteA']})

        task = getFirstTask(workload)
        units, rejectedWork, badWork = Block(**self.splitArgs)(workload, task)
        self.assertEqual(len(units), 47)
        self.assertEqual(len(rejectedWork), 0)
        self.assertEqual(len(badWork), 0)

        # Block blacklist
        newArgs = {}
        newArgs.update(rerecoArgs)
        newArgs.update({'BlockBlacklist': [dataset + '#03fe83c2-0c23-11e1-b764-003048caaace']})
        workload = rerecoWorkload('ReRecoWorkload', newArgs,
                                  assignArgs={'SiteWhitelist': ['T2_XX_SiteA']})

        task = getFirstTask(workload)
        units, rejectedWork, badWork = Block(**self.splitArgs)(workload, task)
        self.assertEqual(len(units), 46)
        self.assertEqual(len(rejectedWork), 0)
        self.assertEqual(len(badWork), 0)
        self.assertNotEqual(list(units[0]['Inputs']), newArgs['BlockBlacklist'])

        # Block Whitelist
        newArgs = {}
        newArgs.update(rerecoArgs)
        newArgs.update({'BlockWhitelist': [dataset + '#03fe83c2-0c23-11e1-b764-003048caaace']})
        workload = rerecoWorkload('ReRecoWorkload', newArgs,
                                  assignArgs={'SiteWhitelist': ['T2_XX_SiteA']})

        task = getFirstTask(workload)
        units, rejectedWork, badWork = Block(**self.splitArgs)(workload, task)
        self.assertEqual(len(units), 1)
        self.assertEqual(len(rejectedWork), 0)
        self.assertEqual(len(badWork), 0)
        self.assertEqual(list(units[0]['Inputs']), newArgs['BlockWhitelist'])

        # Block Mixed Whitelist
        newArgs = {}
        newArgs.update(rerecoArgs)
        newArgs.update({'BlockWhitelist': [dataset + '#04be2fcc-0b8f-11e1-b764-003048caaace'],
                        'BlockBlacklist': [dataset + '#03fe83c2-0c23-11e1-b764-003048caaace']})
        workload = rerecoWorkload('ReRecoWorkload', newArgs,
                                  assignArgs={'SiteWhitelist': ['T2_XX_SiteA']})

        task = getFirstTask(workload)
        units, rejectedWork, badWork = Block(**self.splitArgs)(workload, task)
        self.assertEqual(len(units), 1)
        self.assertEqual(len(rejectedWork), 0)
        self.assertEqual(len(badWork), 0)
        self.assertEqual(list(units[0]['Inputs']), newArgs['BlockWhitelist'])

        # Run Whitelist
        newArgs = {}
        newArgs.update(rerecoArgs)
        newArgs.update({'RunWhitelist': [181367]})
        workload = rerecoWorkload('ReRecoWorkload', newArgs,
                                  assignArgs={'SiteWhitelist': ['T2_XX_SiteA']})

        task = getFirstTask(workload)
        units, rejectedWork, badWork = Block(**self.splitArgs)(workload, task)
        self.assertEqual(len(units), 1)
        self.assertEqual(len(rejectedWork), 46)
        self.assertEqual(len(badWork), 0)
        self.assertEqual(list(units[0]['Inputs']), [dataset + '#03fe83c2-0c23-11e1-b764-003048caaace'])

        # Run Blacklist
        newArgs = {}
        newArgs.update(rerecoArgs)
        newArgs.update({'RunBlacklist': [180899, 180992, 1]})
        workload = rerecoWorkload('ReRecoWorkload', newArgs,
                                  assignArgs={'SiteWhitelist': ['T2_XX_SiteA']})

        task = getFirstTask(workload)
        units, rejectedWork, badWork = Block(**self.splitArgs)(workload, task)
        self.assertEqual(len(units), 45)
        self.assertEqual(len(rejectedWork), 2)
        self.assertEqual(len(badWork), 0)
        self.assertEqual(list(units[0]['Inputs']), [dataset + '#217ea8d8-0c4f-11e1-b764-003048caaace'])

        # Run Mixed Whitelist
        newArgs = {}
        newArgs.update(rerecoArgs)
        newArgs.update({'RunBlacklist': [180899], 'RunWhitelist': [180992]})
        workload = rerecoWorkload('ReRecoWorkload', newArgs,
                                  assignArgs={'SiteWhitelist': ['T2_XX_SiteA']})

        task = getFirstTask(workload)
        units, rejectedWork, badWork = Block(**self.splitArgs)(workload, task)
        self.assertEqual(len(units), 1)
        self.assertEqual(len(rejectedWork), 46)
        self.assertEqual(len(badWork), 0)
        self.assertEqual(list(units[0]['Inputs']), [dataset + '#b469f816-0946-11e1-8347-003048caaace'])

    def testLumiMask(self):
        """Lumi mask test"""
        rerecoArgs2 = {}
        rerecoArgs2.update(rerecoArgs)
        rerecoArgs2["ConfigCacheID"] = createConfig(rerecoArgs2["CouchDBName"])
        dummyWorkload = rerecoWorkload('ReRecoWorkload', rerecoArgs2,
                                       assignArgs={'SiteWhitelist': ['T2_XX_SiteA']})

        # Block blacklist
        lumiWorkload = rerecoWorkload('ReRecoWorkload', rerecoArgs2,
                                      assignArgs={'SiteWhitelist': ['T2_XX_SiteA']})
        task = getFirstTask(lumiWorkload)
        # task.data.input.splitting.runs = ['1']
        task.data.input.splitting.runs = ['180992']
        task.data.input.splitting.lumis = ['1,1']
        units, rejectedWork, badWork = Block(**self.splitArgs)(lumiWorkload, task)
        self.assertEqual(len(units), 1)
        self.assertEqual(len(rejectedWork), 46)
        self.assertEqual(len(badWork), 0)

    def testDataDirectiveFromQueue(self):
        """Test data directive from queue"""
        Tier1ReRecoWorkload = rerecoWorkload('ReRecoWorkload', rerecoArgs,
                                             assignArgs={'SiteWhitelist': ['T2_XX_SiteA']})
        inputDataset = getFirstTask(Tier1ReRecoWorkload).inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                 inputDataset.processed,
                                 inputDataset.tier)
        dbs = {inputDataset.dbsurl: DBSReader(inputDataset.dbsurl)}
        for task in Tier1ReRecoWorkload.taskIterator():
            # Take dataset and force to run over only 1 block
            units, _, _ = Block(**self.splitArgs)(Tier1ReRecoWorkload, task,
                                                  {dataset + '#28315b28-0c5c-11e1-b764-003048caaace': []})
            self.assertEqual(1, len(units))
            for unit in units:
                self.assertEqual(1, unit['Jobs'])
                self.assertEqual(Tier1ReRecoWorkload, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
            self.assertNotEqual(len(units),
                                len(dbs[inputDataset.dbsurl].listFileBlocks(dataset)))

    def testLumiSplitTier1ReRecoWorkload(self):
        """Tier1 Re-reco workflow"""
        splitArgs = dict(SliceType='NumberOfLumis', SliceSize=1)

        Tier1ReRecoWorkload = rerecoWorkload('ReRecoWorkload', rerecoArgs,
                                             assignArgs={'SiteWhitelist': ['T2_XX_SiteA']})
        Tier1ReRecoWorkload.setStartPolicy('Block', **splitArgs)

        for task in Tier1ReRecoWorkload.taskIterator():
            units, rejectedWork, badWork = Block(**splitArgs)(Tier1ReRecoWorkload, task)
            self.assertEqual(47, len(units))
            for unit in units:
                self.assertTrue(1 <= unit['Jobs'])
            self.assertEqual(0, len(rejectedWork))
            self.assertEqual(0, len(badWork))

    def testRunWhitelist(self):
        """
        ReReco lumi split with Run whitelist
        This test may not do much of anything anymore since listRunLumis is not in DBS3
        """

        splitArgs = dict(SliceType='NumberOfLumis', SliceSize=1)

        Tier1ReRecoWorkload = rerecoWorkload('ReRecoWorkload', rerecoArgs,
                                             assignArgs={'SiteWhitelist': ['T2_XX_SiteA']})
        Tier1ReRecoWorkload.setStartPolicy('Block', **splitArgs)
        Tier1ReRecoWorkload.setRunWhitelist([180899, 180992])
        inputDataset = getFirstTask(Tier1ReRecoWorkload).inputDataset()

        dbs = {inputDataset.dbsurl: DBSReader(inputDataset.dbsurl)}
        for task in Tier1ReRecoWorkload.taskIterator():
            units, rejectedWork, badWork = Block(**splitArgs)(Tier1ReRecoWorkload, task)
            # Blocks 1 and 2 match run distribution
            self.assertEqual(2, len(units))
            self.assertEqual(len(rejectedWork), 45)
            self.assertEqual(len(badWork), 0)
            # Check number of jobs in element match number for
            # dataset in run whitelist
            wq_jobs = 0
            for unit in units:
                wq_jobs += unit['Jobs']
                # This fails. listRunLumis does not work correctly with DBS3,
                # returning None for the # of lumis in a run
                runLumis = dbs[inputDataset.dbsurl].listRunLumis(block=list(unit['Inputs'])[0])
                for run in runLumis:
                    if run in getFirstTask(Tier1ReRecoWorkload).inputRunWhitelist():
                        # This is what it is with DBS3 unless we calculate it
                        self.assertEqual(runLumis[run], None)
            self.assertEqual(2, int(wq_jobs))

    def testInvalidSpecs(self):
        """Specs with no work"""
        # no dataset
        processingSpec = rerecoWorkload('ReRecoWorkload', rerecoArgs,
                                        assignArgs={'SiteWhitelist': ['T2_XX_SiteA']})
        getFirstTask(processingSpec).data.input.dataset = None
        for task in processingSpec.taskIterator():
            self.assertRaises(WorkQueueWMSpecError, Block(), processingSpec, task)

        # invalid dbs url
        processingSpec = rerecoWorkload('ReRecoWorkload', rerecoArgs,
                                        assignArgs={'SiteWhitelist': ['T2_XX_SiteA']})
        getFirstTask(processingSpec).data.input.dataset.dbsurl = 'wrongprot://dbs.example.com'
        for task in processingSpec.taskIterator():
            self.assertRaises(DBSReaderError, Block(), processingSpec, task)

        # dataset non existent
        processingSpec = rerecoWorkload('ReRecoWorkload', rerecoArgs,
                                        assignArgs={'SiteWhitelist': ['T2_XX_SiteA']})
        getFirstTask(processingSpec).data.input.dataset.name = "/MinimumBias/FAKE-Filter-v1/RECO"
        for task in processingSpec.taskIterator():
            self.assertRaises(DBSReaderError, Block(), processingSpec, task)

        # invalid run whitelist
        processingSpec = rerecoWorkload('ReRecoWorkload', rerecoArgs,
                                        assignArgs={'SiteWhitelist': ['T2_XX_SiteA']})
        processingSpec.setRunWhitelist([666])  # not in this dataset
        for task in processingSpec.taskIterator():
            self.assertRaises(WorkQueueNoWorkError, Block(), processingSpec, task)

    def notestParentProcessing(self):
        # Does not work with a RAW dataset, need a different workload
        """
        test parent processing: should have the same results as rereco test
        with the parent flag and dataset.
        """
        parentProcArgs["ConfigCacheID"] = createConfig(parentProcArgs["CouchDBName"])
        factory = ReRecoWorkloadFactory()
        parentProcSpec = factory.factoryWorkloadConstruction('testParentProcessing', parentProcArgs)

        inputDataset = getFirstTask(parentProcSpec).inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                 inputDataset.processed,
                                 inputDataset.tier)
        dbs = {inputDataset.dbsurl: DBSReader(inputDataset.dbsurl)}
        for task in parentProcSpec.taskIterator():
            units, _, _ = Block(**self.splitArgs)(parentProcSpec, task)
            self.assertEqual(47, len(units))
            for unit in units:
                self.assertTrue(1 <= unit['Jobs'])
                self.assertEqual(parentProcSpec, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
                self.assertEqual(True, unit['ParentFlag'])
                self.assertEqual(1, len(unit['ParentData']))
            self.assertEqual(len(units),
                             len(dbs[inputDataset.dbsurl].listFileBlocks(dataset)))

    def testIgnore0SizeBlocks(self):
        """Ignore blocks with 0 files"""
        Tier1ReRecoWorkload = rerecoWorkload('ReRecoWorkload', rerecoArgs,
                                             assignArgs={'SiteWhitelist': ['T2_XX_SiteA']})
        Tier1ReRecoWorkload.setRunWhitelist([2, 3])

        for task in Tier1ReRecoWorkload.taskIterator():
            self.assertRaises(WorkQueueNoWorkError, Block(**self.splitArgs), Tier1ReRecoWorkload, task)

    def testContinuousSplittingSupport(self):
        """Can modify successfully policies for continuous splitting"""
        policyInstance = Block(**self.splitArgs)
        self.assertTrue(policyInstance.supportsWorkAddition(), "Block instance should support continuous splitting")
        Tier1ReRecoWorkload = rerecoWorkload('ReRecoWorkload', rerecoArgs,
                                             assignArgs={'SiteWhitelist': ['T2_XX_SiteA']})
        Tier1ReRecoWorkload.data.request.priority = 69
        inputDataset = getFirstTask(Tier1ReRecoWorkload).inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                 inputDataset.processed,
                                 inputDataset.tier)
        dbs = {inputDataset.dbsurl: DBSReader(inputDataset.dbsurl)}
        for task in Tier1ReRecoWorkload.taskIterator():
            units, _, _ = policyInstance(Tier1ReRecoWorkload, task)
            self.assertEqual(47, len(units))
            blocks = []  # fill with blocks as we get work units for them
            inputs = {}
            for unit in units:
                blocks.extend(list(unit['Inputs']))
                inputs.update(unit['Inputs'])
                self.assertEqual(69, unit['Priority'])
                self.assertTrue(1 <= unit['Jobs'])
                self.assertEqual(Tier1ReRecoWorkload, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
                self.assertTrue(1 <= unit['NumberOfLumis'])
                self.assertTrue(1 <= unit['NumberOfFiles'])
                self.assertTrue(0 <= unit['NumberOfEvents'])
            self.assertEqual(len(units),
                             len(dbs[inputDataset.dbsurl].listFileBlocks(dataset)))

        # Modify the spec and task, get first a fresh policy instance
        policyInstance = Block(**self.splitArgs)
        for task in Tier1ReRecoWorkload.taskIterator():
            policyInstance.modifyPolicyForWorkAddition({'ProcessedInputs': list(inputs)})
            self.assertRaises(WorkQueueNoWorkError, policyInstance, Tier1ReRecoWorkload, task)

        # Run one last time
        policyInstance = Block(**self.splitArgs)
        for task in Tier1ReRecoWorkload.taskIterator():
            policyInstance.modifyPolicyForWorkAddition({'ProcessedInputs': list(inputs)})
            self.assertRaises(WorkQueueNoWorkError, policyInstance, Tier1ReRecoWorkload, task)

        return

    def testDatasetLocation(self):
        """
        _testDatasetLocation_

        This is a function of all start policies so only test it here
        as there is no StartPolicyInterface unit test
        """
        policyInstance = Block(**self.splitArgs)
        # The policy instance must be called first to initialize the values
        Tier1ReRecoWorkload = rerecoWorkload('ReRecoWorkload', rerecoArgs,
                                             assignArgs={'SiteWhitelist': ['T2_XX_SiteA']})
        for task in Tier1ReRecoWorkload.taskIterator():
            policyInstance(Tier1ReRecoWorkload, task)
            outputs = policyInstance.getDatasetLocations(Tier1ReRecoWorkload.listInputDatasets())
            for dataset in outputs:
                self.assertItemsEqual(outputs[dataset], ['T2_XX_SiteA', 'T2_XX_SiteB'])
        return

    def testPileupData(self):
        """
        _testPileupData_

        Check that every workqueue element split contains the pile up data
        if it is present in the workload.
        """
        for task in MultiTaskProcessingWorkload.taskIterator():
            units, _, _ = Block(**self.splitArgs)(MultiTaskProcessingWorkload, task)
            self.assertEqual(58, len(units))
            for unit in units:
                pileupData = unit["PileupData"]
                self.assertEqual(len(pileupData), 1)
                self.assertItemsEqual(listvalues(pileupData)[0], ['T2_XX_SiteA', 'T2_XX_SiteB', 'T2_XX_SiteC'])
        return

    def testWithMaskedBlocks(self):
        """
        _testWithMaskedBlocks_

        Test job splitting with masked blocks
        """

        Tier1ReRecoWorkload = rerecoWorkload('ReRecoWorkload', rerecoArgs,
                                             assignArgs={'SiteWhitelist': ['T2_XX_SiteA']})
        Tier1ReRecoWorkload.data.request.priority = 69
        task = getFirstTask(Tier1ReRecoWorkload)
        dummyDataset = task.inputDataset()

        task.data.input.splitting.runs = [181061, 180899]
        task.data.input.splitting.lumis = ['1,50,60,70', '1,1']
        lumiMask = LumiList(compactList={'206371': [[1, 50], [60, 70]], '180899': [[1, 1]], })

        units, _, _ = Block(**self.splitArgs)(Tier1ReRecoWorkload, task)

        nLumis = 0
        for unit in units:
            nLumis += unit['NumberOfLumis']

        self.assertEqual(len(lumiMask.getLumis()), nLumis)

    def testGetMaskedBlocks(self):
        """
        _testGetMaskedBlocks_

        Check that getMaskedBlocks is returning the correct information
        """

        rerecoArgs["ConfigCacheID"] = createConfig(rerecoArgs["CouchDBName"])
        factory = ReRecoWorkloadFactory()

        Tier1ReRecoWorkload = factory.factoryWorkloadConstruction('ReRecoWorkload', rerecoArgs)
        Tier1ReRecoWorkload.data.request.priority = 69
        task = getFirstTask(Tier1ReRecoWorkload)
        inputDataset = task.inputDataset()
        inputDataset.primary = 'SingleElectron'
        inputDataset.processed = 'StoreResults-Run2011A-WElectron-PromptSkim-v4-ALCARECO-NOLC-36cfce5a1d3f3ab4df5bd2aa0a4fa380'
        inputDataset.tier = 'USER'

        task.data.input.splitting.runs = [166921, 166429, 166911]
        task.data.input.splitting.lumis = ['40,70', '1,50', '1,5,16,20']
        lumiMask = LumiList(compactList={'166921': [[40, 70]], '166429': [[1, 50]], '166911': [[1, 5], [16, 20]], })
        inputLumis = LumiList(compactList={'166921': [[1, 67]], '166429': [[1, 91]], '166911': [[1, 104]], })
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                 inputDataset.processed,
                                 inputDataset.tier)
        dbs = DBSReader(inputDataset.dbsurl)
        maskedBlocks = Block(**self.splitArgs).getMaskedBlocks(task, dbs, dataset)
        for dummyBlock, files in viewitems(maskedBlocks):
            for dummyFile, lumiList in viewitems(files):
                self.assertEqual(str(lumiList), str(inputLumis & lumiMask))


if __name__ == '__main__':
    unittest.main()