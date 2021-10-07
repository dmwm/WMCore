#!/usr/bin/env python
"""
    WorkQueue.Policy.Start.Dataset tests
"""

import unittest

from WMCore_t.WMSpec_t.samples.MultiTaskProcessingWorkload \
    import workload as MultiTaskProcessingWorkload
from WMCore_t.WorkQueue_t.WorkQueue_t import getFirstTask

from WMCore.Services.DBS.DBSErrors import DBSReaderError
from WMCore.WMSpec.StdSpecs.DQMHarvest import DQMHarvestWorkloadFactory
from WMCore.WMSpec.StdSpecs.ReReco import ReRecoWorkloadFactory
from WMCore.WorkQueue.Policy.Start.Dataset import Dataset
from WMCore.WorkQueue.WorkQueueExceptions import (WorkQueueWMSpecError, WorkQueueNoWorkError)
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMQuality.Emulators.RucioClient.MockRucioApi import NOT_EXIST_DATASET
from WMQuality.Emulators.WMSpecGenerator.WMSpecGenerator import createConfig


def getRequestArgs():
    """ Returns default values defined in the spec workload """
    return DQMHarvestWorkloadFactory.getTestArguments()


def getReRecoArgs(parent=False):
    """ Returns default values defined in the spec workload """
    rerecoArgs = ReRecoWorkloadFactory.getTestArguments()
    if parent:
        rerecoArgs.update(IncludeParents="True")
    return rerecoArgs


def rerecoWorkload(workloadName, arguments, assignArgs=None):
    factory = ReRecoWorkloadFactory()
    wmspec = factory.factoryWorkloadConstruction(workloadName, arguments)
    if assignArgs:
        args = factory.getAssignTestArguments()
        args.update(assignArgs)
        wmspec.updateArguments(args)
    return wmspec


class DatasetTestCase(EmulatedUnitTestCase):
    def __init__(self, methodName='runTest'):
        super(DatasetTestCase, self).__init__(methodName=methodName)

    def setUp(self):
        super(DatasetTestCase, self).setUp()

    def tearDown(self):
        super(DatasetTestCase, self).tearDown()

    def testDatasetPolicy(self):
        """
        Test ordinary NumberOfRuns splitting
        """
        dqmHarvArgs = getRequestArgs()
        dqmHarvArgs["DQMConfigCacheID"] = createConfig(dqmHarvArgs["CouchDBName"])
        factory = DQMHarvestWorkloadFactory()
        DQMHarvWorkload = factory.factoryWorkloadConstruction('DQMHarvestTest', dqmHarvArgs)
        DQMHarvWorkload.setSiteWhitelist(["T2_XX_SiteA", "T2_XX_SiteB", "T2_XX_SiteC"])
        splitArgs = DQMHarvWorkload.startPolicyParameters()
        inputDataset = getFirstTask(DQMHarvWorkload).getInputDatasetPath()

        for task in DQMHarvWorkload.taskIterator():
            units, _, _ = Dataset(**splitArgs)(DQMHarvWorkload, task)
            self.assertEqual(1, len(units))
            for unit in units:
                self.assertEqual(47, unit['Jobs'])
                self.assertEqual(DQMHarvWorkload, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
                self.assertEqual(list(unit['Inputs']), [inputDataset])
                self.assertEqual(4855, unit['NumberOfLumis'])
                self.assertEqual(72, unit['NumberOfFiles'])
                self.assertEqual(743201, unit['NumberOfEvents'])

    def testDatasetRunWhitelist(self):
        """
        Test NumberOfRuns splitting with run white list
        """
        dqmHarvArgs = getRequestArgs()
        dqmHarvArgs["DQMConfigCacheID"] = createConfig(dqmHarvArgs["CouchDBName"])
        dqmHarvArgs["RunWhitelist"] = [181358, 181417, 180992, 181151]
        factory = DQMHarvestWorkloadFactory()
        DQMHarvWorkload = factory.factoryWorkloadConstruction('DQMHarvestTest', dqmHarvArgs)
        DQMHarvWorkload.setSiteWhitelist(["T2_XX_SiteA", "T2_XX_SiteB", "T2_XX_SiteC"])
        splitArgs = DQMHarvWorkload.startPolicyParameters()
        inputDataset = getFirstTask(DQMHarvWorkload).getInputDatasetPath()

        for task in DQMHarvWorkload.taskIterator():
            units, _, _ = Dataset(**splitArgs)(DQMHarvWorkload, task)
            self.assertEqual(1, len(units))
            for unit in units:
                self.assertEqual(4, unit['Jobs'])
                self.assertEqual(DQMHarvWorkload, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
                self.assertEqual(list(unit['Inputs']), [inputDataset])
                self.assertEqual(217, unit['NumberOfLumis'])
                self.assertEqual(8, unit['NumberOfFiles'])
                self.assertEqual(83444, unit['NumberOfEvents'])

    def testDatasetLumiMask(self):
        """
        Test NumberOfRuns splitting type with lumi mask
        """
        dqmHarvArgs = getRequestArgs()
        dqmHarvArgs["DQMConfigCacheID"] = createConfig(dqmHarvArgs["CouchDBName"])
        dqmHarvArgs["LumiList"] = {"181358": [[71, 80], [95, 110]], "181151": [[1, 20]]}
        factory = DQMHarvestWorkloadFactory()
        DQMHarvWorkload = factory.factoryWorkloadConstruction('DQMHarvestTest', dqmHarvArgs)
        DQMHarvWorkload.setSiteWhitelist(["T2_XX_SiteA", "T2_XX_SiteB", "T2_XX_SiteC"])
        splitArgs = DQMHarvWorkload.startPolicyParameters()
        inputDataset = getFirstTask(DQMHarvWorkload).getInputDatasetPath()

        for task in DQMHarvWorkload.taskIterator():
            units, _, _ = Dataset(**splitArgs)(DQMHarvWorkload, task)
            self.assertEqual(1, len(units))
            for unit in units:
                self.assertEqual(2, unit['Jobs'])
                self.assertEqual(DQMHarvWorkload, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
                self.assertEqual(list(unit['Inputs']), [inputDataset])
                self.assertEqual(46, unit['NumberOfLumis'])
                self.assertEqual(4, unit['NumberOfFiles'])
                self.assertEqual(12342, unit['NumberOfEvents'])

    def testDatasetSingleJob(self):
        """
        Test NumberOfRuns splitting type with very large SliceSize
        """
        dqmHarvArgs = getRequestArgs()
        dqmHarvArgs["DQMConfigCacheID"] = createConfig(dqmHarvArgs["CouchDBName"])
        dqmHarvArgs["DQMHarvestUnit"] = 'multiRun'
        factory = DQMHarvestWorkloadFactory()
        DQMHarvWorkload = factory.factoryWorkloadConstruction('DQMHarvestTest', dqmHarvArgs)
        DQMHarvWorkload.setSiteWhitelist(["T2_XX_SiteA", "T2_XX_SiteB", "T2_XX_SiteC"])
        splitArgs = DQMHarvWorkload.startPolicyParameters()
        inputDataset = getFirstTask(DQMHarvWorkload).getInputDatasetPath()

        for task in DQMHarvWorkload.taskIterator():
            units, _, _ = Dataset(**splitArgs)(DQMHarvWorkload, task)
            self.assertEqual(1, len(units))
            for unit in units:
                self.assertEqual(1, unit['Jobs'])
                self.assertEqual(DQMHarvWorkload, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
                self.assertEqual(list(unit['Inputs']), [inputDataset])
                self.assertEqual(4855, unit['NumberOfLumis'])
                self.assertEqual(72, unit['NumberOfFiles'])
                self.assertEqual(743201, unit['NumberOfEvents'])

    def testTier1ReRecoWorkload(self):
        """Tier1 Re-reco workflow"""
        splitArgs = dict(SliceType='NumberOfFiles', SliceSize=5)
        rerecoArgs = getReRecoArgs()
        rerecoArgs["ConfigCacheID"] = createConfig(rerecoArgs["CouchDBName"])
        Tier1ReRecoWorkload = rerecoWorkload('ReRecoWorkload', rerecoArgs,
                                             assignArgs={'SiteWhitelist': ['T2_XX_SiteA']})

        Tier1ReRecoWorkload.setStartPolicy('Dataset', **splitArgs)
        inputDataset = getFirstTask(Tier1ReRecoWorkload).getInputDatasetPath()
        for task in Tier1ReRecoWorkload.taskIterator():
            units, _, _ = Dataset(**splitArgs)(Tier1ReRecoWorkload, task)
            self.assertEqual(1, len(units))
            for unit in units:
                self.assertEqual(15, unit['Jobs'])
                self.assertEqual(Tier1ReRecoWorkload, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
                self.assertEqual(list(unit['Inputs']), [inputDataset])
                self.assertEqual(4855, unit['NumberOfLumis'])
                self.assertEqual(72, unit['NumberOfFiles'])
                self.assertEqual(743201, unit['NumberOfEvents'])

    def testMultiTaskProcessingWorkload(self):
        """Multi Task Processing Workflow"""
        splitArgs = dict(SliceType='NumberOfFiles', SliceSize=5)
        datasets = []
        tasks, count = 0, 0
        for task in MultiTaskProcessingWorkload.taskIterator():
            tasks += 1
            inputDataset = task.getInputDatasetPath()
            datasets.append(inputDataset)
        for task in MultiTaskProcessingWorkload.taskIterator():
            units, _, _ = Dataset(**splitArgs)(MultiTaskProcessingWorkload, task)
            self.assertEqual(1, len(units))
            for unit in units:
                self.assertEqual(22, unit['Jobs'])
                self.assertEqual(MultiTaskProcessingWorkload, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
                self.assertEqual(list(unit['Inputs']), [datasets[count]])
            count += 1
        self.assertEqual(tasks, count)

    def testLumiSplitTier1ReRecoWorkload(self):
        """Tier1 Re-reco workflow split by Lumi"""
        splitArgs = dict(SliceType='NumberOfLumis', SliceSize=2)
        rerecoArgs = getReRecoArgs()
        rerecoArgs["ConfigCacheID"] = createConfig(rerecoArgs["CouchDBName"])
        factory = ReRecoWorkloadFactory()
        Tier1ReRecoWorkload = factory.factoryWorkloadConstruction('ReRecoWorkload', rerecoArgs)
        Tier1ReRecoWorkload.setStartPolicy('Dataset', **splitArgs)
        Tier1ReRecoWorkload.setSiteWhitelist(["T2_XX_SiteA", "T2_XX_SiteB", "T2_XX_SiteC"])
        for task in Tier1ReRecoWorkload.taskIterator():
            units, _, _ = Dataset(**splitArgs)(Tier1ReRecoWorkload, task)
            self.assertEqual(1, len(units))
            for unit in units:
                self.assertEqual(2428, unit['Jobs'])

    def testParentProcessing(self):
        """
        test parent processing: should have the same results as rereco test
        with the parent flag and dataset.
        """
        splitArgs = dict(SliceType='NumberOfLumis', SliceSize=2)
        parentProcArgs = getReRecoArgs(parent=True)
        parentProcArgs["ConfigCacheID"] = createConfig(parentProcArgs["CouchDBName"])
        # This dataset does have parents. Adding it here to keep the test going.
        # It seems like "dbs" below is never used
        parentProcArgs2 = {}
        parentProcArgs2.update(parentProcArgs)
        #parentProcArgs2.update({'InputDataset': '/SingleMu/CMSSW_6_2_0_pre4-PRE_61_V1_RelVal_mu2012A-v1/RECO'})
        parentProcArgs2.update({'InputDataset': '/Cosmics/ComissioningHI-PromptReco-v1/RECO'})
        from pprint import pprint
        pprint(parentProcArgs2)
        parentProcSpec = rerecoWorkload('ReRecoWorkload', parentProcArgs2,
                                        assignArgs={'SiteWhitelist': ['T2_XX_SiteA']})
        parentProcSpec.setStartPolicy('Dataset', **splitArgs)
        inputDataset = getFirstTask(parentProcSpec).getInputDatasetPath()
        for task in parentProcSpec.taskIterator():
            units, _, _ = Dataset(**splitArgs)(parentProcSpec, task)
            self.assertEqual(1, len(units))
            for unit in units:
                self.assertEqual(3993, unit['Jobs'])
                self.assertEqual(7985, unit['NumberOfLumis'])
                self.assertEqual(parentProcSpec, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
                self.assertEqual(list(unit['Inputs']), [inputDataset])
                self.assertEqual(True, unit['ParentFlag'])
                self.assertEqual(0, len(unit['ParentData']))

    def testDataDirectiveFromQueue(self):
        """Test data directive from queue"""
        dqmHarvArgs = getRequestArgs()
        dqmHarvArgs["DQMConfigCacheID"] = createConfig(dqmHarvArgs["CouchDBName"])
        factory = DQMHarvestWorkloadFactory()
        DQMHarvWorkload = factory.factoryWorkloadConstruction('DQMHarvestTest', dqmHarvArgs)
        DQMHarvWorkload.setSiteWhitelist(["T2_XX_SiteA", "T2_XX_SiteB", "T2_XX_SiteC"])
        splitArgs = DQMHarvWorkload.startPolicyParameters()

        for task in DQMHarvWorkload.taskIterator():
            self.assertRaises(RuntimeError, Dataset(**splitArgs), DQMHarvWorkload, task, {NOT_EXIST_DATASET: []})

    def testInvalidSpecs(self):
        """Specs with no work"""
        dqmHarvArgs = getRequestArgs()
        # no dataset
        dqmHarvArgs["DQMConfigCacheID"] = createConfig(dqmHarvArgs["CouchDBName"])
        factory = DQMHarvestWorkloadFactory()
        DQMHarvWorkload = factory.factoryWorkloadConstruction('NoInputDatasetTest', dqmHarvArgs)
        DQMHarvWorkload.setSiteWhitelist(["T2_XX_SiteA", "T2_XX_SiteB", "T2_XX_SiteC"])
        getFirstTask(DQMHarvWorkload).data.input.dataset = None
        for task in DQMHarvWorkload.taskIterator():
            self.assertRaises(WorkQueueWMSpecError, Dataset(), DQMHarvWorkload, task)

        # invalid dataset name
        DQMHarvWorkload = factory.factoryWorkloadConstruction('InvalidInputDatasetTest', dqmHarvArgs)
        DQMHarvWorkload.setSiteWhitelist(["T2_XX_SiteA", "T2_XX_SiteB", "T2_XX_SiteC"])
        getFirstTask(DQMHarvWorkload).data.input.dataset.name = '/MinimumBias/FAKE-Filter-v1/RECO'
        for task in DQMHarvWorkload.taskIterator():
            self.assertRaises(DBSReaderError, Dataset(), DQMHarvWorkload, task)

        # invalid run whitelist
        DQMHarvWorkload = factory.factoryWorkloadConstruction('InvalidRunNumberTest', dqmHarvArgs)
        DQMHarvWorkload.setSiteWhitelist(["T2_XX_SiteA", "T2_XX_SiteB", "T2_XX_SiteC"])
        DQMHarvWorkload.setRunWhitelist([666])  # not in this dataset
        for task in DQMHarvWorkload.taskIterator():
            self.assertRaises(WorkQueueNoWorkError, Dataset(), DQMHarvWorkload, task)


if __name__ == '__main__':
    unittest.main()
