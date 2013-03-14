 #!/usr/bin/env python
"""
    WorkQueue.Policy.Start.Block tests
"""

import unittest
import shutil
from WMCore.WorkQueue.Policy.Start.Block import Block
from WMCore.WMSpec.StdSpecs.ReReco import rerecoWorkload, getTestArguments
from WMCore.Services.EmulatorSwitch import EmulatorHelper
from WMCore.Services.DBS.DBSReader import DBSReader
from WMCore_t.WMSpec_t.samples.MultiTaskProcessingWorkload \
    import workload as MultiTaskProcessingWorkload
from WMCore.WorkQueue.WorkQueueExceptions import *
from WMCore_t.WorkQueue_t.WorkQueue_t import getFirstTask
from WMQuality.Emulators.DataBlockGenerator import Globals


rerecoArgs = getTestArguments()
parentProcArgs = getTestArguments()
parentProcArgs.update(IncludeParents = "True")

class BlockTestCase(unittest.TestCase):

    splitArgs = dict(SliceType = 'NumberOfFiles', SliceSize = 10)

    def setUp(self):
        EmulatorHelper.setEmulators(phedex = True, dbs = True,
                            siteDB = True, requestMgr = False)
        Globals.GlobalParams.resetParams()
    def tearDown(self):
        EmulatorHelper.resetEmulators()
        Globals.GlobalParams.resetParams()

    def testTier1ReRecoWorkload(self):
        """Tier1 Re-reco workflow"""
        Tier1ReRecoWorkload = rerecoWorkload('ReRecoWorkload', rerecoArgs)
        Tier1ReRecoWorkload.data.request.priority = 69
        inputDataset = getFirstTask(Tier1ReRecoWorkload).inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        dbs = {inputDataset.dbsurl : DBSReader(inputDataset.dbsurl)}
        for task in Tier1ReRecoWorkload.taskIterator():
            units, _ = Block(**self.splitArgs)(Tier1ReRecoWorkload, task)
            self.assertEqual(Globals.GlobalParams.numOfBlocksPerDataset(), len(units))
            blocks = [] # fill with blocks as we get work units for them
            for unit in units:
                self.assertEqual(69, unit['Priority'])
                self.assertEqual(1, unit['Jobs'])
                self.assertEqual(Tier1ReRecoWorkload, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
                self.assertEqual(4, unit['NumberOfLumis'])
                self.assertEqual(10, unit['NumberOfFiles'])
                self.assertEqual(10000, unit['NumberOfEvents'])
            self.assertEqual(len(units),
                             len(dbs[inputDataset.dbsurl].getFileBlocksInfo(dataset)))


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
            units, _ = Block(**self.splitArgs)(MultiTaskProcessingWorkload, task)
            self.assertEqual(Globals.GlobalParams.numOfBlocksPerDataset(), len(units))
            blocks = [] # fill with blocks as we get work units for them

            for unit in units:
                self.assertEqual(1, unit['Jobs'])
                self.assertEqual(MultiTaskProcessingWorkload, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
            self.assertEqual(len(units),
                             len(dbs[inputDataset.dbsurl].getFileBlocksInfo(datasets[0])))
            count += 1
        self.assertEqual(tasks, count)


    def testWhiteBlackLists(self):
        """Block/Run White/Black lists"""
        Tier1ReRecoWorkload = rerecoWorkload('ReRecoWorkload', rerecoArgs)
        inputDataset = getFirstTask(Tier1ReRecoWorkload).inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        dbs = {inputDataset.dbsurl : DBSReader(inputDataset.dbsurl)}

        # Block blacklist
        rerecoArgs2 = {'BlockBlacklist' : [dataset + '#1']}
        rerecoArgs2.update(rerecoArgs)
        blacklistBlockWorkload = rerecoWorkload('ReRecoWorkload',
                                              rerecoArgs2)
        task = getFirstTask(blacklistBlockWorkload)
        units, rejectedWork = Block(**self.splitArgs)(blacklistBlockWorkload, task)
        self.assertEqual(len(units), 1)
        self.assertEqual(len(rejectedWork), 0)
        self.assertNotEqual(units[0]['Inputs'].keys(), rerecoArgs2['BlockBlacklist'])

        # Block Whitelist
        rerecoArgs2['BlockWhitelist'] = [dataset + '#1']
        rerecoArgs2['BlockBlacklist'] = []
        blacklistBlockWorkload = rerecoWorkload('ReRecoWorkload',
                                                     rerecoArgs2)
        task = getFirstTask(blacklistBlockWorkload)
        units, rejectedWork = Block(**self.splitArgs)(blacklistBlockWorkload, task)
        self.assertEqual(len(units), 1)
        self.assertEqual(len(rejectedWork), 0)
        self.assertEqual(units[0]['Inputs'].keys(), rerecoArgs2['BlockWhitelist'])

        # Block Mixed Whitelist
        rerecoArgs2['BlockWhitelist'] = [dataset + '#2']
        rerecoArgs2['BlockBlacklist'] = [dataset + '#1']
        blacklistBlockWorkload = rerecoWorkload('ReRecoWorkload',
                                                     rerecoArgs2)
        task = getFirstTask(blacklistBlockWorkload)
        units, rejectedWork = Block(**self.splitArgs)(blacklistBlockWorkload, task)
        self.assertEqual(len(units), 1)
        self.assertEqual(len(rejectedWork), 0)
        self.assertEqual(units[0]['Inputs'].keys(), rerecoArgs2['BlockWhitelist'])

        # Run Whitelist
        rerecoArgs3 = {'RunWhitelist' : [1]}
        rerecoArgs3.update(rerecoArgs)
        blacklistBlockWorkload = rerecoWorkload('ReRecoWorkload',
                                                     rerecoArgs3)
        task = getFirstTask(blacklistBlockWorkload)
        units, rejectedWork = Block(**self.splitArgs)(blacklistBlockWorkload, task)
        self.assertEqual(len(units), 1)
        self.assertEqual(len(rejectedWork), 1)
        self.assertEqual(units[0]['Inputs'].keys(), [dataset + '#1'])

        # Run Blacklist
        rerecoArgs3 = {'RunBlacklist' : [2, 3]}
        rerecoArgs3.update(rerecoArgs)
        blacklistBlockWorkload = rerecoWorkload('ReRecoWorkload',
                                                    rerecoArgs3)
        task = getFirstTask(blacklistBlockWorkload)
        units, rejectedWork = Block(**self.splitArgs)(blacklistBlockWorkload, task)
        self.assertEqual(len(units), 1)
        self.assertEqual(len(rejectedWork), 1)
        self.assertEqual(units[0]['Inputs'].keys(), [dataset + '#1'])

        # Run Mixed Whitelist
        rerecoArgs3 = {'RunBlacklist' : [1], 'RunWhitelist' : [2]}
        rerecoArgs3.update(rerecoArgs)
        blacklistBlockWorkload = rerecoWorkload('ReRecoWorkload',
                                                     rerecoArgs3)
        task = getFirstTask(blacklistBlockWorkload)
        units, rejectedWork = Block(**self.splitArgs)(blacklistBlockWorkload, task)
        self.assertEqual(len(units), 1)
        self.assertEqual(len(rejectedWork), 1)
        self.assertEqual(units[0]['Inputs'].keys(), [dataset + '#2'])

    def testLumiMask(self):
        """Lumi mask test"""
        rerecoArgs2 = {}
        rerecoArgs2.update(rerecoArgs)
        Tier1ReRecoWorkload = rerecoWorkload('ReRecoWorkload', rerecoArgs2)
        inputDataset = getFirstTask(Tier1ReRecoWorkload).inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        dbs = {inputDataset.dbsurl : DBSReader(inputDataset.dbsurl)}

        # Block blacklist
        lumiWorkload = rerecoWorkload('ReRecoWorkload',
                                              rerecoArgs2)
        task = getFirstTask(lumiWorkload)
        task.data.input.splitting.runs = ['1']
        task.data.input.splitting.lumis = ['1,1']
        units, rejectedWork = Block(**self.splitArgs)(lumiWorkload, task)
        self.assertEqual(len(units), 1)
        self.assertEqual(len(rejectedWork), 1)


    def testDataDirectiveFromQueue(self):
        """Test data directive from queue"""
        Tier1ReRecoWorkload = rerecoWorkload('ReRecoWorkload', rerecoArgs)
        inputDataset = getFirstTask(Tier1ReRecoWorkload).inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        dbs = {inputDataset.dbsurl : DBSReader(inputDataset.dbsurl)}
        for task in Tier1ReRecoWorkload.taskIterator():
            # Take dataset and force to run over only 1 block
            units, _ = Block(**self.splitArgs)(Tier1ReRecoWorkload, task,
                                            {dataset + '#1' : []})
            self.assertEqual(1, len(units))
            blocks = [] # fill with blocks as we get work units for them
            for unit in units:
                self.assertEqual(1, unit['Jobs'])
                self.assertEqual(Tier1ReRecoWorkload, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
            self.assertNotEqual(len(units),
                             len(dbs[inputDataset.dbsurl].getFileBlocksInfo(dataset)))

    def testLumiSplitTier1ReRecoWorkload(self):
        """Tier1 Re-reco workflow"""
        splitArgs = dict(SliceType = 'NumberOfLumis', SliceSize = 1)

        Tier1ReRecoWorkload = rerecoWorkload('ReRecoWorkload', rerecoArgs)
        Tier1ReRecoWorkload.setStartPolicy('Block', **splitArgs)
        inputDataset = getFirstTask(Tier1ReRecoWorkload).inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        dbs = {inputDataset.dbsurl : DBSReader(inputDataset.dbsurl)}
        for task in Tier1ReRecoWorkload.taskIterator():
            units, rejectedWork = Block(**splitArgs)(Tier1ReRecoWorkload, task)
            self.assertEqual(2, len(units))
            blocks = [] # fill with blocks as we get work units for them
            for unit in units:
                self.assertEqual(4, unit['Jobs'])
            self.assertEqual(0, len(rejectedWork))

    def testRunWhitelist(self):
        """ReReco lumi split with Run whitelist"""
        # get files with multiple runs
        Globals.GlobalParams.setNumOfRunsPerFile(8)
        # a large number of lumis to ensure we get multiple runs
        Globals.GlobalParams.setNumOfLumisPerBlock(20)
        splitArgs = dict(SliceType = 'NumberOfLumis', SliceSize = 1)

        Tier1ReRecoWorkload = rerecoWorkload('ReRecoWorkload', rerecoArgs)
        Tier1ReRecoWorkload.setStartPolicy('Block', **splitArgs)
        Tier1ReRecoWorkload.setRunWhitelist([2, 3])
        inputDataset = getFirstTask(Tier1ReRecoWorkload).inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        dbs = {inputDataset.dbsurl : DBSReader(inputDataset.dbsurl)}
        for task in Tier1ReRecoWorkload.taskIterator():
            units, rejectedWork = Block(**splitArgs)(Tier1ReRecoWorkload, task)
            # Blocks 1 and 2 match run distribution
            self.assertEqual(2, len(units))
            self.assertEqual(len(rejectedWork), 0)
            # Check number of jobs in element match number for
            # dataset in run whitelist
            jobs = 0
            wq_jobs = 0
            for unit in units:
                wq_jobs += unit['Jobs']
                runLumis = dbs[inputDataset.dbsurl].listRunLumis(block = unit['Inputs'].keys()[0])
                for run in runLumis:
                    if run in getFirstTask(Tier1ReRecoWorkload).inputRunWhitelist():
                        jobs += runLumis[run]
            self.assertEqual(int(jobs / splitArgs['SliceSize'] ) , int(wq_jobs))

    def testInvalidSpecs(self):
        """Specs with no work"""
        # no dataset
        processingSpec = rerecoWorkload('testProcessingInvalid', rerecoArgs)
        getFirstTask(processingSpec).data.input.dataset = None
        for task in processingSpec.taskIterator():
            self.assertRaises(WorkQueueWMSpecError, Block(), processingSpec, task)

        # invalid dbs url
        processingSpec = rerecoWorkload('testProcessingInvalid', rerecoArgs)
        getFirstTask(processingSpec).data.input.dataset.dbsurl = 'wrongprot://dbs.example.com'
        for task in processingSpec.taskIterator():
            self.assertRaises(WorkQueueWMSpecError, Block(), processingSpec, task)

        # invalid dataset name
        processingSpec = rerecoWorkload('testProcessingInvalid', rerecoArgs)
        getFirstTask(processingSpec).data.input.dataset.primary = Globals.NOT_EXIST_DATASET
        for task in processingSpec.taskIterator():
            self.assertRaises(WorkQueueNoWorkError, Block(), processingSpec, task)

        # invalid run whitelist
        processingSpec = rerecoWorkload('testProcessingInvalid', rerecoArgs)
        processingSpec.setRunWhitelist([666]) # not in this dataset
        for task in processingSpec.taskIterator():
            self.assertRaises(WorkQueueNoWorkError, Block(), processingSpec, task)

        # blocks with 0 files are skipped
        # set all blocks in request to 0 files, no work should be found & an error is raised
        Globals.GlobalParams.setNumOfFilesPerBlock(0)
        processingSpec = rerecoWorkload('testProcessingInvalid', rerecoArgs)
        for task in processingSpec.taskIterator():
            self.assertRaises(WorkQueueNoWorkError, Block(), processingSpec, task)
        Globals.GlobalParams.resetParams()

    def testParentProcessing(self):
        """
        test parent processing: should have the same results as rereco test
        with the parent flag and dataset.
        """
        parentProcSpec = rerecoWorkload('testParentProcessing', parentProcArgs)

        inputDataset = getFirstTask(parentProcSpec).inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        dbs = {inputDataset.dbsurl : DBSReader(inputDataset.dbsurl)}
        for task in parentProcSpec.taskIterator():
            units, _ = Block(**self.splitArgs)(parentProcSpec, task)
            self.assertEqual(Globals.GlobalParams.numOfBlocksPerDataset(), len(units))
            blocks = [] # fill with blocks as we get work units for them
            for unit in units:
                self.assertEqual(1, unit['Jobs'])
                self.assertEqual(parentProcSpec, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
                self.assertEqual(True, unit['ParentFlag'])
                self.assertEqual(1, len(unit['ParentData']))
            self.assertEqual(len(units),
                             len(dbs[inputDataset.dbsurl].getFileBlocksInfo(dataset)))

    def testIgnore0SizeBlocks(self):
        """Ignore blocks with 0 files"""
        Globals.GlobalParams.setNumOfFilesPerBlock(0)

        Tier1ReRecoWorkload = rerecoWorkload('ReRecoWorkload', rerecoArgs)
        Tier1ReRecoWorkload.setRunWhitelist([2, 3])
        inputDataset = getFirstTask(Tier1ReRecoWorkload).inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        dbs = {inputDataset.dbsurl : DBSReader(inputDataset.dbsurl)}
        for task in Tier1ReRecoWorkload.taskIterator():
            self.assertRaises(WorkQueueNoWorkError, Block(**self.splitArgs), Tier1ReRecoWorkload, task)

    def testContinuousSplittingSupport(self):
        """Can modify successfully policies for continuous splitting"""
        policyInstance = Block(**self.splitArgs)
        self.assertTrue(policyInstance.supportsWorkAddition(), "Block instance should support continuous splitting")
        Tier1ReRecoWorkload = rerecoWorkload('ReRecoWorkload', rerecoArgs)
        Tier1ReRecoWorkload.data.request.priority = 69
        inputDataset = getFirstTask(Tier1ReRecoWorkload).inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        dbs = {inputDataset.dbsurl : DBSReader(inputDataset.dbsurl)}
        for task in Tier1ReRecoWorkload.taskIterator():
            units, _ = policyInstance(Tier1ReRecoWorkload, task)
            self.assertEqual(Globals.GlobalParams.numOfBlocksPerDataset(), len(units))
            blocks = [] # fill with blocks as we get work units for them
            inputs = {}
            for unit in units:
                blocks.extend(unit['Inputs'].keys())
                inputs.update(unit['Inputs'])
                self.assertEqual(69, unit['Priority'])
                self.assertEqual(1, unit['Jobs'])
                self.assertEqual(Tier1ReRecoWorkload, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
                self.assertEqual(4, unit['NumberOfLumis'])
                self.assertEqual(10, unit['NumberOfFiles'])
                self.assertEqual(10000, unit['NumberOfEvents'])
            self.assertEqual(len(units),
                             len(dbs[inputDataset.dbsurl].getFileBlocksInfo(dataset)))

        # Modify the spec and task, get first a fresh policy instance
        policyInstance = Block(**self.splitArgs)
        for task in Tier1ReRecoWorkload.taskIterator():
            policyInstance.modifyPolicyForWorkAddition({'ProcessedInputs' : inputs.keys()})
            self.assertRaises(WorkQueueNoWorkError, policyInstance, Tier1ReRecoWorkload, task)

        # Pop up 2 more blocks for the dataset with different statistics
        Globals.GlobalParams.setNumOfBlocksPerDataset(Globals.GlobalParams.numOfBlocksPerDataset() + 2)
        Globals.GlobalParams.setNumOfFilesPerBlock(10) # Emulator is crooked, it gives the sum of all the files in the dataset not block


        # Now run another pass of the Block policy
        policyInstance = Block(**self.splitArgs)
        policyInstance.modifyPolicyForWorkAddition({'ProcessedInputs' : inputs.keys()})
        for task in Tier1ReRecoWorkload.taskIterator():
            units, rejectedWork = policyInstance(Tier1ReRecoWorkload, task)
            self.assertEqual(2, len(units))
            self.assertEqual(0, len(rejectedWork))
            for unit in units:
                blocks.extend(unit['Inputs'].keys())
                inputs.update(unit['Inputs'])
                self.assertEqual(69, unit['Priority'])
                self.assertEqual(1, unit['Jobs'])
                self.assertEqual(Tier1ReRecoWorkload, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
                self.assertEqual(8, unit['NumberOfLumis'])
                self.assertEqual(40, unit['NumberOfFiles'])
                self.assertEqual(40000, unit['NumberOfEvents'])
            self.assertEqual(len(units),
                             len(dbs[inputDataset.dbsurl].getFileBlocksInfo(dataset)) - 2)

        # Run one last time
        policyInstance = Block(**self.splitArgs)
        for task in Tier1ReRecoWorkload.taskIterator():
            policyInstance.modifyPolicyForWorkAddition({'ProcessedInputs' : inputs.keys()})
            self.assertRaises(WorkQueueNoWorkError, policyInstance, Tier1ReRecoWorkload, task)

        return

if __name__ == '__main__':
    unittest.main()
