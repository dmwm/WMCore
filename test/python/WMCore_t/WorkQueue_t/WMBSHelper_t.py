#!/usr/bin/env python
"""
    WorkQueue tests
"""




import unittest

from WMCore.WorkQueue.WMBSHelper import WMBSHelper

from WorkQueueTestCase import WorkQueueTestCase

#from WMCore_t.WMSpec_t.samples.BasicProductionWorkload import workload as BasicProductionWorkload
from WMCore_t.WorkQueue_t.WorkQueue_t import TestReRecoFactory, rerecoArgs
from WMCore_t.WorkQueue_t.WorkQueue_t import getFirstTask
from WMCore_t.WorkQueue_t.MockDBSReader import MockDBSReader

#TODO: This is a stub, it needs major fleshing out.

# pylint: disable=E1103


class WMBSHelperTest(WorkQueueTestCase):
    """
    _WMBSHelperTest_

    """
    def setUp(self):
        """
        setup things - db etc
        """
        WorkQueueTestCase.setUp(self)
        self.wmspec = self.createWMSpec()
        self.topLevelTask = getFirstTask(self.wmspec)
        self.inputDataset = self.topLevelTask.inputDataset()
        self.dataset = self.topLevelTask.getInputDatasetPath()
        self.dbs = MockDBSReader(self.inputDataset.dbsurl, self.dataset)
        
        
    def tearDown(self):
        """tearDown"""
        WorkQueueTestCase.tearDown(self)
        pass

    def createWMSpec(self, name = 'ReRecoWorkload', args = rerecoArgs):
        
        wmspec = TestReRecoFactory()(name, args)
        
        return wmspec 
    
    def getDBS(self, wmspec):
        topLevelTask = getFirstTask(wmspec.taskIterator())
        inputDataset = topLevelTask.inputDataset()
        dataset = topLevelTask.getInputDatasetPath()
        dbs = MockDBSReader(inputDataset.dbsurl, dataset)
        #dbsDict = {self.inputDataset.dbsurl : self.dbs}
        return dbs
        
        
    def createWMBSHelperWithTopTask(self, wmspec, block):
        
        topLevelTask = getFirstTask(wmspec)
         
        wmbs = WMBSHelper(wmspec, '/somewhere',
                          "whatever", topLevelTask.name(), 
                          topLevelTask.taskType(),
                          [], [], block)
        wmbs.createSubscription()
        
        return wmbs

#    def testProduction(self):
#        """Production workflow"""
#        pass

    def testReReco(self):
        """ReReco workflow"""
        # create workflow
        block = self.dataset + "#1"
        wmbs = self.createWMBSHelperWithTopTask(self.wmspec, block)
        files = wmbs.validFiles(self.dbs.getFileBlock(block))
        self.assertEqual(len(files), 1)

    def testReRecoBlackRunRestriction(self):
        """ReReco workflow with Run restrictions"""
        block = self.dataset + "#2"
        #add run blacklist
        self.topLevelTask.setInputRunBlacklist([1, 2, 3, 4])
        wmbs = self.createWMBSHelperWithTopTask(self.wmspec, block)
        
        files = wmbs.validFiles(self.dbs.getFileBlock(block)[block]['Files'])
        self.assertEqual(len(files), 0)


    def testReRecoWhiteRunRestriction(self):
        block = self.dataset + "#2"
        # Run Whitelist
        self.topLevelTask.setInputRunWhitelist([2, 3])
        wmbs = self.createWMBSHelperWithTopTask(self.wmspec, block)
        files = wmbs.validFiles(self.dbs.getFileBlock(block)[block]['Files'])
        self.assertEqual(len(files), 1)
        
    def testDuplicateFileInsert(self):
        # using default wmspec
        block = self.dataset + "#1"
        wmbs = self.createWMBSHelperWithTopTask(self.wmspec, block)
        numOfFiles = wmbs.addFiles(self.dbs.getFileBlock(block)[block])
        # check initially inserted files.
        dbsFiles = self.dbs.getFileBlock(block)[block]['Files']
        self.assertEqual(numOfFiles, len(dbsFiles))
        firstFileset = wmbs.topLevelFileset
        wmbsDao = wmbs.daofactory(classname = "Files.InFileset")
        
        numOfFiles = len(wmbsDao.execute(firstFileset.id))
        self.assertEqual(numOfFiles, len(dbsFiles))
        
        # use the new spec with same inputdataset
        block = self.dataset + "#1"
        wmspec = self.createWMSpec("TestSpec1")
        dbs = self.getDBS(wmspec)
        wmbs = self.createWMBSHelperWithTopTask(wmspec, block)
        # check duplicate insert
        dbsFiles = dbs.getFileBlock(block)[block]['Files']
        numOfFiles = wmbs.addFiles(dbs.getFileBlock(block)[block])
        self.assertEqual(numOfFiles, 0)
        secondFileset = wmbs.topLevelFileset
        
        wmbsDao = wmbs.daofactory(classname = "Files.InFileset")
        numOfFiles = len(wmbsDao.execute(secondFileset.id))
        self.assertEqual(numOfFiles, len(dbsFiles))
        
        self.assertNotEqual(firstFileset.id, secondFileset.id)
    
    def testParentage(self):
        """
        TODO: add the parentage test. 
        1. check whether parent files are created in wmbs.
        2. check parent files are associated to child.
        3. When 2 specs with the same input data (one with parent processing, one without it)
           is inserted, if one without parent processing inserted first then the other with 
           parent processing insert, it still needs to create parent files although child files
           are duplicate 
        """
        pass
        
# pylint: enable=E1103

if __name__ == '__main__':
    unittest.main()