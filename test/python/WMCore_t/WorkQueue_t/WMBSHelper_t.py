#!/usr/bin/env python
"""
_WMBSHelper_t_

"""

import unittest
import os
import threading

from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Job import Job

from WMCore.DAOFactory import DAOFactory

from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMCore.WorkQueue.WMBSHelper import killWorkflow

from WorkQueueTestCase import WorkQueueTestCase

from WMCore_t.WorkQueue_t.WorkQueue_t import TestReRecoFactory, rerecoArgs
from WMCore_t.WorkQueue_t.WorkQueue_t import getFirstTask
from WMCore_t.WorkQueue_t.MockDBSReader import MockDBSReader

class WMBSHelperTest(WorkQueueTestCase):
    def setUp(self):
        """
        _setUp_

        """
        WorkQueueTestCase.setUp(self)
        self.wmspec = self.createWMSpec()
        self.topLevelTask = getFirstTask(self.wmspec)
        self.inputDataset = self.topLevelTask.inputDataset()
        self.dataset = self.topLevelTask.getInputDatasetPath()
        self.dbs = MockDBSReader(self.inputDataset.dbsurl, self.dataset)
        return
        
    def tearDown(self):
        """
        _tearDown_

        Clear out the database.
        """
        WorkQueueTestCase.tearDown(self)
        return

    def setupForKillTest(self):
        """
        _setupForKillTest_

        Inject a workflow into WMBS that has a processing task, a merge task and
        a cleanup task.  Inject files into the various tasks at various
        processing states (acquired, complete, available...).  Also create jobs
        for each subscription in various states.
        """
        myThread = threading.currentThread()
        daoFactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)

        locationAction = daoFactory(classname = "Locations.New")
        changeStateAction = daoFactory(classname = "Jobs.ChangeState")
        locationAction.execute(siteName = "site1", seName = "goodse.cern.ch")

        inputFileset = Fileset("input")
        inputFileset.create()

        inputFileA = File("lfnA", locations = "goodse.cern.ch")
        inputFileB = File("lfnB", locations = "goodse.cern.ch")
        inputFileC = File("lfnC", locations = "goodse.cern.ch")
        inputFileA.create()
        inputFileB.create()
        inputFileC.create()

        inputFileset.addFile(inputFileA)
        inputFileset.addFile(inputFileB)
        inputFileset.addFile(inputFileC)
        inputFileset.commit()
        
        unmergedOutputFileset = Fileset("unmerged")        
        unmergedOutputFileset.create()

        unmergedFileA = File("lfnA", locations = "goodse.cern.ch")
        unmergedFileB = File("lfnB", locations = "goodse.cern.ch")
        unmergedFileC = File("lfnC", locations = "goodse.cern.ch")
        unmergedFileA.create()
        unmergedFileB.create()
        unmergedFileC.create()        

        unmergedOutputFileset.addFile(unmergedFileA)
        unmergedOutputFileset.addFile(unmergedFileB)
        unmergedOutputFileset.addFile(unmergedFileC)
        unmergedOutputFileset.commit()

        mainProcWorkflow = Workflow(spec = "spec1", owner = "Steve",
                                    name = "Main", task = "Proc")
        mainProcWorkflow.create()
        mainProcMergeWorkflow = Workflow(spec = "spec1", owner = "Steve",
                                         name = "Main", task = "ProcMerge")
        mainProcMergeWorkflow.create()
        mainCleanupWorkflow = Workflow(spec = "spec1", owner = "Steve",
                                       name = "Main", task = "Cleanup")
        mainCleanupWorkflow.create()

        self.mainProcSub = Subscription(fileset = inputFileset,
                                        workflow = mainProcWorkflow,
                                        type = "Processing")
        self.mainProcSub.create()
        self.mainProcSub.acquireFiles(inputFileA)
        self.mainProcSub.completeFiles(inputFileB)

        procJobGroup = JobGroup(subscription = self.mainProcSub)
        procJobGroup.create()
        self.procJobA = Job(name = "ProcJobA")
        self.procJobA["state"] = "new"
        self.procJobB = Job(name = "ProcJobB")
        self.procJobB["state"] = "executing"
        self.procJobC = Job(name = "ProcJobC")
        self.procJobC["state"] = "complete"
        self.procJobA.create(procJobGroup)
        self.procJobB.create(procJobGroup)
        self.procJobC.create(procJobGroup)

        self.mainMergeSub = Subscription(fileset = unmergedOutputFileset,
                                         workflow = mainProcMergeWorkflow,
                                         type = "Merge")
        self.mainMergeSub.create()
        self.mainMergeSub.acquireFiles(unmergedFileA)
        self.mainMergeSub.failFiles(unmergedFileB)

        mergeJobGroup = JobGroup(subscription = self.mainMergeSub)
        mergeJobGroup.create()
        self.mergeJobA = Job(name = "MergeJobA")
        self.mergeJobA["state"] = "exhausted"
        self.mergeJobB = Job(name = "MergeJobB")
        self.mergeJobB["state"] = "cleanout"
        self.mergeJobC = Job(name = "MergeJobC")
        self.mergeJobC["state"] = "new"
        self.mergeJobA.create(mergeJobGroup)
        self.mergeJobB.create(mergeJobGroup)
        self.mergeJobC.create(mergeJobGroup)
        
        self.mainCleanupSub = Subscription(fileset = unmergedOutputFileset,
                                           workflow = mainCleanupWorkflow,
                                           type = "Cleanup")
        self.mainCleanupSub.create()
        self.mainCleanupSub.acquireFiles(unmergedFileA)
        self.mainCleanupSub.completeFiles(unmergedFileB)

        cleanupJobGroup = JobGroup(subscription = self.mainCleanupSub)
        cleanupJobGroup.create()
        self.cleanupJobA = Job(name = "CleanupJobA")
        self.cleanupJobA["state"] = "new"
        self.cleanupJobB = Job(name = "CleanupJobB")
        self.cleanupJobB["state"] = "executing"
        self.cleanupJobC = Job(name = "CleanupJobC")
        self.cleanupJobC["state"] = "complete"
        self.cleanupJobA.create(cleanupJobGroup)
        self.cleanupJobB.create(cleanupJobGroup)
        self.cleanupJobC.create(cleanupJobGroup)

        changeStateAction.execute([self.procJobA, self.procJobB, self.procJobC,
                                   self.mergeJobA, self.mergeJobB, self.mergeJobC,
                                   self.cleanupJobA, self.cleanupJobB, self.cleanupJobC])
        return
        
    def verifyFileKillStatus(self):
        """
        _verifyFileKillStatus_

        Verify that all files were killed correctly.  The status of files in
        Cleanup and LogCollect subscriptions isn't modified.  Status of
        already completed and failed files is not modified.
        """
        failedFiles = self.mainProcSub.filesOfStatus("Failed")
        acquiredFiles = self.mainProcSub.filesOfStatus("Acquired")
        completedFiles = self.mainProcSub.filesOfStatus("Completed")
        availableFiles = self.mainProcSub.filesOfStatus("Available")

        self.assertEqual(len(availableFiles), 0, \
                         "Error: There should be no available files.")
        self.assertEqual(len(acquiredFiles), 0, \
                         "Error: There should be no acquired files.")

        self.assertEqual(len(completedFiles), 1, \
                         "Error: There should be only one completed file.")
        self.assertEqual(list(completedFiles)[0]["lfn"], "lfnB", \
                         "Error: Wrong completed LFN.")

        self.assertEqual(len(failedFiles), 2, \
                         "Error: There should be only two failed files.")
        goldenLFNs = ["lfnA", "lfnC"]
        for failedFile in failedFiles:
            self.assertTrue(failedFile["lfn"] in goldenLFNs, \
                          "Error: Extra failed file.")
            goldenLFNs.remove(failedFile["lfn"])

        self.assertEqual(len(goldenLFNs), 0, \
                         "Error: Missing LFN")

        failedFiles = self.mainMergeSub.filesOfStatus("Failed")
        acquiredFiles = self.mainMergeSub.filesOfStatus("Acquired")
        completedFiles = self.mainMergeSub.filesOfStatus("Completed")
        availableFiles = self.mainMergeSub.filesOfStatus("Available")

        self.assertEqual(len(acquiredFiles), 0, \
                         "Error: Merge subscription should have 0 acq files.")
        self.assertEqual(len(availableFiles), 0, \
                         "Error: Merge subscription should have 0 avail files.") 
        self.assertEqual(len(completedFiles), 0, \
                         "Error: Merge subscription should have 0 compl files.")

        self.assertEqual(len(failedFiles), 3, \
                         "Error: There should be only three failed files.")
        goldenLFNs = ["lfnA", "lfnB", "lfnC"]
        for failedFile in failedFiles:
            self.assertTrue(failedFile["lfn"] in goldenLFNs, \
                          "Error: Extra failed file.")
            goldenLFNs.remove(failedFile["lfn"])

        self.assertEqual(len(goldenLFNs), 0, \
                         "Error: Missing LFN")

        failedFiles = self.mainCleanupSub.filesOfStatus("Failed")
        acquiredFiles = self.mainCleanupSub.filesOfStatus("Acquired")
        completedFiles = self.mainCleanupSub.filesOfStatus("Completed")
        availableFiles = self.mainCleanupSub.filesOfStatus("Available")

        self.assertEqual(len(failedFiles), 0, \
                         "Error: Cleanup subscription should have 0 fai files.")

        self.assertEqual(len(acquiredFiles), 1, \
                         "Error: There should be only one acquired file.")
        self.assertEqual(list(acquiredFiles)[0]["lfn"], "lfnA", \
                         "Error: Wrong completed LFN.")

        self.assertEqual(len(completedFiles), 1, \
                         "Error: There should be only one completed file.")
        self.assertEqual(list(completedFiles)[0]["lfn"], "lfnB", \
                         "Error: Wrong completed LFN.")

        self.assertEqual(len(availableFiles), 1, \
                         "Error: There should be only one available file.")
        self.assertEqual(list(availableFiles)[0]["lfn"], "lfnC", \
                         "Error: Wrong completed LFN.")

        return

    def verifyJobKillStatus(self):
        """
        _verifyJobKillStatus_

        Verify that jobs are killed correctly.  Jobs belonging to Cleanup and
        LogCollect subscriptions are not killed.  The status of jobs that have
        already finished running is not changed.
        """
        self.procJobA.load()
        self.procJobB.load()
        self.procJobC.load()

        self.assertEqual(self.procJobA["state"], "killed", \
                         "Error: Proc job A should be killed.")
        self.assertEqual(self.procJobB["state"], "killed", \
                         "Error: Proc job B should be killed.")
        self.assertEqual(self.procJobC["state"], "complete", \
                         "Error: Proc job C should be complete.")

        self.mergeJobA.load()
        self.mergeJobB.load()
        self.mergeJobC.load()

        self.assertEqual(self.mergeJobA["state"], "exhausted", \
                         "Error: Merge job A should be exhausted.")
        self.assertEqual(self.mergeJobB["state"], "cleanout", \
                         "Error: Merge job B should be cleanout.")
        self.assertEqual(self.mergeJobC["state"], "killed", \
                         "Error: Merge job C should be killed.")

        self.cleanupJobA.load()
        self.cleanupJobB.load()
        self.cleanupJobC.load()

        self.assertEqual(self.cleanupJobA["state"], "new", \
                         "Error: Cleanup job A should be new.")
        self.assertEqual(self.cleanupJobB["state"], "executing", \
                         "Error: Cleanup job B should be executing.")
        self.assertEqual(self.cleanupJobC["state"], "complete", \
                         "Error: Cleanup job C should be complete.")
        return

    def testKillWorkflow(self):
        """
        _testKillWorkflow_

        Verify that workflow killing works correctly.
        """
        self.setupForKillTest()
        killWorkflow("Main", os.getenv("COUCHURL"), "wmbshelper_t")

        self.verifyFileKillStatus()
        self.verifyJobKillStatus()
        return
    
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
                          [], [], block, None)
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

if __name__ == '__main__':
    unittest.main()
