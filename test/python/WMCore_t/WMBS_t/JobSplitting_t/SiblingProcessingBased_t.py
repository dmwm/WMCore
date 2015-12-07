#!/usr/bin/env python
"""
_SiblingProcessingBased_t_

Test SiblingProcessing job splitting.
"""

import unittest
import threading

from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow

from WMCore.DAOFactory import DAOFactory
from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMQuality.TestInit import TestInit

class SiblingProcessingBasedTest(unittest.TestCase):
    """
    _SiblingProcessingBasedTest_

    Test SiblingProcessing job splitting.
    """
    def setUp(self):
        """
        _setUp_

        Setup the database connections and schema.
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)

        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)

        locationAction = daofactory(classname = "Locations.New")
        locationAction.execute("T2_CH_CERN", pnn = "T2_CH_CERN")
        locationAction.execute("T1_US_FNAL", pnn = "T1_US_FNAL_Disk")

        self.testFilesetA = Fileset(name = "FilesetA")
        self.testFilesetA.create()
        self.testFilesetB = Fileset(name = "FilesetB")
        self.testFilesetB.create()

        self.testFileA = File("testFileA", size = 1000, events = 100,
                              locations = set(["T2_CH_CERN"]))
        self.testFileA.create()
        self.testFileB = File("testFileB", size = 1000, events = 100,
                              locations = set(["T2_CH_CERN"]))
        self.testFileB.create()
        self.testFileC = File("testFileC", size = 1000, events = 100,
                              locations = set(["T2_CH_CERN"]))
        self.testFileC.create()

        self.testFilesetA.addFile(self.testFileA)
        self.testFilesetA.addFile(self.testFileB)
        self.testFilesetA.addFile(self.testFileC)
        self.testFilesetA.commit()

        self.testFileD = File("testFileD", size = 1000, events = 100,
                              locations = set(["T2_CH_CERN"]))
        self.testFileD.create()
        self.testFileE = File("testFileE", size = 1000, events = 100,
                              locations = set(["T2_CH_CERN"]))
        self.testFileE.create()
        self.testFileF = File("testFileF", size = 1000, events = 100,
                              locations = set(["T2_CH_CERN"]))
        self.testFileF.create()

        self.testFilesetB.addFile(self.testFileD)
        self.testFilesetB.addFile(self.testFileE)
        self.testFilesetB.addFile(self.testFileF)
        self.testFilesetB.commit()

        testWorkflowA = Workflow(spec = "specA.xml", owner = "Steve",
                                 name = "wfA", task = "Test")
        testWorkflowA.create()
        testWorkflowB = Workflow(spec = "specB.xml", owner = "Steve",
                                 name = "wfB", task = "Test")
        testWorkflowB.create()
        testWorkflowC = Workflow(spec = "specC.xml", owner = "Steve",
                                 name = "wfC", task = "Test")
        testWorkflowC.create()
        testWorkflowD = Workflow(spec = "specD.xml", owner = "Steve",
                                 name = "wfD", task = "Test")
        testWorkflowD.create()

        self.testSubscriptionA = Subscription(fileset = self.testFilesetA,
                                              workflow = testWorkflowA,
                                              split_algo = "FileBased",
                                              type = "Processing")
        self.testSubscriptionA.create()
        self.testSubscriptionB = Subscription(fileset = self.testFilesetB,
                                              workflow = testWorkflowB,
                                              split_algo = "FileBased",
                                              type = "Processing")
        self.testSubscriptionB.create()
        self.testSubscriptionC = Subscription(fileset = self.testFilesetB,
                                              workflow = testWorkflowC,
                                              split_algo = "FileBased",
                                              type = "Processing")
        self.testSubscriptionC.create()
        self.testSubscriptionD = Subscription(fileset = self.testFilesetB,
                                              workflow = testWorkflowD,
                                              split_algo = "FileBased",
                                              type = "Processing")
        self.testSubscriptionD.create()

        deleteWorkflow = Workflow(spec = "specE.xml", owner = "Steve",
                                  name = "wfE", task = "Test")
        deleteWorkflow.create()

        self.deleteSubscriptionA = Subscription(fileset = self.testFilesetA,
                                                workflow = deleteWorkflow,
                                                split_algo = "SiblingProcessingBased",
                                                type = "Cleanup")
        self.deleteSubscriptionA.create()
        self.deleteSubscriptionB = Subscription(fileset = self.testFilesetB,
                                                workflow = deleteWorkflow,
                                                split_algo = "SiblingProcessingBased",
                                                type = "Cleanup")
        self.deleteSubscriptionB.create()
        return

    def tearDown(self):
        """
        _tearDown_

        Clear out WMBS.
        """
        self.testInit.clearDatabase()
        return

    def testSiblingProcessing(self):
        """
        _testSiblingProcessing_

        Verify that the sibling processing split works correctly dealing with
        failed files and acquiring files correctly.
        """
        splitter = SplitterFactory()
        deleteFactoryA = splitter(package = "WMCore.WMBS",
                                  subscription = self.deleteSubscriptionA)
        deleteFactoryB = splitter(package = "WMCore.WMBS",
                                  subscription = self.deleteSubscriptionB)

        result = deleteFactoryA()

        assert len(result) == 0, \
               "Error: No jobs should be returned."

        result = deleteFactoryB()

        assert len(result) == 0, \
               "Error: No jobs should be returned."

        self.testSubscriptionA.completeFiles(self.testFileA)

        result = deleteFactoryA(files_per_job = 1)

        assert len(result) == 1, \
               "Error: Only one jobgroup should be returned."
        assert len(result[0].jobs) == 1, \
               "Error: There should only be one job in the jobgroup."
        assert result[0].jobs[0]["possiblePSN"] == set(["T2_CH_CERN"]), \
               "Error: possiblePSN is wrong."
        assert len(result[0].jobs[0]["input_files"]) == 1, \
               "Error: Job should only have one input file."
        assert result[0].jobs[0]["input_files"][0]["lfn"] == "testFileA", \
               "Error: Input file for job is wrong."

        result = deleteFactoryB(files_per_job = 1)

        assert len(result) == 0, \
               "Error: Second subscription should have no jobs."

        result = deleteFactoryA(files_per_job = 1)

        assert len(result) == 0, \
               "Error: No jobs should have been created."

        self.testSubscriptionB.completeFiles(self.testFileD)
        self.testSubscriptionC.failFiles(self.testFileD)

        result = deleteFactoryA(files_per_job = 1)

        assert len(result) == 0, \
               "Error: No jobs should have been created."

        result = deleteFactoryB(files_per_job = 1)

        assert len(result) == 0, \
               "Error: No jobs should have been created."

        self.testSubscriptionD.failFiles(self.testFileD)

        result = deleteFactoryA(files_per_job = 1)

        assert len(result) == 0, \
               "Error: No jobs should have been created."

        result = deleteFactoryB(files_per_job = 1)

        assert len(result) == 0, \
               "Error: No job groups should have been created."

        self.testSubscriptionB.completeFiles([self.testFileE, self.testFileF])
        self.testSubscriptionC.completeFiles([self.testFileE, self.testFileF])
        self.testSubscriptionD.completeFiles([self.testFileE, self.testFileF])

        result = deleteFactoryB(files_per_job = 10)

        assert len(result) == 0, \
               "Error: No jobs should have been created."

        self.testFilesetB.markOpen(False)

        result = deleteFactoryB(files_per_job = 10)

        assert len(result) == 1, \
               "Error: One jobgroup should have been returned."
        assert len(result[0].jobs) == 1, \
               "Error: There should only be one job in the jobgroup."
        assert len(result[0].jobs[0]["input_files"]) == 2, \
               "Error: Job should only have one input file."

        lfns = [result[0].jobs[0]["input_files"][0]["lfn"], result[0].jobs[0]["input_files"][1]["lfn"]]

        assert "testFileE" in lfns, \
               "Error: TestFileE missing from job input."
        assert "testFileF" in lfns, \
               "Error: TestFileF missing from job input."

        self.assertEqual(len(self.deleteSubscriptionB.availableFiles()), 0,
                         "Error: There should be no available files.")

        completeFiles = self.deleteSubscriptionB.filesOfStatus("Completed")
        self.assertEqual(len(completeFiles), 1,
                         "Error: There should only be one complete file.")
        self.assertEqual(list(completeFiles)[0]["lfn"], "testFileD",
                         "Error: Test file D should be complete.")

        return

    def testMultipleLocations(self):
        """
        _testMultipleLocations_

        Verify that the sibling processing based algorithm doesn't create jobs
        that run over files at multiple sites.
        """
        testFile1 = File("testFile1", size = 1000, events = 100,
                         locations = set(["T1_US_FNAL_Disk"]))
        testFile1.create()
        testFile2 = File("testFile2", size = 1000, events = 100,
                         locations = set(["T1_US_FNAL_Disk"]))
        testFile2.create()
        testFile3 = File("testFile3", size = 1000, events = 100,
                         locations = set(["T1_US_FNAL_Disk"]))
        testFile3.create()

        self.testFilesetA.addFile(testFile1)
        self.testFilesetA.addFile(testFile2)
        self.testFilesetA.addFile(testFile3)
        self.testFilesetA.commit()
        self.testFilesetA.markOpen(False)

        self.testSubscriptionA.completeFiles([testFile1, testFile2, testFile3])
        self.testSubscriptionA.completeFiles([self.testFileA, self.testFileB, self.testFileC])

        splitter = SplitterFactory()
        deleteFactoryA = splitter(package = "WMCore.WMBS",
                                  subscription = self.deleteSubscriptionA)

        result = deleteFactoryA(files_per_job = 50)

        assert len(result) == 2, \
               "Error: Wrong number of jobgroups returned."

        goldenFilesA = ["testFileA", "testFileB", "testFileC"]
        goldenFilesB = ["testFile1", "testFile2", "testFile3"]

        for jobGroup in result:
            assert len(jobGroup.jobs) == 1, \
                   "Error: Wrong number of jobs in jobgroup."
            assert len(jobGroup.jobs[0]["input_files"]) == 3, \
                   "Error: Wrong number of input files in job."

            jobSite = jobGroup.jobs[0]["possiblePSN"]

            assert (jobSite == set(["T2_CH_CERN"])
                    or jobSite == set(["T1_US_FNAL"])), \
                    "Error: Wrong site for job."

            if jobSite == set(["T2_CH_CERN"]):
                goldenFiles = goldenFilesA
            else:
                goldenFiles = goldenFilesB

            for jobFile in jobGroup.jobs[0]["input_files"]:
                goldenFiles.remove(jobFile["lfn"])

            assert len(goldenFiles) == 0,  \
                   "Error: Files are missing."

        return

    def testLargeNumberOfFiles(self):
        """
        _testLargeNumberOfFiles_

        Setup a subscription with 500 files and verify that the splitting algo
        works correctly.
        """
        testWorkflowA = Workflow(spec = "specA.xml", owner = "Steve",
                                 name = "wfA", task = "Test")
        testWorkflowA.create()
        testWorkflowB = Workflow(spec = "specB.xml", owner = "Steve",
                                 name = "wfB", task = "Test")
        testWorkflowB.create()

        testFileset = Fileset(name = "TestFileset")
        testFileset.create()

        allFiles = []
        for i in range(500):
            testFile = File(str(i), size = 1000, events = 100,
                            locations = set(["T2_CH_CERN"]))
            testFile.create()
            allFiles.append(testFile)
            testFileset.addFile(testFile)
        testFileset.commit()

        testSubscriptionA = Subscription(fileset = testFileset,
                                         workflow = testWorkflowA,
                                         split_algo = "FileBased",
                                         type = "Processing")
        testSubscriptionA.create()
        testSubscriptionB = Subscription(fileset = testFileset,
                                         workflow = testWorkflowB,
                                         split_algo = "SiblingProcessingBased",
                                         type = "Processing")
        testSubscriptionB.create()

        testSubscriptionA.completeFiles(allFiles)

        splitter = SplitterFactory()
        deleteFactoryA = splitter(package = "WMCore.WMBS",
                                  subscription = testSubscriptionB)

        result = deleteFactoryA(files_per_job = 50)
        self.assertEqual(len(result), 1,
                         "Error: Wrong number of job groups returned.")
        self.assertEqual(len(result[0].jobs), 10,
                         "Error: Wrong number of jobs returned.")

        return

    def testFilesWithoutOtherSubscriptions(self):
        """
        _testFilesWithoutOtherSubscriptions_

        Test the case where files only in the delete subscription
        can happen if cleanup of the other subscriptions is fast

        """
        testWorkflowA = Workflow(spec = "specA.xml", owner = "Steve",
                                 name = "wfA", task = "Test")
        testWorkflowA.create()

        testFileset = Fileset(name = "TestFileset")
        testFileset.create()

        allFiles = []
        for i in range(500):
            testFile = File(str(i), size = 1000, events = 100,
                            locations = set(["T2_CH_CERN"]))
            testFile.create()
            allFiles.append(testFile)
            testFileset.addFile(testFile)
        testFileset.commit()

        testSubscriptionA = Subscription(fileset = testFileset,
                                         workflow = testWorkflowA,
                                         split_algo = "SiblingProcessingBased",
                                         type = "Processing")
        testSubscriptionA.create()

        splitter = SplitterFactory()
        deleteFactoryA = splitter(package = "WMCore.WMBS",
                                  subscription = testSubscriptionA)

        result = deleteFactoryA(files_per_job = 50)
        self.assertEqual(len(result), 1,
                         "Error: Wrong number of job groups returned.")
        self.assertEqual(len(result[0].jobs), 10,
                         "Error: Wrong number of jobs returned.")

        return

if __name__ == '__main__':
    unittest.main()
