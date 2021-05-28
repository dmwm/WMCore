#!/usr/bin/env python
"""
_Job_t_

Unit tests for the WMBS job class.
"""
from __future__ import absolute_import

from builtins import str
import threading
import unittest

from WMCore.DataStructs.Run import Run
from WMCore.Services.UUIDLib import makeUUID
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset as Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Mask import Mask
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.WorkUnit import WorkUnit
from WMCore.WMBS.Workflow import Workflow
from WMCore_t.WMBS_t.JobTestBase import JobTestBase


class JobTest(JobTestBase):
    def testCreateDeleteExists(self):
        """
        _testCreateDeleteExists_

        Create and then delete a job.  Use the job class's exists() method to
        determine if the job has been written to the database before it is
        created, after it has been created and after it has been deleted.
        """
        testWorkflow = Workflow(spec="spec.xml", owner="Simon",
                                name="wf001", task="Test")
        testWorkflow.create()

        testWMBSFileset = Fileset(name="TestFileset")
        testWMBSFileset.create()

        testSubscription = Subscription(fileset=testWMBSFileset,
                                        workflow=testWorkflow)
        testSubscription.create()

        testJobGroup = JobGroup(subscription=testSubscription)
        testJobGroup.create()

        testFileA = File(lfn="/this/is/a/lfnA", size=1024, events=10)
        testFileB = File(lfn="/this/is/a/lfnB", size=1024, events=10)
        testFileA.create()
        testFileB.create()

        testJob = Job(name="TestJob", files=[testFileA, testFileB])
        # testWU = WorkUnit(taskID=testWorkflow.id, fileid=testFileA['id'], runLumi=Run(1, *[44]))

        self.assertFalse(testJob.exists(), "Job exists before it was created")
        # self.assertFalse(testWU.exists(), "WorkUnit exists before it was created")

        testJob.create(group=testJobGroup)

        self.assertTrue(testJob.exists(), "Job does not exist after it was created")
        # self.assertFalse(testWU.exists(), "WorkUnit exists when there is no work")

        # Test the getWorkflow method
        workflow = testJob.getWorkflow()
        self.assertEqual(workflow['task'], 'Test')
        self.assertEqual(workflow['name'], 'wf001')

        testJob.delete()

        self.assertFalse(testJob.exists(), "Job exists after it was deleted")
        # self.assertFalse(testWU.exists(), "WorkUnit exists after job is deleted")

        return

    def testCreateTransaction(self):
        """
        _testCreateTransaction_

        Create a job and save it to the database.  Roll back the database
        transaction and verify that the job is no longer in the database.
        """
        testWorkflow = Workflow(spec="spec.xml", owner="Simon",
                                name="wf001", task="Test")
        testWorkflow.create()

        testWMBSFileset = Fileset(name="TestFileset")
        testWMBSFileset.create()

        testSubscription = Subscription(fileset=testWMBSFileset,
                                        workflow=testWorkflow)
        testSubscription.create()

        testJobGroup = JobGroup(subscription=testSubscription)
        testJobGroup.create()

        testFileA = File(lfn="/this/is/a/lfnA", size=1024, events=10)
        testFileB = File(lfn="/this/is/a/lfnB", size=1024, events=10)
        testFileA.create()
        testFileB.create()

        myThread = threading.currentThread()
        myThread.transaction.begin()

        testJob = Job(name="TestJob", files=[testFileA, testFileB])

        assert testJob.exists() is False, \
            "ERROR: Job exists before it was created"

        testJob.create(group=testJobGroup)

        assert testJob.exists() >= 0, \
            "ERROR: Job does not exist after it was created"

        myThread.transaction.rollback()

        assert testJob.exists() is False, \
            "ERROR: Job exists after transaction was rolled back."

        return

    def testDeleteTransaction(self):
        """
        _testDeleteTransaction_

        Create a new job and commit it to the database.  Start a new transaction
        and delete the file from the database.  Verify that the file has been
        deleted.  After that, roll back the transaction and verify that the
        job is once again in the database.
        """
        testWorkflow = Workflow(spec="spec.xml", owner="Simon",
                                name="wf001", task="Test")
        testWorkflow.create()

        testWMBSFileset = Fileset(name="TestFileset")
        testWMBSFileset.create()

        testSubscription = Subscription(fileset=testWMBSFileset,
                                        workflow=testWorkflow)
        testSubscription.create()

        testJobGroup = JobGroup(subscription=testSubscription)
        testJobGroup.create()

        testFileA = File(lfn="/this/is/a/lfnA", size=1024, events=10)
        testFileB = File(lfn="/this/is/a/lfnB", size=1024, events=10)
        testFileA.create()
        testFileB.create()

        testJob = Job(name="TestJob", files=[testFileA, testFileB])

        assert testJob.exists() is False, \
            "ERROR: Job exists before it was created"

        testJob.create(group=testJobGroup)

        assert testJob.exists() >= 0, \
            "ERROR: Job does not exist after it was created"

        myThread = threading.currentThread()
        myThread.transaction.begin()

        testJob.delete()

        assert testJob.exists() is False, \
            "ERROR: Job exists after it was delete"

        myThread.transaction.rollback()

        assert testJob.exists() >= 0, \
            "ERROR: Job does not exist after transaction was rolled back."

        return

    def testCreateDeleteExistsNoFiles(self):
        """
        _testCreateDeleteExistsNoFiles_

        Create and then delete a job but don't add any input files to it.
        Use the job class's exists() method to determine if the job has been
        written to the database before it is created, after it has been created
        and after it has been deleted.
        """
        testWorkflow = Workflow(spec="spec.xml", owner="Simon",
                                name="wf001", task="Test")
        testWorkflow.create()

        testWMBSFileset = Fileset(name="TestFileset")
        testWMBSFileset.create()

        testSubscription = Subscription(fileset=testWMBSFileset,
                                        workflow=testWorkflow)
        testSubscription.create()

        testJobGroup = JobGroup(subscription=testSubscription)
        testJobGroup.create()

        testJob = Job(name="TestJob")

        assert testJob.exists() is False, \
            "ERROR: Job exists before it was created"

        testJob.create(group=testJobGroup)

        assert testJob.exists() >= 0, \
            "ERROR: Job does not exist after it was created"

        testJob.delete()

        assert testJob.exists() is False, \
            "ERROR: Job exists after it was delete"

        return

    def testLoad(self):
        """
        _testLoad_

        Create a job and save it to the database.  Load it back from the
        database using the name and the id and then verify that all information
        was loaded correctly.
        """
        testJobA = self.createTestJob()
        testJobB = Job(id=testJobA["id"])
        testJobC = Job(name=testJobA["name"])
        testJobB.load()
        testJobC.load()

        assert (testJobA["id"] == testJobB["id"]) and \
               (testJobA["name"] == testJobB["name"]) and \
               (testJobA["jobgroup"] == testJobB["jobgroup"]) and \
               (testJobA["couch_record"] == testJobB["couch_record"]) and \
               (testJobA["location"] == testJobB["location"]), \
            "ERROR: Load from ID didn't load everything correctly"

        assert (testJobA["id"] == testJobC["id"]) and \
               (testJobA["name"] == testJobC["name"]) and \
               (testJobA["jobgroup"] == testJobC["jobgroup"]) and \
               (testJobA["couch_record"] == testJobC["couch_record"]) and \
               (testJobA["location"] == testJobC["location"]), \
            "ERROR: Load from name didn't load everything correctly"

        self.assertEqual(testJobB['outcome'], 'failure')
        self.assertEqual(testJobC['outcome'], 'failure')
        self.assertEqual(testJobB['fwjr'], None)
        self.assertEqual(testJobC['fwjr'], None)

        return

    def testLoadData(self):
        """
        _testLoadData_

        Create a job and save it to the database.  Load it back from the
        database using the name and the id.  Verify that all job information
        is correct including input files and the job mask.
        """
        testJobA = self.createTestJob()

        testJobA["mask"]["FirstEvent"] = 1
        testJobA["mask"]["LastEvent"] = 2
        testJobA["mask"]["FirstLumi"] = 3
        testJobA["mask"]["LastLumi"] = 4
        testJobA["mask"]["FirstRun"] = 5
        testJobA["mask"]["LastRun"] = 6

        testJobA.save()

        testJobB = Job(id=testJobA["id"])
        testJobC = Job(name=testJobA["name"])
        testJobB.loadData()
        testJobC.loadData()

        assert (testJobA["id"] == testJobB["id"]) and \
               (testJobA["name"] == testJobB["name"]) and \
               (testJobA["jobgroup"] == testJobB["jobgroup"]) and \
               (testJobA["couch_record"] == testJobB["couch_record"]) and \
               (testJobA["location"] == testJobB["location"]), \
            "ERROR: Load from ID didn't load everything correctly"

        assert (testJobA["id"] == testJobC["id"]) and \
               (testJobA["name"] == testJobC["name"]) and \
               (testJobA["jobgroup"] == testJobC["jobgroup"]) and \
               (testJobA["couch_record"] == testJobC["couch_record"]) and \
               (testJobA["location"] == testJobC["location"]), \
            "ERROR: Load from name didn't load everything correctly"

        assert testJobA["mask"] == testJobB["mask"], \
            "ERROR: Job mask did not load properly"

        assert testJobA["mask"] == testJobC["mask"], \
            "ERROR: Job mask did not load properly"

        goldenFiles = testJobA.getFiles()
        for testFile in testJobB.getFiles():
            assert testFile in goldenFiles, \
                "ERROR: Job loaded an unknown file"
            goldenFiles.remove(testFile)

        assert not goldenFiles, "ERROR: Job didn't load all files"

        goldenFiles = testJobA.getFiles()
        for testFile in testJobC.getFiles():
            assert testFile in goldenFiles, \
                "ERROR: Job loaded an unknown file"
            goldenFiles.remove(testFile)

        assert not goldenFiles, "ERROR: Job didn't load all files"

        return

    def testGetFiles(self):
        """
        _testGetFiles_

        Test the Job's getFiles() method.  This should load the files from
        the database if they haven't been loaded already.
        """
        testJobA = self.createTestJob()

        testJobB = Job(id=testJobA["id"])
        testJobB.loadData()

        goldenFiles = testJobA.getFiles()
        for testFile in testJobB.getFiles():
            assert testFile in goldenFiles, \
                "ERROR: Job loaded an unknown file: %s" % testFile
            goldenFiles.remove(testFile)

        assert not goldenFiles, "ERROR: Job didn't load all files"

        return

    def testSaveTransaction(self):
        """
        _testSaveTransaction_

        Create a job and a job mask and save them both to the database.  Load
        the job from the database and verify that everything was written
        correctly.  Begin a new transaction and update the job mask again.
        Load the mask and verify that it's correct.  Finally, rollback the
        transaction and reload the mask to verify that it is in the correct
        state.
        """
        testJobA = self.createTestJob()

        testJobA["mask"]["FirstEvent"] = 1
        testJobA["mask"]["LastEvent"] = 2
        testJobA["mask"]["FirstLumi"] = 3
        testJobA["mask"]["LastLumi"] = 4
        testJobA["mask"]["FirstRun"] = 5
        testJobA["mask"]["LastRun"] = 6

        testJobA.save()

        testJobB = Job(id=testJobA["id"])
        testJobB.loadData()

        assert testJobA["mask"] == testJobB["mask"], \
            "ERROR: Job mask did not load properly"

        myThread = threading.currentThread()
        myThread.transaction.begin()

        testJobA["mask"]["FirstEvent"] = 7
        testJobA["mask"]["LastEvent"] = 8
        testJobA["mask"]["FirstLumi"] = 9
        testJobA["mask"]["LastLumi"] = 10
        testJobA["mask"]["FirstRun"] = 11
        testJobA["mask"]["LastRun"] = 12
        testJobA["name"] = "stevesJob"
        testJobA["couch_record"] = "someCouchRecord"
        testJobA["location"] = "test2.site.ch"

        testJobA.save()
        testJobC = Job(id=testJobA["id"])
        testJobC.loadData()

        assert testJobA["mask"] == testJobC["mask"], \
            "ERROR: Job mask did not load properly"

        assert testJobC["name"] == "stevesJob", \
            "ERROR: Job name did not save"

        assert testJobC["couch_record"] == "someCouchRecord", \
            "ERROR: Job couch record did not save"

        assert testJobC["location"] == "test2.site.ch", \
            "ERROR: Job site did not save"

        myThread.transaction.rollback()

        testJobD = Job(id=testJobA["id"])
        testJobD.loadData()

        assert testJobB["mask"] == testJobD["mask"], \
            "ERROR: Job mask did not load properly"

        return

    def testJobState(self):
        """
        _testJobState_

        Unittest to see if we can figure out what the jobState actually is and set it
        """

        testJobA = self.createTestJob()

        value = testJobA.getState()

        self.assertEqual(value, 'new')

        return

    def testJobCacheDir(self):
        """
        _testJobCacheDir_

        Check retrieval of the jobCache directory.
        """
        testJobA = self.createTestJob()
        value = testJobA.getCache()

        self.assertEqual(value, None)

        testJobA.setCache('UnderTheDeepBlueSea')
        value = testJobA.getCache()

        self.assertEqual(value, 'UnderTheDeepBlueSea')

        return

    def testGetOutputParentLFNs(self):
        """
        _testGetOutputParentLFNs_

        Verify that the getOutputDBSParentLFNs() method returns the correct
        parent LFNs.
        """
        testWorkflow = Workflow(spec="spec.xml", owner="Simon",
                                name="wf001", task="Test")
        testWorkflow.create()

        testWMBSFileset = Fileset(name="TestFileset")
        testWMBSFileset.create()

        testSubscription = Subscription(fileset=testWMBSFileset,
                                        workflow=testWorkflow)
        testSubscription.create()

        testJobGroup = JobGroup(subscription=testSubscription)
        testJobGroup.create()

        testFileA = File(lfn="/this/is/a/lfnA", size=1024, events=10,
                         merged=True)
        testFileB = File(lfn="/this/is/a/lfnB", size=1024, events=10,
                         merged=True)
        testFileC = File(lfn="/this/is/a/lfnC", size=1024, events=10,
                         merged=False)
        testFileD = File(lfn="/this/is/a/lfnD", size=1024, events=10,
                         merged=False)
        testFileE = File(lfn="/this/is/a/lfnE", size=1024, events=10,
                         merged=True)
        testFileF = File(lfn="/this/is/a/lfnF", size=1024, events=10,
                         merged=True)
        testFileA.create()
        testFileB.create()
        testFileC.create()
        testFileD.create()
        testFileE.create()
        testFileF.create()

        testFileE.addChild(testFileC["lfn"])
        testFileF.addChild(testFileD["lfn"])

        testJobA = Job(name="TestJob", files=[testFileA, testFileB])
        testJobA["couch_record"] = "somecouchrecord"
        testJobA["location"] = "test.site.ch"
        testJobA.create(group=testJobGroup)
        testJobA.associateFiles()

        testJobB = Job(name="TestJobB", files=[testFileC, testFileD])
        testJobB["couch_record"] = "somecouchrecord"
        testJobB["location"] = "test.site.ch"
        testJobB.create(group=testJobGroup)
        testJobB.associateFiles()

        goldenLFNs = ["/this/is/a/lfnA", "/this/is/a/lfnB"]

        parentLFNs = testJobA.getOutputDBSParentLFNs()
        for parentLFN in parentLFNs:
            assert parentLFN in goldenLFNs, \
                "ERROR: Unknown lfn: %s" % parentLFN
            goldenLFNs.remove(parentLFN)

        assert not goldenLFNs, "ERROR: LFNs are missing: %s" % goldenLFNs

        goldenLFNs = ["/this/is/a/lfnE", "/this/is/a/lfnF"]

        parentLFNs = testJobB.getOutputDBSParentLFNs()
        for parentLFN in parentLFNs:
            assert parentLFN in goldenLFNs, \
                "ERROR: Unknown lfn: %s" % parentLFN
            goldenLFNs.remove(parentLFN)

        assert not goldenLFNs, "ERROR: LFNs are missing..."

        return

    def testJobFWJRPath(self):
        """
        _testJobFWJRPath_

        Verify the correct operation of the Jobs.SetFWJRPath and
        Jobs.GetFWJRByState DAOs.
        """
        testJobA = self.createTestJob()
        testJobA["state"] = "complete"
        testJobB = self.createTestJob()
        testJobB["state"] = "executing"
        testJobC = self.createTestJob()
        testJobC["state"] = "complete"

        myThread = threading.currentThread()
        setFWJRAction = self.daoFactory(classname="Jobs.SetFWJRPath")
        setFWJRAction.execute(jobID=testJobA["id"], fwjrPath="NonsenseA",
                              conn=myThread.transaction.conn,
                              transaction=True)
        setFWJRAction.execute(jobID=testJobB["id"], fwjrPath="NonsenseB",
                              conn=myThread.transaction.conn,
                              transaction=True)
        setFWJRAction.execute(jobID=testJobC["id"], fwjrPath="NonsenseC",
                              conn=myThread.transaction.conn,
                              transaction=True)

        changeStateAction = self.daoFactory(classname="Jobs.ChangeState")
        changeStateAction.execute(jobs=[testJobA, testJobB, testJobC],
                                  conn=myThread.transaction.conn,
                                  transaction=True)

        getJobsAction = self.daoFactory(classname="Jobs.GetFWJRByState")
        jobs = getJobsAction.execute(state="complete",
                                     conn=myThread.transaction.conn,
                                     transaction=True)

        goldenIDs = [testJobA["id"], testJobC["id"]]
        for job in jobs:
            assert job["id"] in goldenIDs, \
                "Error: Unknown job: %s" % job["id"]

            goldenIDs.remove(job["id"])

            if job["id"] == testJobA["id"]:
                assert job["fwjr_path"] == "NonsenseA", \
                    "Error: Wrong fwjr path: %s" % job["fwjr_path"]
            else:
                assert job["fwjr_path"] == "NonsenseC", \
                    "Error: Wrong fwjr path: %s" % job["fwjr_path"]

        assert not goldenIDs, "Error: Jobs missing: %s" % len(goldenIDs)

        return

    def testFailJobInput(self):
        """
        _testFailJobInput_

        Test the Jobs.FailInput DAO and verify that it doesn't affect other
        jobs/subscriptions that run over the same files.
        """
        testWorkflow = Workflow(spec="spec.xml", owner="Steve",
                                name="wf001", task="Test")
        bogusWorkflow = Workflow(spec="spec1.xml", owner="Steve",
                                 name="wf002", task="Test")
        testWorkflow.create()
        bogusWorkflow.create()

        testFileset = Fileset(name="TestFileset")
        bogusFileset = Fileset(name="BogusFileset")
        testFileset.create()
        bogusFileset.create()

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=testWorkflow)
        bogusSubscription = Subscription(fileset=bogusFileset,
                                         workflow=bogusWorkflow)
        testSubscription.create()
        bogusSubscription.create()

        testFileA = File(lfn=makeUUID(), locations="T2_CH_CERN")
        testFileB = File(lfn=makeUUID(), locations="T2_CH_CERN")
        testFileC = File(lfn=makeUUID(), locations="T2_CH_CERN")
        testFileA.create()
        testFileB.create()
        testFileC.create()

        testFileset.addFile([testFileA, testFileB, testFileC])
        bogusFileset.addFile([testFileA, testFileB, testFileC])
        testFileset.commit()
        bogusFileset.commit()

        testSubscription.completeFiles([testFileA, testFileB, testFileC])
        bogusSubscription.acquireFiles([testFileA, testFileB, testFileC])

        testJobGroup = JobGroup(subscription=testSubscription)
        bogusJobGroup = JobGroup(subscription=bogusSubscription)
        testJobGroup.create()
        bogusJobGroup.create()

        testJobA = Job(name="TestJobA", files=[testFileA, testFileB, testFileC])
        testJobB = Job(name="TestJobB", files=[testFileA, testFileB, testFileC])

        bogusJob = Job(name="BogusJob", files=[testFileA, testFileB, testFileC])

        testJobA.create(group=testJobGroup)
        testJobB.create(group=testJobGroup)

        bogusJob.create(group=bogusJobGroup)

        testJobA.failInputFiles()
        testJobB.failInputFiles()

        self.assertEqual(len(testSubscription.filesOfStatus("Available")), 0)
        self.assertEqual(len(testSubscription.filesOfStatus("Acquired")), 0)
        self.assertEqual(len(testSubscription.filesOfStatus("Failed")), 3)
        self.assertEqual(len(testSubscription.filesOfStatus("Completed")), 0)

        changeStateAction = self.daoFactory(classname="Jobs.ChangeState")
        testJobB["state"] = "cleanout"
        changeStateAction.execute([testJobB])

        # Try again

        testJobA.failInputFiles()

        # Should now be failed
        self.assertEqual(len(testSubscription.filesOfStatus("Available")), 0)
        self.assertEqual(len(testSubscription.filesOfStatus("Acquired")), 0)
        self.assertEqual(len(testSubscription.filesOfStatus("Failed")), 3)
        self.assertEqual(len(testSubscription.filesOfStatus("Completed")), 0)

        # bogus should be unchanged
        self.assertEqual(len(bogusSubscription.filesOfStatus("Available")), 0)
        self.assertEqual(len(bogusSubscription.filesOfStatus("Acquired")), 3)
        self.assertEqual(len(bogusSubscription.filesOfStatus("Failed")), 0)
        self.assertEqual(len(bogusSubscription.filesOfStatus("Completed")), 0)

        return

    def testCompleteJobInput(self):
        """
        _testCompleteJobInput_

        Verify the correct output of the CompleteInput DAO.  This should mark
        the input for a job as complete once all the jobs that run over a
        particular file have complete successfully.
        """
        testWorkflow = Workflow(spec="spec.xml", owner="Steve",
                                name="wf001", task="Test")
        bogusWorkflow = Workflow(spec="spec1.xml", owner="Steve",
                                 name="wf002", task="Test")
        testWorkflow.create()
        bogusWorkflow.create()

        testFileset = Fileset(name="TestFileset")
        bogusFileset = Fileset(name="BogusFileset")
        testFileset.create()
        bogusFileset.create()

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=testWorkflow)
        bogusSubscription = Subscription(fileset=bogusFileset,
                                         workflow=bogusWorkflow)
        testSubscription.create()
        bogusSubscription.create()

        testFileA = File(lfn=makeUUID(), locations="T2_CH_CERN")
        testFileB = File(lfn=makeUUID(), locations="T2_CH_CERN")
        testFileC = File(lfn=makeUUID(), locations="T2_CH_CERN")
        testFileA.create()
        testFileB.create()
        testFileC.create()

        testFileset.addFile([testFileA, testFileB, testFileC])
        bogusFileset.addFile([testFileA, testFileB, testFileC])
        testFileset.commit()
        bogusFileset.commit()

        testSubscription.acquireFiles([testFileA, testFileB, testFileC])
        bogusSubscription.acquireFiles([testFileA, testFileB, testFileC])

        testJobGroup = JobGroup(subscription=testSubscription)
        bogusJobGroup = JobGroup(subscription=bogusSubscription)
        testJobGroup.create()
        bogusJobGroup.create()

        testJobA = Job(name="TestJobA", files=[testFileA])
        testJobB = Job(name="TestJobB", files=[testFileA, testFileB])
        testJobC = Job(name="TestJobC", files=[testFileC])
        bogusJob = Job(name="BogusJob", files=[testFileA, testFileB, testFileC])
        testJobA.create(group=testJobGroup)
        testJobB.create(group=testJobGroup)
        testJobC.create(group=testJobGroup)
        bogusJob.create(group=bogusJobGroup)

        testJobA["outcome"] = "success"
        testJobB["outcome"] = "failure"
        testJobC["outcome"] = "success"
        testJobA.save()
        testJobB.save()
        testJobC.save()

        testJobA.completeInputFiles()

        compFiles = len(testSubscription.filesOfStatus("Completed"))
        assert compFiles == 0, \
            "Error: test sub has wrong number of complete files: %s" % compFiles

        testJobB["outcome"] = "success"
        testJobB.save()

        testJobB.completeInputFiles(skipFiles=[testFileB["lfn"]])

        availFiles = len(testSubscription.filesOfStatus("Available"))
        assert availFiles == 0, \
            "Error: test sub has wrong number of available files: %s" % availFiles

        acqFiles = len(testSubscription.filesOfStatus("Acquired"))
        assert acqFiles == 1, \
            "Error: test sub has wrong number of acquired files: %s" % acqFiles

        compFiles = len(testSubscription.filesOfStatus("Completed"))
        assert compFiles == 1, \
            "Error: test sub has wrong number of complete files: %s" % compFiles

        failFiles = len(testSubscription.filesOfStatus("Failed"))
        assert failFiles == 1, \
            "Error: test sub has wrong number of failed files: %s" % failFiles

        availFiles = len(bogusSubscription.filesOfStatus("Available"))
        assert availFiles == 0, \
            "Error: test sub has wrong number of available files: %s" % availFiles

        acqFiles = len(bogusSubscription.filesOfStatus("Acquired"))
        assert acqFiles == 3, \
            "Error: test sub has wrong number of acquired files: %s" % acqFiles

        compFiles = len(bogusSubscription.filesOfStatus("Completed"))
        assert compFiles == 0, \
            "Error: test sub has wrong number of complete files: %s" % compFiles

        failFiles = len(bogusSubscription.filesOfStatus("Failed"))
        assert failFiles == 0, \
            "Error: test sub has wrong number of failed files: %s" % failFiles

        return

    def testJobTypeDAO(self):
        """
        _testJobTypeDAO_

        Verify that the Jobs.GetType DAO returns the correct job type.  The
        job type is retrieved from the subscription type. When only a single
        job is passed.
        """
        testJob = self.createTestJob()

        jobTypeAction = self.daoFactory(classname="Jobs.GetType")
        jobType = jobTypeAction.execute(jobID=testJob["id"])

        assert jobType == "Merge", \
            "Error: GetJobType DAO returned the wrong job type."

        return

    def testJobTypeDAOBulk(self):
        """
        _testJobTypeDAOBulk_

        Verify that the Jobs.GetType DAO returns the correct job type.  The
        job type is retrieved from the subscription type. When a list of jobs
        ids is passed.
        """
        testJobA = self.createTestJob(subscriptionType="Merge")
        testJobB = self.createTestJob(subscriptionType="Processing")
        testJobC = self.createTestJob(subscriptionType="Production")
        testJobD = self.createTestJob(subscriptionType="Merge")
        testJobE = self.createTestJob(subscriptionType="Skim")

        jobIds = []
        jobIds.append(testJobA["id"])
        jobIds.append(testJobB["id"])
        jobIds.append(testJobC["id"])
        jobIds.append(testJobD["id"])
        jobIds.append(testJobE["id"])

        jobTypeAction = self.daoFactory(classname="Jobs.GetType")
        jobTypes = jobTypeAction.execute(jobID=jobIds)

        entryMap = {}
        for entry in jobTypes:
            entryMap[entry["id"]] = entry["type"]

        assert entryMap[testJobA["id"]] == "Merge", \
            "Error: GetJobType DAO returned the wrong job type."
        assert entryMap[testJobB["id"]] == "Processing", \
            "Error: GetJobType DAO returned the wrong job type."
        assert entryMap[testJobC["id"]] == "Production", \
            "Error: GetJobType DAO returned the wrong job type."
        assert entryMap[testJobD["id"]] == "Merge", \
            "Error: GetJobType DAO returned the wrong job type."
        assert entryMap[testJobE["id"]] == "Skim", \
            "Error: GetJobType DAO returned the wrong job type."

        return

    def testGetOutputMapDAO(self):
        """
        _testGetOutputMapDAO_

        Verify the proper behavior of the GetOutputMapDAO for a variety of
        different processing chains.
        """
        recoOutputFileset = Fileset(name="RECO")
        recoOutputFileset.create()
        mergedRecoOutputFileset = Fileset(name="MergedRECO")
        mergedRecoOutputFileset.create()
        alcaOutputFileset = Fileset(name="ALCA")
        alcaOutputFileset.create()
        mergedAlcaOutputFileset = Fileset(name="MergedALCA")
        mergedAlcaOutputFileset.create()
        dqmOutputFileset = Fileset(name="DQM")
        dqmOutputFileset.create()
        mergedDqmOutputFileset = Fileset(name="MergedDQM")
        mergedDqmOutputFileset.create()
        cleanupFileset = Fileset(name="Cleanup")
        cleanupFileset.create()

        testWorkflow = Workflow(spec="wf001.xml", owner="Steve",
                                name="TestWF", task="None")
        testWorkflow.create()
        testWorkflow.addOutput("output", recoOutputFileset,
                               mergedRecoOutputFileset)
        testWorkflow.addOutput("ALCARECOStreamCombined", alcaOutputFileset,
                               mergedAlcaOutputFileset)
        testWorkflow.addOutput("DQM", dqmOutputFileset,
                               mergedDqmOutputFileset)
        testWorkflow.addOutput("output", cleanupFileset)
        testWorkflow.addOutput("ALCARECOStreamCombined", cleanupFileset)
        testWorkflow.addOutput("DQM", cleanupFileset)

        testRecoMergeWorkflow = Workflow(spec="wf002.xml", owner="Steve",
                                         name="TestRecoMergeWF", task="None")
        testRecoMergeWorkflow.create()
        testRecoMergeWorkflow.addOutput("anything", mergedRecoOutputFileset,
                                        mergedRecoOutputFileset)

        testRecoProcWorkflow = Workflow(spec="wf004.xml", owner="Steve",
                                        name="TestRecoProcWF", task="None")
        testRecoProcWorkflow.create()

        testAlcaChildWorkflow = Workflow(spec="wf003.xml", owner="Steve",
                                         name="TestAlcaChildWF", task="None")
        testAlcaChildWorkflow.create()

        inputFile = File(lfn="/path/to/some/lfn", size=600000, events=60000,
                         locations="cmssrm.fnal.gov")
        inputFile.create()

        testFileset = Fileset(name="TestFileset")
        testFileset.create()
        testFileset.addFile(inputFile)
        testFileset.commit()

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=testWorkflow,
                                        split_algo="EventBased",
                                        type="Processing")

        testMergeRecoSubscription = Subscription(fileset=recoOutputFileset,
                                                 workflow=testRecoMergeWorkflow,
                                                 split_algo="WMBSMergeBySize",
                                                 type="Merge")
        testProcRecoSubscription = Subscription(fileset=recoOutputFileset,
                                                workflow=testRecoProcWorkflow,
                                                split_algo="FileBased",
                                                type="Processing")

        testChildAlcaSubscription = Subscription(fileset=alcaOutputFileset,
                                                 workflow=testAlcaChildWorkflow,
                                                 split_algo="FileBased",
                                                 type="Processing")
        testSubscription.create()
        testMergeRecoSubscription.create()
        testProcRecoSubscription.create()
        testChildAlcaSubscription.create()
        testSubscription.acquireFiles()

        testJobGroup = JobGroup(subscription=testSubscription)
        testJobGroup.create()

        testJob = Job(name="SplitJobA", files=[inputFile])
        testJob.create(group=testJobGroup)
        testJob["state"] = "complete"
        testJob.save()

        outputMapAction = self.daoFactory(classname="Jobs.GetOutputMap")
        outputMap = outputMapAction.execute(jobID=testJob["id"])

        assert len(outputMap) == 3, \
            "Error: Wrong number of outputs for primary workflow."

        goldenMap = {"output": (recoOutputFileset.id,
                                mergedRecoOutputFileset.id),
                     "ALCARECOStreamCombined": (alcaOutputFileset.id,
                                                mergedAlcaOutputFileset.id),
                     "DQM": (dqmOutputFileset.id,
                             mergedDqmOutputFileset.id)}

        for outputID in outputMap:
            for outputFilesets in outputMap[outputID]:
                if outputFilesets["merged_output_fileset"] is None:
                    self.assertEqual(outputFilesets["output_fileset"],
                                     cleanupFileset.id,
                                     "Error: Cleanup fileset is wrong.")
                    continue

                self.assertTrue(outputID in goldenMap,
                                "Error: Output identifier is missing.")
                self.assertEqual(outputFilesets["output_fileset"],
                                 goldenMap[outputID][0],
                                 "Error: Output fileset is wrong.")
                self.assertEqual(outputFilesets["merged_output_fileset"],
                                 goldenMap[outputID][1],
                                 "Error: Merged output fileset is wrong.")
                del goldenMap[outputID]

        self.assertEqual(len(goldenMap), 0,
                         "Error: Missing output maps.")

        return

    def testLocations(self):
        """
        _testLocations_

        Test setting and getting locations using DAO objects
        """
        testJob = self.createTestJob()

        jobGetLocation = self.daoFactory(classname="Jobs.GetLocation")
        jobSetLocation = self.daoFactory(classname="Jobs.SetLocation")

        result = jobGetLocation.execute(jobid=testJob['id'])
        self.assertEqual(result, [['test.site.ch']])
        jobSetLocation.execute(jobid=testJob['id'], location="test2.site.ch")
        result = jobGetLocation.execute(jobid=testJob['id'])
        self.assertEqual(result, [['test2.site.ch']])

        testJob2 = self.createTestJob()
        testJob3 = self.createTestJob()

        binds = [{'jobid': testJob['id']}, {'jobid': testJob2['id']}, {'jobid': testJob3['id']}]
        result = jobGetLocation.execute(jobid=binds)
        self.assertEqual(result,
                         [{'site_name': 'test2.site.ch', 'id': 1},
                          {'site_name': 'test.site.ch', 'id': 2},
                          {'site_name': 'test.site.ch', 'id': 3}])

        return

    def testGetDataStructsJob(self):
        """
        _testGetDataStructsJob_

        Test the ability to 'cast' as a DataStructs job type
        """
        testJob = self.createTestJob()
        testJob['test'] = 'ThisIsATest'
        testJob.baggage.section_('TestSection')
        testJob.baggage.TestSection.test = 100
        finalJob = testJob.getDataStructsJob()

        for key in finalJob:
            if key == 'input_files':
                for inputFile in testJob['input_files']:
                    self.assertEqual(inputFile.returnDataStructsFile() in finalJob['input_files'], True)
                continue
            self.assertEqual(testJob[key], finalJob[key])

        self.assertEqual(str(finalJob.__class__), "<class 'WMCore.DataStructs.Job.Job'>")
        self.assertEqual(str(finalJob["mask"].__class__), "<class 'WMCore.DataStructs.Mask.Mask'>")

        for key in testJob["mask"]:
            self.assertEqual(testJob["mask"][key], finalJob["mask"][key],
                             "Error: The masks should be the same")

        self.assertEqual(finalJob.baggage.TestSection.test, 100)
        return

    def testLoadOutputID(self):
        """
        _testLoadOutputID_

        Test whether we can load an output ID for a job
        """

        testWorkflow = Workflow(spec="spec.xml", owner="Steve",
                                name="wf001", task="Test")

        testWorkflow.create()

        testFileset = Fileset(name="TestFileset")
        testFileset.create()

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=testWorkflow)

        testSubscription.create()

        testFileA = File(lfn=makeUUID(), locations="test.site.ch")
        testFileB = File(lfn=makeUUID(), locations="test.site.ch")
        testFileA.create()
        testFileB.create()

        testFileset.addFile([testFileA, testFileB])
        testFileset.commit()

        testSubscription.acquireFiles([testFileA, testFileB])

        testJobGroup = JobGroup(subscription=testSubscription)
        testJobGroup.create()

        testJob = Job()
        testJob.create(group=testJobGroup)

        self.assertEqual(testJob.loadOutputID(), testJobGroup.output.id)

        return

    def testLoadForTaskArchiver(self):
        """
        _testLoadForTaskArchiver_

        Tests the return of the DAO for the TaskArchiver
        """
        # Create 2 jobs
        jobA = self.createTestJob()
        jobB = self.createTestJob()

        # Put a mask in one
        mask = Mask()
        mask.addRunAndLumis(1, [45])
        mask.save(jobA['id'])

        # Execute the DAO
        taskArchiverDAO = self.daoFactory(classname="Jobs.LoadForTaskArchiver")
        jobs = taskArchiverDAO.execute([jobA['id'], jobB['id']])

        # Sort the jobs and check the results, we care about id, input files and mask
        jobs.sort(key=lambda x: x['id'])

        jobAprime = jobs[0]
        lfns = [x['lfn'] for x in jobAprime['input_files']]
        self.assertTrue('/this/is/a/lfnA' in lfns, 'Missing LFN lfnA from the input files')
        self.assertTrue('/this/is/a/lfnB' in lfns, 'Missing LFN lfnB from the input files')

        for inFile in jobAprime['input_files']:
            if inFile['lfn'] == '/this/is/a/lfnA':
                run = inFile['runs'].pop()
                self.assertEqual(run.run, 1, 'The run number is wrong')
                self.assertEqual(run.lumis, [45], 'The lumis are wrong')
            else:
                run = inFile['runs'].pop()
                self.assertEqual(run.run, 1, 'The run number is wrong')
                self.assertEqual(run.lumis, [46], 'The lumis are wrong')

        mask = jobAprime['mask']
        self.assertEqual(mask['runAndLumis'], {1: [[45, 45]]}, "Wrong run and lumis in mask")

        jobBprime = jobs[1]
        for inFile in jobBprime['input_files']:
            if inFile['lfn'] == '/this/is/a/lfnA':
                run = inFile['runs'].pop()
                self.assertEqual(run.run, 1, 'The run number is wrong')
                self.assertEqual(run.lumis, [45], 'The lumis are wrong')
            else:
                run = inFile['runs'].pop()
                self.assertEqual(run.run, 1, 'The run number is wrong')
                self.assertEqual(run.lumis, [46], 'The lumis are wrong')
        runs = []
        for inputFile in jobBprime['input_files']:
            runs.extend(inputFile.getRuns())
        self.assertEqual(jobBprime['mask'].filterRunLumisByMask(runs=runs), runs, "Wrong mask in jobB")

        return

    def testMask(self):
        """
        _testMask_

        Test the new mask setup
        """

        testWorkflow = Workflow(spec="spec.xml", owner="Steve",
                                name="wf001", task="Test")

        testWorkflow.create()

        testFileset = Fileset(name="TestFileset")
        testFileset.create()

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=testWorkflow)

        testSubscription.create()

        testFileA = File(lfn=makeUUID(), locations="test.site.ch")
        testFileB = File(lfn=makeUUID(), locations="test.site.ch")
        testFileA.create()
        testFileB.create()

        testFileset.addFile([testFileA, testFileB])
        testFileset.commit()

        testSubscription.acquireFiles([testFileA, testFileB])

        testJobGroup = JobGroup(subscription=testSubscription)
        testJobGroup.create()

        testJob = Job()
        testJob['mask'].addRunAndLumis(run=100, lumis=[101, 102])
        testJob['mask'].addRunAndLumis(run=200, lumis=[201, 202])
        testJob.create(group=testJobGroup)

        loadJob = Job(id=testJob.exists())
        loadJob.loadData()

        runs = loadJob['mask'].getRunAndLumis()
        self.assertEqual(len(runs), 2)
        self.assertEqual(runs[100], [[101, 102]])
        self.assertEqual(runs[200], [[201, 202]])

        bigRun = Run(100, *[101, 102, 103, 104])
        badRun = Run(300, *[1001, 1002])
        result = loadJob['mask'].filterRunLumisByMask([bigRun, badRun])

        self.assertEqual(len(result), 1)
        alteredRun = result.pop()
        self.assertEqual(alteredRun.run, 100)
        self.assertEqual(alteredRun.lumis, [101, 102])

        run0 = Run(300, *[1001, 1002])
        run1 = Run(300, *[1001, 1002])
        loadJob['mask'].filterRunLumisByMask([run0, run1])

        return

    def testAutoIncrementCheck(self):
        """
        _AutoIncrementCheck_

        Test and see whether we can find and set the auto_increment values
        """
        myThread = threading.currentThread()
        if not myThread.dialect.lower() == 'mysql':
            return

        testWorkflow = Workflow(spec="spec.xml", owner="Steve",
                                name="wf001", task="Test")

        testWorkflow.create()

        testFileset = Fileset(name="TestFileset")
        testFileset.create()

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=testWorkflow)

        testSubscription.create()

        testFileA = File(lfn=makeUUID(), locations="test.site.ch")
        testFileB = File(lfn=makeUUID(), locations="test.site.ch")
        testFileA.create()
        testFileB.create()

        testFileset.addFile([testFileA, testFileB])
        testFileset.commit()

        testSubscription.acquireFiles([testFileA, testFileB])

        testJobGroup = JobGroup(subscription=testSubscription)
        testJobGroup.create()

        incrementDAO = self.daoFactory(classname="Jobs.AutoIncrementCheck")
        incrementDAO.execute()

        testJob = Job()
        testJob.create(group=testJobGroup)
        self.assertEqual(testJob.exists(), 1)

        incrementDAO.execute()

        testJob = Job()
        testJob.create(group=testJobGroup)
        self.assertEqual(testJob.exists(), 2)

        incrementDAO.execute(input=10)

        testJob = Job()
        testJob.create(group=testJobGroup)
        self.assertEqual(testJob.exists(), 11)

        incrementDAO.execute(input=5)

        testJob = Job()
        testJob.create(group=testJobGroup)
        self.assertEqual(testJob.exists(), 12)

        return


if __name__ == "__main__":
    unittest.main()
