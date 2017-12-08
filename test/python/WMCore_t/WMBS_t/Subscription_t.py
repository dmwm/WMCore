#!/usr/bin/env python
"""
_Subscription_t_

Unit tests for the WMBS Subscription class and all it's DAOs.
"""
from __future__ import division, print_function
import threading
import time
import unittest
from functools import reduce

from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.Run import Run
from WMCore.WMBS.CreateWMBSBase import CreateWMBSBase
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMQuality.TestInit import TestInit


class SubscriptionTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        WMBS tables.  Also, create some dummy locations.
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules=["WMCore.WMBS"],
                                useDefault=False)

        myThread = threading.currentThread()
        self.daofactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)

        locationAction = self.daofactory(classname="Locations.New")
        locationAction.execute(siteName="site1", pnn="goodse.cern.ch")
        locationAction.execute(siteName="site2", pnn="testse.cern.ch")
        locationAction.execute(siteName="site3", pnn="badse.cern.ch")

        stateDAO = self.daofactory(classname="Jobs.GetStateID")
        self.stateID = stateDAO.execute('cleanout')

        return

    def tearDown(self):
        """
        _tearDown_

        Drop all the WMBS tables.
        """
        self.testInit.clearDatabase()

    def createSubscriptionWithFileABC(self):
        """
        _createSubscriptionWithFileABC_

        Create a subscription where the input fileset has three files.  Also
        create a second subscription that has acquired two of the files.
        """
        testWorkflow = Workflow(spec="spec.xml", owner="Simon",
                                name="wf001", task="Test")
        testWorkflow.create()
        testWorkflow2 = Workflow(spec="specBOGUS.xml", owner="Simon",
                                 name="wfBOGUS", task="Test")
        testWorkflow2.create()

        testFileA = File(lfn="/this/is/a/lfnA", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileA.addRun(Run(1, *[45]))

        testFileB = File(lfn="/this/is/a/lfnB", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileB.addRun(Run(1, *[45]))

        testFileC = File(lfn="/this/is/a/lfnC", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileC.addRun(Run(2, *[48]))

        testFileA.create()
        testFileB.create()
        testFileC.create()

        testFileset = Fileset(name="TestFileset")
        testFileset.create()

        testFileset.addFile(testFileA)
        testFileset.addFile(testFileB)
        testFileset.addFile(testFileC)
        testFileset.commit()

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=testWorkflow)
        testSubscription2 = Subscription(fileset=testFileset,
                                         workflow=testWorkflow2)
        testSubscription2.create()
        testSubscription2.acquireFiles([testFileA, testFileB])

        return (testSubscription, testFileset, testWorkflow, testFileA,
                testFileB, testFileC)

    def createParentageScenario(self):
        """
        _createParentageScenario_

        Populate the DB with two workflows,
        with different filesets but one file in common.
        For one workflow this file belongs to the input fileset
        but for the other it is the parent of one of the files
        in its input dataset
        """

        # Create several files and set parentage relations
        # The genealogy goes like this
        # fileA and fileB belong to fileset1
        # fileC belongs to fileset2
        # fileB is a parent of fileC
        # fileD is parent of fileA and doesn't belong to any fileset
        testFileA = File(lfn="/this/is/a/lfnA", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileA.addRun(Run(1, *[45]))

        testFileB = File(lfn="/this/is/a/lfnB", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileB.addRun(Run(1, *[45]))

        testFileC = File(lfn="/this/is/a/lfnC", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileC.addRun(Run(2, *[48]))

        testFileD = File(lfn="/this/is/a/lfnD", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileD.addRun(Run(2, *[48]))

        testFileA.create()
        testFileB.create()
        testFileC.create()
        testFileD.create()

        testFileA.addParent(testFileD['lfn'])
        testFileC.addParent(testFileB['lfn'])

        testFileset1 = Fileset(name="TestFileset1")
        testFileset1.create()

        testFileset1.addFile(testFileA)
        testFileset1.addFile(testFileB)
        testFileset1.commit()

        testFileset2 = Fileset(name="TestFileset2")
        testFileset2.create()

        testFileset2.addFile(testFileC)
        testFileset2.commit()

        # Now to the workflows and subscriptions

        testWorkflow1 = Workflow(spec="specA.xml", owner="Simon",
                                 name="wf001", task="Test")
        testWorkflow1.create()
        testWorkflow2 = Workflow(spec="specB.xml", owner="Simon",
                                 name="wf002", task="Test")
        testWorkflow2.create()

        testSubscription1 = Subscription(fileset=testFileset1,
                                         workflow=testWorkflow1)
        testSubscription1.create()

        testSubscription2 = Subscription(fileset=testFileset2,
                                         workflow=testWorkflow2)
        testSubscription2.create()

        return {'Subscriptions': [testSubscription1, testSubscription2],
                'Workflows': [testWorkflow1, testWorkflow2],
                'Filesets': [testFileset1, testFileset2],
                'Files': [testFileA, testFileB, testFileC, testFileD]}

    def testCreateDeleteExists(self):
        """
        _testCreateDeleteExists_

        Create and delete a subscription and use the exists() method to
        determine if the create()/delete() methods were successful.
        """
        (testSubscription, testFileset, testWorkflow,
         testFileA, testFileB, testFileC) = self.createSubscriptionWithFileABC()

        self.assertFalse(testSubscription.exists(), "Subscription exists before it was created")

        testSubscription.create()

        self.assertTrue(testSubscription.exists() >= 0, "Subscription does not exist after it was created")

        testSubscription.delete()

        self.assertFalse(testSubscription.exists(), "Subscription exists after it was deleted")

        testFileset.delete()
        testFileA.delete()
        testFileB.delete()
        testFileC.delete()
        testWorkflow.delete()
        return

    def testCreateTransaction(self):
        """
        _testCreateTransaction_

        Create a subscription and commit it to the database.  Rollback the
        database connection and verify that the subscription is no longer
        there.
        """
        (testSubscription, testFileset, testWorkflow,
         testFileA, testFileB, testFileC) = self.createSubscriptionWithFileABC()

        self.assertFalse(testSubscription.exists(), "Subscription exists before it was created")

        myThread = threading.currentThread()
        myThread.transaction.begin()

        testSubscription.create()

        self.assertTrue(testSubscription.exists() >= 0, "Subscription does not exist after it was created")

        myThread.transaction.rollback()

        self.assertFalse(testSubscription.exists(), "Subscription exists after transaction was rolled back.")

        testFileset.delete()
        testFileA.delete()
        testFileB.delete()
        testFileC.delete()
        testWorkflow.delete()
        return

    def testDeleteTransaction(self):
        """
        _testDeleteTransaction_

        Create a subscription and commit it to the database.  Begin a new
        transactions and delete the subscription.  Verify that the subscription
        is no longer in the database and then roll back the transaction.  Verify
        that the subscription is once again in the database.
        """
        (testSubscription, testFileset, testWorkflow,
         testFileA, testFileB, testFileC) = self.createSubscriptionWithFileABC()

        self.assertFalse(testSubscription.exists(), "Subscription exists before it was created")

        testSubscription.create()

        self.assertTrue(testSubscription.exists() >= 0, "Subscription does not exist after it was created")

        myThread = threading.currentThread()
        myThread.transaction.begin()

        testSubscription.delete()

        self.assertFalse(testSubscription.exists(), "Subscription exists after it was deleted")

        myThread.transaction.rollback()

        self.assertTrue(testSubscription.exists() >= 0, "Subscription does not exist after roll back.")

        testFileset.delete()
        testFileA.delete()
        testFileB.delete()
        testFileC.delete()
        testWorkflow.delete()
        return

    def testFailFiles(self):
        """
        _testFailFiles_

        Create a subscription and fail a couple of files in it's fileset.  Test
        to make sure that only the failed files are marked as failed.
        """
        (testSubscription, testFileset, testWorkflow,
         testFileA, testFileB, testFileC) = self.createSubscriptionWithFileABC()

        testSubscription.create()

        dummyWorkflow = Workflow(spec="spec1.xml", owner="Simon",
                                 name="wf002", task='Test')
        dummyWorkflow.create()

        failSubscription = Subscription(fileset=testFileset,
                                        workflow=dummyWorkflow)
        failSubscription.create()
        failSubscription.failFiles(failSubscription.filesOfStatus("Available"))

        testSubscription.failFiles([testFileA, testFileC])
        failedFiles = testSubscription.filesOfStatus(status="Failed")

        goldenFiles = [testFileA, testFileC]
        for failedFile in failedFiles:
            assert failedFile in goldenFiles, \
                "Unknown failed files"
            goldenFiles.remove(failedFile)

        assert len(goldenFiles) == 0, \
            "Missing failed files"

        testSubscription.delete()
        testWorkflow.delete()
        testFileset.delete()
        testFileA.delete()
        testFileB.delete()
        testFileC.delete()
        return

    def testFailFilesTransaction(self):
        """
        _testFailFilesTransaction_

        Create a subscription and fail some files that are in it's input
        fileset.  Rollback the subscription and verify that the files are
        no longer failed.
        """
        (testSubscription, testFileset, testWorkflow,
         testFileA, testFileB, testFileC) = self.createSubscriptionWithFileABC()

        testSubscription.create()

        myThread = threading.currentThread()
        myThread.transaction.begin()

        testSubscription.failFiles([testFileA, testFileC])
        failedFiles = testSubscription.filesOfStatus(status="Failed")

        goldenFiles = [testFileA, testFileC]
        for failedFile in failedFiles:
            assert failedFile in goldenFiles, \
                "Unknown failed files"
            goldenFiles.remove(failedFile)

        assert len(goldenFiles) == 0, \
            "Missing failed files"

        myThread.transaction.rollback()

        failedFiles = testSubscription.filesOfStatus(status="Failed")

        assert len(failedFiles) == 0, \
            "Transaction did not roll back failed files"

        testSubscription.delete()
        testWorkflow.delete()
        testFileset.delete()
        testFileA.delete()
        testFileB.delete()
        testFileC.delete()
        return

    def testCompleteFiles(self):
        """
        _testCompleteFiles_

        Create a subscription and complete a couple of files in it's fileset.  Test
        to make sure that only the completed files are marked as complete.
        """
        (testSubscription, testFileset, testWorkflow,
         testFileA, testFileB, testFileC) = self.createSubscriptionWithFileABC()

        testSubscription.create()

        dummyWorkflow = Workflow(spec="spec2.xml", owner="Simon",
                                 name="wf003", task='Test')
        dummyWorkflow.create()

        completeSubscription = Subscription(fileset=testFileset,
                                            workflow=dummyWorkflow)
        completeSubscription.create()
        completeSubscription.completeFiles(completeSubscription.filesOfStatus("Available"))

        testSubscription.completeFiles([testFileA, testFileC])
        completedFiles = testSubscription.filesOfStatus(status="Completed")

        goldenFiles = [testFileA, testFileC]
        for completedFile in completedFiles:
            assert completedFile in goldenFiles, \
                "Unknown completed file"
            goldenFiles.remove(completedFile)

        assert len(goldenFiles) == 0, \
            "Missing completed files"

        testSubscription.delete()
        testWorkflow.delete()
        testFileset.delete()
        testFileA.delete()
        testFileB.delete()
        testFileC.delete()
        return

    def testCompleteFilesTransaction(self):
        """
        _testCompleteFilesTransaction_

        Create a subscription and complete some files that are in it's input
        fileset.  Rollback the transaction and verify that the files are no
        longer marked as complete.
        """
        (testSubscription, testFileset, testWorkflow,
         testFileA, testFileB, testFileC) = self.createSubscriptionWithFileABC()

        testSubscription.create()

        myThread = threading.currentThread()
        myThread.transaction.begin()

        testSubscription.completeFiles([testFileA, testFileC])
        completedFiles = testSubscription.filesOfStatus(status="Completed")

        goldenFiles = [testFileA, testFileC]
        for completedFile in completedFiles:
            assert completedFile in goldenFiles, \
                "Unknown completed file"
            goldenFiles.remove(completedFile)

        assert len(goldenFiles) == 0, \
            "Missing completed files"

        myThread.transaction.rollback()

        completedFiles = testSubscription.filesOfStatus(status="Completed")

        assert len(completedFiles) == 0, \
            "Transaction didn't roll back completed files."

        testSubscription.delete()
        testWorkflow.delete()
        testFileset.delete()
        testFileA.delete()
        testFileB.delete()
        testFileC.delete()
        return

    def testAcquireFiles(self):
        """
        _testAcquireFiles_

        Create a subscription and acquire a couple of files in it's fileset.  Test
        to make sure that only the acquired files are marked as acquired.
        """
        (testSubscription, testFileset, testWorkflow,
         testFileA, testFileB, testFileC) = self.createSubscriptionWithFileABC()

        testSubscription.create()

        dummyWorkflow = Workflow(spec="spec2.xml", owner="Simon",
                                 name="wf003", task='Test')
        dummyWorkflow.create()

        acquireSubscription = Subscription(fileset=testFileset,
                                           workflow=dummyWorkflow)
        acquireSubscription.create()
        acquireSubscription.acquireFiles()

        testSubscription.acquireFiles([testFileA, testFileC])
        acquiredFiles = testSubscription.filesOfStatus(status="Acquired")

        goldenFiles = [testFileA, testFileC]
        for acquiredFile in acquiredFiles:
            assert acquiredFile in goldenFiles, \
                "Unknown acquired file"
            goldenFiles.remove(acquiredFile)

        assert len(goldenFiles) == 0, \
            "Missing acquired files"

        testSubscription.delete()
        testWorkflow.delete()
        testFileset.delete()
        testFileA.delete()
        testFileB.delete()
        testFileC.delete()
        return

    def testAcquireFilesTransaction(self):
        """
        _testAcquireFilesTransaction_

        Create a subscription and acquire some files from it's input fileset.
        Rollback the transaction and verify that the files are no longer marked
        as acquired.
        """
        (testSubscription, testFileset, testWorkflow,
         testFileA, testFileB, testFileC) = self.createSubscriptionWithFileABC()

        testSubscription.create()

        myThread = threading.currentThread()
        myThread.transaction.begin()

        testSubscription.acquireFiles([testFileA, testFileC])
        acquiredFiles = testSubscription.filesOfStatus(status="Acquired")

        goldenFiles = [testFileA, testFileC]
        for acquiredFile in acquiredFiles:
            assert acquiredFile in goldenFiles, \
                "Unknown acquired file"
            goldenFiles.remove(acquiredFile)

        assert len(goldenFiles) == 0, \
            "Missing acquired files"

        myThread.transaction.rollback()
        acquiredFiles = testSubscription.filesOfStatus(status="Acquired")

        assert len(acquiredFiles) == 0, \
            "Transaction didn't roll back acquired files."

        testSubscription.delete()
        testWorkflow.delete()
        testFileset.delete()
        testFileA.delete()
        testFileB.delete()
        testFileC.delete()
        return

    def testAvailableFiles(self):
        """
        _testAvailableFiles_

        Create a subscription and mark a couple files as failed, complete and
        acquired.  Test to make sure that the remainder of the files show up
        as available.
        """
        testWorkflow = Workflow(spec="spec.xml", owner="Simon",
                                name="wf001", task='Test')
        testWorkflow.create()

        testFileA = File(lfn="/this/is/a/lfnA", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileB = File(lfn="/this/is/a/lfnB", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileC = File(lfn="/this/is/a/lfnC", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileD = File(lfn="/this/is/a/lfnD", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileE = File(lfn="/this/is/a/lfnE", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileF = File(lfn="/this/is/a/lfnF", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileA.create()
        testFileB.create()
        testFileC.create()
        testFileD.create()
        testFileE.create()
        testFileF.create()

        testFileset = Fileset(name="TestFileset")
        testFileset.create()

        testFileset.addFile(testFileA)
        testFileset.addFile(testFileB)
        testFileset.addFile(testFileC)
        testFileset.addFile(testFileD)
        testFileset.addFile(testFileE)
        testFileset.addFile(testFileF)
        testFileset.commit()

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=testWorkflow)
        testSubscription.create()

        testSubscription.acquireFiles([testFileA])
        testSubscription.completeFiles([testFileB])
        testSubscription.failFiles([testFileC])
        availableFiles = testSubscription.availableFiles()

        goldenFiles = [testFileD, testFileE, testFileF]
        for availableFile in availableFiles:
            assert availableFile in goldenFiles, \
                "Unknown available file"
            assert len(availableFile["locations"]) == 1, \
                "Wrong number of available files."
            assert "goodse.cern.ch" in availableFile["locations"], \
                "Wrong SE name in file location."
            goldenFiles.remove(availableFile)

        assert len(goldenFiles) == 0, \
            "Missing available files"

        testSubscription.delete()
        testWorkflow.delete()
        testFileset.delete()
        testFileA.delete()
        testFileB.delete()
        testFileC.delete()
        testFileD.delete()
        testFileE.delete()
        testFileF.delete()
        return

    def testAvailableFilesMeta(self):
        """
        _testAvailableFilesMeta_

        Create a subscription and mark a couple files as failed, complete and
        acquired.  Test to make sure that the remainder of the files show up
        as available using the GetAvailableFilesMeta DAO object.
        """
        testWorkflow = Workflow(spec="spec.xml", owner="Simon",
                                name="wf001", task='Test')
        testWorkflow.create()

        testFileA = File(lfn="/this/is/a/lfnA", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileB = File(lfn="/this/is/a/lfnB", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileC = File(lfn="/this/is/a/lfnC", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileD = File(lfn="/this/is/a/lfnD", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileD.addRun(Run(1, *[45]))
        testFileD.addRun(Run(2, *[45]))
        testFileE = File(lfn="/this/is/a/lfnE", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileE.addRun(Run(1, *[45]))
        testFileE.addRun(Run(2, *[45]))
        testFileF = File(lfn="/this/is/a/lfnF", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileF.addRun(Run(1, *[45]))
        testFileF.addRun(Run(2, *[45]))
        testFileA.create()
        testFileB.create()
        testFileC.create()
        testFileD.create()
        testFileE.create()
        testFileF.create()

        testFileset = Fileset(name="TestFileset")
        testFileset.create()

        testFileset.addFile(testFileA)
        testFileset.addFile(testFileB)
        testFileset.addFile(testFileC)
        testFileset.addFile(testFileD)
        testFileset.addFile(testFileE)
        testFileset.addFile(testFileF)
        testFileset.commit()

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=testWorkflow)
        testSubscription.create()

        testSubscription.acquireFiles([testFileA])
        testSubscription.completeFiles([testFileB])
        testSubscription.failFiles([testFileC])

        availAction = self.daofactory(classname="Subscriptions.GetAvailableFilesMeta")
        availableFiles = availAction.execute(subscription=testSubscription["id"])

        testFileDDict = {"id": testFileD["id"], "lfn": testFileD["lfn"],
                         "size": testFileD["size"], "events": testFileD["events"],
                         "run": 1}
        testFileEDict = {"id": testFileE["id"], "lfn": testFileE["lfn"],
                         "size": testFileE["size"], "events": testFileE["events"],
                         "run": 1}
        testFileFDict = {"id": testFileF["id"], "lfn": testFileF["lfn"],
                         "size": testFileF["size"], "events": testFileF["events"],
                         "run": 1}

        goldenFiles = [testFileDDict, testFileEDict, testFileFDict]
        for availableFile in availableFiles:
            assert availableFile in goldenFiles, \
                "Unknown available file: %s" % availableFile
            goldenFiles.remove(availableFile)

        assert len(goldenFiles) == 0, \
            "Missing available files"

        testSubscription.delete()
        testWorkflow.delete()
        testFileset.delete()
        testFileA.delete()
        testFileB.delete()
        testFileC.delete()
        testFileD.delete()
        testFileE.delete()
        testFileF.delete()
        return

    def testAvailableFilesTransaction(self):
        """
        _testAvailableFilesTransaction_

        Create a subscription and mark a couple of it's input files as
        complete, failed and acquired.  Rollback the transactions and verify
        that all the files are listed as available.
        """
        testWorkflow = Workflow(spec="spec.xml", owner="Simon",
                                name="wf001", task='Test')
        testWorkflow.create()

        testFileA = File(lfn="/this/is/a/lfnA", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileB = File(lfn="/this/is/a/lfnB", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileC = File(lfn="/this/is/a/lfnC", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileD = File(lfn="/this/is/a/lfnD", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileE = File(lfn="/this/is/a/lfnE", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileF = File(lfn="/this/is/a/lfnF", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileA.create()
        testFileB.create()
        testFileC.create()
        testFileD.create()
        testFileE.create()
        testFileF.create()

        testFileset = Fileset(name="TestFileset")
        testFileset.create()

        testFileset.addFile(testFileA)
        testFileset.addFile(testFileB)
        testFileset.addFile(testFileC)
        testFileset.addFile(testFileD)
        testFileset.addFile(testFileE)
        testFileset.addFile(testFileF)
        testFileset.commit()

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=testWorkflow)
        testSubscription.create()

        myThread = threading.currentThread()
        myThread.transaction.begin()

        testSubscription.acquireFiles([testFileA])
        testSubscription.completeFiles([testFileB])
        testSubscription.failFiles([testFileC])
        availableFiles = testSubscription.availableFiles()

        goldenFiles = [testFileD, testFileE, testFileF]
        for availableFile in availableFiles:
            assert availableFile in goldenFiles, \
                "Unknown available file"
            goldenFiles.remove(availableFile)

        assert len(goldenFiles) == 0, \
            "Missing available files"

        myThread.transaction.rollback()
        availableFiles = testSubscription.availableFiles()

        goldenFiles = [testFileA, testFileB, testFileC, testFileD, testFileE,
                       testFileF]

        for availableFile in availableFiles:
            assert availableFile in goldenFiles, \
                "Unknown available file"
            goldenFiles.remove(availableFile)

        assert len(goldenFiles) == 0, \
            "Missing available files after rollback."

        testSubscription.delete()
        testWorkflow.delete()
        testFileset.delete()
        testFileA.delete()
        testFileB.delete()
        testFileC.delete()
        testFileD.delete()
        testFileE.delete()
        testFileF.delete()
        return

    def testLoad(self):
        """
        _testLoad_

        Create a subscription and save it to the database.  Test the various
        load methods to make sure that everything saves/loads.
        """
        (testSubscriptionA, testFileset, testWorkflow,
         dummyTestFileA, dummyTestFileB, dummyTestFileC) = self.createSubscriptionWithFileABC()

        testSubscriptionA.create()

        testSubscriptionB = Subscription(id=testSubscriptionA["id"])
        testSubscriptionC = Subscription(workflow=testSubscriptionA["workflow"],
                                         fileset=testSubscriptionA["fileset"],
                                         type=testSubscriptionA["type"])
        testSubscriptionB.load()
        testSubscriptionC.load()

        self.assertTrue(isinstance(testSubscriptionB["id"], int),
                        "Subscription id is not an int.")
        self.assertTrue(isinstance(testSubscriptionB["workflow"].id, int),
                        "Subscription workflow id is not an int.")
        self.assertTrue(isinstance(testSubscriptionC["workflow"].id, int),
                        "Subscription workflow id is not an int.")
        self.assertTrue(isinstance(testSubscriptionB["fileset"].id, int),
                        "Subscription fileset id is not an int.")
        self.assertTrue(isinstance(testSubscriptionC["fileset"].id, int),
                        "Subscription fileset id is not an int.")

        self.assertEqual(testWorkflow.id, testSubscriptionB["workflow"].id,
                         "Subscription load by ID didn't load workflow correctly")
        self.assertEqual(testFileset.id, testSubscriptionB["fileset"].id,
                         "Subscription load by ID didn't load fileset correctly")
        self.assertEqual(testSubscriptionA["id"], testSubscriptionC["id"],
                         "Subscription didn't load ID correctly.")

        return

    def testLoadData(self):
        """
        _testLoadData_

        Test the Subscription's loadData() method to make sure that everything
        that should be loaded is loaded correctly.
        """
        (testSubscriptionA, testFileset, testWorkflow,
         testFileA, testFileB, testFileC) = self.createSubscriptionWithFileABC()

        testSubscriptionA.create()

        testSubscriptionB = Subscription(id=testSubscriptionA["id"])
        testSubscriptionC = Subscription(workflow=testSubscriptionA["workflow"],
                                         fileset=testSubscriptionA["fileset"])
        testSubscriptionB.loadData()
        testSubscriptionC.loadData()

        assert (testWorkflow.id == testSubscriptionB["workflow"].id) and \
               (testWorkflow.name == testSubscriptionB["workflow"].name) and \
               (testWorkflow.spec == testSubscriptionB["workflow"].spec) and \
               (testWorkflow.owner == testSubscriptionB["workflow"].owner), \
            "Workflow.LoadFromID Failed"

        assert testFileset.id == testSubscriptionB["fileset"].id, \
            "Load didn't load fileset id"

        assert testFileset.name == testSubscriptionB["fileset"].name, \
            "Load didn't load fileset name"

        goldenFiles = [testFileA, testFileB, testFileC]
        for filesetFile in testSubscriptionB["fileset"].files:
            assert filesetFile in goldenFiles, \
                "Unknown file in fileset"
            goldenFiles.remove(filesetFile)

        assert len(goldenFiles) == 0, \
            "Fileset is missing files"

        assert testSubscriptionA["id"] == testSubscriptionC["id"], \
            "Subscription didn't load ID correctly."

        return

    def testSubscriptionList(self):
        """
        _testSubscriptionList_

        Create two subscriptions and verify that the Subscriptions.List DAO
        object returns their IDs.
        """
        testWorkflowA = Workflow(spec="spec.xml", owner="Simon",
                                 name="wf001", task='Test')
        testWorkflowB = Workflow(spec="spec2.xml", owner="Simon",
                                 name="wf002", task='Test')
        testWorkflowA.create()
        testWorkflowB.create()

        testFileset = Fileset(name="TestFileset")
        testFileset.create()

        testSubscriptionA = Subscription(fileset=testFileset,
                                         workflow=testWorkflowA)
        testSubscriptionB = Subscription(fileset=testFileset,
                                         workflow=testWorkflowB)
        testSubscriptionA.create()
        testSubscriptionB.create()

        myThread = threading.currentThread()
        daofactory = DAOFactory(package="WMCore.WMBS",
                                logger=myThread.logger,
                                dbinterface=myThread.dbi)

        subListAction = daofactory(classname="Subscriptions.List")
        subIDs = subListAction.execute()

        assert len(subIDs) == 2, \
            "Too many subscriptions returned."

        assert testSubscriptionA["id"] in subIDs, \
            "Subscription A is missing."

        assert testSubscriptionB["id"] in subIDs, \
            "Subscription B is missing."

        return

    def testGetNumberOfJobsPerSite(self):
        """
        Test for a JobCreator specific function

        """

        (testSubscription, dummyTestFileset, dummyTestWorkflow,
         testFileA, testFileB, testFileC) = self.createSubscriptionWithFileABC()

        testSubscription.create()

        testJobGroup = JobGroup(subscription=testSubscription)
        testJobGroup.create()

        jobA = Job(name='testA')
        jobA.addFile(testFileA)
        jobA["location"] = "site1"
        jobA.create(testJobGroup)

        jobB = Job(name='testB')
        jobB.addFile(testFileB)
        jobB["location"] = "site1"
        jobB.create(testJobGroup)

        jobC = Job(name='testC')
        jobC.addFile(testFileC)
        jobC["location"] = "site1"
        jobC.create(testJobGroup)

        testJobGroup.add(jobA)
        testJobGroup.add(jobB)
        testJobGroup.add(jobC)

        testJobGroup.commit()

        nJobs = testSubscription.getNumberOfJobsPerSite('site1', 'new')

        self.assertEqual(nJobs, 3)

        nZero = testSubscription.getNumberOfJobsPerSite('site3', 'new')

        self.assertEqual(nZero, 0)

        return

    def testListIncompleteDAO(self):
        """
        _testListIncompeteDAO_

        Test the Subscription.ListIncomplete DAO object that returns a list of
        the subscriptions that have not completed processing all files.
        """
        (testSubscription, dummyTestFileset, dummyTestWorkflow,
         testFileA, testFileB, testFileC) = self.createSubscriptionWithFileABC()
        testSubscription.create()

        subIncomplete = self.daofactory(classname="Subscriptions.ListIncomplete")

        incompleteSubs = subIncomplete.execute()

        self.assertEqual(len(incompleteSubs), 2,
                         "Wrong number of incomplete subscriptions returned: %s" % len(incompleteSubs))
        self.assertTrue(testSubscription["id"] in incompleteSubs, "Original subscription is missing.")

        otherSub = None
        for subId in incompleteSubs:
            if subId != testSubscription["id"]:
                otherSub = subId

        testSubscription.completeFiles([testFileA, testFileB, testFileC])

        incompleteSubs = subIncomplete.execute()

        self.assertEqual(len(incompleteSubs), 1, "Wrong number of incomplete subscriptions returned.")
        self.assertTrue(otherSub in incompleteSubs, "Wrong subscription ID returned.")

        return

    def testGetJobGroups(self):
        """
        _testGetJobGroups_

        Verify that the getJobGroups() method will return a list of JobGroups
        that contain jobs that have not been acquired/completed/failed.
        """
        (testSubscription, dummyTestFileset, dummyTestWorkflow,
         testFileA, testFileB, testFileC) = self.createSubscriptionWithFileABC()
        testSubscription.create()

        changeJobState = self.daofactory(classname="Jobs.ChangeState")

        testJobGroupA = JobGroup(subscription=testSubscription)
        testJobGroupA.create()

        testJobA = Job(name="TestJobA")
        testJobA.addFile(testFileA)

        testJobB = Job(name="TestJobB")
        testJobB.addFile(testFileB)

        testJobGroupA.add(testJobA)
        testJobGroupA.add(testJobB)
        testJobGroupA.commit()

        testJobGroupB = JobGroup(subscription=testSubscription)
        testJobGroupB.create()

        testFileD = File(lfn="/this/is/a/lfnD", size=1024, events=10)
        testFileD.addRun(Run(10, *[12312]))
        testFileD.create()

        testJobC = Job(name="TestJobC")
        testJobC.addFile(testFileC)

        testJobD = Job(name="TestJobD")
        testJobD.addFile(testFileD)

        testJobGroupB.add(testJobC)
        testJobGroupB.add(testJobD)
        testJobGroupB.commit()

        firstResult = testSubscription.getJobGroups()
        for jobGroup in [testJobGroupA.id, testJobGroupB.id]:
            assert jobGroup in firstResult, \
                "jobgroup %s is missing. " % jobGroup
            firstResult.remove(jobGroup)

        assert len(firstResult) == 0, \
            "Too monay job groups in result."

        testJobA["state"] = "created"
        changeJobState.execute([testJobA])

        secondResult = testSubscription.getJobGroups()
        for jobGroup in [testJobGroupA.id, testJobGroupB.id]:
            assert jobGroup in secondResult, \
                "jobgroup %s is missing. " % jobGroup
            secondResult.remove(jobGroup)

        assert len(secondResult) == 0, \
            "Too monay job groups in result."

        testJobB["state"] = "created"
        changeJobState.execute([testJobB])

        thirdResult = testSubscription.getJobGroups()
        for jobGroup in [testJobGroupB.id]:
            assert jobGroup in thirdResult, \
                "jobgroup %s is missing. " % jobGroup
            thirdResult.remove(jobGroup)

        assert len(thirdResult) == 0, \
            "Too monay job groups in result."

        return

    def testDeleteEverything(self):
        """
        _testDeleteEverything_

        Tests the delete function that should delete all component of a subscription
        """
        myThread = threading.currentThread()

        testWorkflow = Workflow(spec="spec.xml", owner="Simon",
                                name="wf001", task="Test")
        testWorkflow.create()

        testFileA = File(lfn="/this/is/a/lfnA", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileA.addRun(Run(1, *[45]))

        testFileB = File(lfn="/this/is/a/lfnB", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileB.addRun(Run(1, *[45]))

        testFileC = File(lfn="/this/is/a/lfnC", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileC.addRun(Run(2, *[48]))

        testFileD = File(lfn="/this/is/a/lfnD", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileD.addRun(Run(2, *[48]))

        testFile1 = File(lfn="/this/is/a/lfn1", size=1024, events=20,
                         locations={"goodse.cern.ch"}, merged=False)
        testFile1.addRun(Run(2, *[48]))

        testFile2 = File(lfn="/this/is/a/lfn2", size=1024, events=20,
                         locations={"goodse.cern.ch"}, merged=False)
        testFile2.addRun(Run(2, *[48]))

        testFileA.create()
        testFileB.create()
        testFileC.create()
        testFileD.create()
        testFile1.create()
        testFileA.addChild(testFile1['lfn'])

        testFileset = Fileset(name="TestFileset")
        testFileset.create()

        testFileset2 = Fileset(name="TestFileset2")
        testFileset2.create()

        testFileset.addFile(testFileA)
        testFileset.addFile(testFileB)
        testFileset.addFile(testFileC)
        testFileset.addFile(testFileD)
        testFileset.commit()

        testFilesetQ = Fileset(name="TestFilesetQ")
        testFilesetQ.create()
        testMergedFilesetQ = Fileset(name="TestMergedFilesetQ")
        testMergedFilesetQ.create()

        testWorkflow.addOutput(outputIdentifier='a',
                               outputFileset=testFilesetQ,
                               mergedOutputFileset=testMergedFilesetQ)

        testFileset2.addFile(testFileD)
        testFileset2.commit()

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=testWorkflow)
        testSubscription.create()
        testSubscription2 = Subscription(fileset=testFileset2,
                                         workflow=testWorkflow)
        testSubscription2.create()

        testJobGroupA = JobGroup(subscription=testSubscription)
        testJobGroupA.create()

        testJobA = Job(name="TestJobA")
        testJobA.addFile(testFileA)

        testJobB = Job(name="TestJobB")
        testJobB.addFile(testFileB)

        testJobGroupA.add(testJobA)
        testJobGroupA.add(testJobB)
        testJobGroupA.output.addFile(testFile1)
        testJobGroupA.output.addFile(testFile2)
        testJobGroupA.output.commit()

        testJobGroupA.commit()
        testSubscription.save()

        testSubscription.deleteEverything()

        self.assertEqual(testSubscription.exists(), False)
        self.assertEqual(testWorkflow.exists(), 1)
        self.assertEqual(testFileset.exists(), False)
        self.assertEqual(testFileset2.exists(), 2)

        result = myThread.dbi.processData("SELECT * FROM wmbs_job")[0].fetchall()
        self.assertEqual(len(result), 0)
        result = myThread.dbi.processData("SELECT * FROM wmbs_jobgroup")[0].fetchall()
        self.assertEqual(len(result), 0)
        self.assertFalse(testJobGroupA.output.exists())
        self.assertEqual(testFile1.exists(), False)
        self.assertEqual(testFile2.exists(), False)
        self.assertFalse(testFilesetQ.exists())
        self.assertEqual(testFileA.exists(), False)
        self.assertEqual(testFileB.exists(), False)
        self.assertEqual(testFileC.exists(), False)
        self.assertEqual(testFileD.exists(), 4)

    def testIsFileCompleted(self):
        """
        _testIsFileCompleted_

        Test file completion markings
        """
        (testSubscription, testFileset, testWorkflow,
         testFileA, testFileB, testFileC) = self.createSubscriptionWithFileABC()

        testSubscription.create()

        myThread = threading.currentThread()
        myThread.transaction.begin()

        testSubscription.completeFiles([testFileA, testFileC])

        self.assertTrue(testSubscription.isFileCompleted(testFileA), "file A should be completed")
        self.assertTrue(testSubscription.isFileCompleted([testFileA, testFileC]), "file A, C should be completed")
        self.assertFalse(testSubscription.isFileCompleted([testFileA, testFileB, testFileC]),
                         "file A, B, C shouldn't be completed")

        self.assertFalse(testSubscription.isFileCompleted(testFileB), "file B shouldn't be completed")

        testSubscription.delete()
        testWorkflow.delete()
        testFileset.delete()
        testFileA.delete()
        testFileB.delete()
        testFileC.delete()
        return

    def testGetSubTypes(self):
        """
        _testGetSubTypes_

        Test the getSubTypes function
        """
        createBase = CreateWMBSBase()
        subTypes = createBase.subTypes

        getSubTypes = self.daofactory(classname="Subscriptions.GetSubTypes")
        result = getSubTypes.execute()

        self.assertEqual(result.sort(), subTypes.sort())

        return

    def testBulkCommit(self):
        """
        _testBulkCommit_

        Test committing everything in bulk
        """
        testWorkflow = Workflow(spec="spec.xml", owner="Simon",
                                name="wf001", task="Test")
        testWorkflow.create()

        testFileA = File(lfn="/this/is/a/lfnA", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileA.addRun(Run(1, *[45]))

        testFileB = File(lfn="/this/is/a/lfnB", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileB.addRun(Run(1, *[45]))

        testFileC = File(lfn="/this/is/a/lfnC", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileC.addRun(Run(2, *[48]))

        testFileD = File(lfn="/this/is/a/lfnD", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileD.addRun(Run(2, *[48]))

        testFileA.create()
        testFileB.create()
        testFileC.create()
        testFileD.create()

        testFileset = Fileset(name="TestFileset")
        testFileset.create()

        testFileset2 = Fileset(name="TestFileset2")
        testFileset2.create()

        testFileset.addFile(testFileA)
        testFileset.addFile(testFileB)
        testFileset.addFile(testFileC)
        testFileset.addFile(testFileD)
        testFileset.commit()

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=testWorkflow)
        testSubscription.create()

        # Everything above here has to exist before we get started

        testJobGroupA = JobGroup(subscription=testSubscription)
        testJobGroupB = JobGroup(subscription=testSubscription)

        testJobA = Job(name="TestJobA")
        testJobA.addFile(testFileA)
        testJobA['mask']['FirstEvent'] = 101
        testJobA['mask']['LastEvent'] = 101
        testJobA['mask']['FirstLumi'] = 102
        testJobA['mask']['LastLumi'] = 102

        testJobB = Job(name="TestJobB")
        testJobB.addFile(testFileB)

        testJobC = Job(name="TestJobC")
        testJobC.addFile(testFileC)

        testJobD = Job(name="TestJobD")
        testJobD.addFile(testFileD)

        testJobGroupA.add(testJobA)
        testJobGroupA.add(testJobB)
        testJobGroupB.add(testJobC)
        testJobGroupB.add(testJobD)

        testSubscription.bulkCommit(jobGroups=[testJobGroupA, testJobGroupB])

        self.assertEqual(testJobA.exists() > 0, True)
        self.assertEqual(testJobB.exists() > 0, True)
        self.assertEqual(testJobC.exists() > 0, True)
        self.assertEqual(testJobD.exists() > 0, True)
        self.assertEqual(testJobGroupA.exists() > 0, True)
        self.assertEqual(testJobGroupB.exists() > 0, True)

        result = testSubscription.filesOfStatus(status="Acquired")
        self.assertEqual(len(result), 4,
                         'Should have acquired 4 files, instead have %i' % (len(result)))
        self.assertEqual(len(testJobGroupA.jobs), 2)

        testJob1 = Job(id=testJobA.exists())
        testJob1.loadData()
        self.assertEqual(testJob1['mask']['FirstEvent'], 101)
        self.assertEqual(testJob1['mask']['LastEvent'], 101)
        self.assertEqual(testJob1['mask']['FirstLumi'], 102)
        self.assertEqual(testJob1['mask']['LastLumi'], 102)

        # # Test that work units were created
        #
        # testRunLumi = Run(1, 45)
        # loadByFRL = WorkUnit(taskID=testWorkflow.id, fileid=testFileA['id'], runLumi=testRunLumi)
        # loadByFRL.load()
        #
        # self.assertGreater(loadByFRL['id'], 0)
        # self.assertEqual(loadByFRL['taskid'], testWorkflow.id)
        # self.assertEqual(loadByFRL['fileid'], testFileA['id'])
        # self.assertEqual(loadByFRL['last_unit_count'], 1)  # One lumi in this job
        #
        # testRunLumi = Run(2, 48)
        # loadByFRL = WorkUnit(taskID=testWorkflow.id, fileid=testFileD['id'], runLumi=testRunLumi)
        # loadByFRL.load()
        #
        # self.assertGreater(loadByFRL['id'], 0)
        # self.assertEqual(loadByFRL['taskid'], testWorkflow.id)
        # self.assertEqual(loadByFRL['fileid'], testFileD['id'])
        # self.assertEqual(loadByFRL['last_unit_count'], 1)  # One lumi in this job

        return

    # def testBulkCommit2(self):
    #     """
    #     _testBulkCommit2_
    #
    #     Test committing everything in bulk with a different set of parameters,
    #     making sure work units are done correctly
    #     """
    #
    #     testWorkflow = Workflow(spec="spec.xml", owner="Simon", name="wf001", task="Test")
    #     testWorkflow.create()
    #
    #     testFileA = File(lfn="/this/is/a/lfnA", size=1024, events=20, locations={"goodse.cern.ch"})
    #     testFileA.addRun(Run(1, *[45]))
    #
    #     testFileB = File(lfn="/this/is/a/lfnB", size=1024, events=20, locations={"goodse.cern.ch"})
    #     testFileB.addRun(Run(1, *[45]))
    #
    #     testFileC = File(lfn="/this/is/a/lfnC", size=1024, events=20, locations={"goodse.cern.ch"})
    #     testFileC.addRun(Run(2, *[46, 48, 50]))
    #
    #     testFileD = File(lfn="/this/is/a/lfnD", size=1024, events=20, locations={"goodse.cern.ch"})
    #     testFileD.addRun(Run(2, *[49, 51, 52]))
    #
    #     testFileA.create()
    #     testFileB.create()
    #     testFileC.create()
    #     testFileD.create()
    #
    #     testFileset = Fileset(name="TestFileset")
    #     testFileset.create()
    #
    #     testFileset.addFile(testFileA)
    #     testFileset.addFile(testFileB)
    #     testFileset.addFile(testFileC)
    #     testFileset.addFile(testFileD)
    #     testFileset.commit()
    #
    #     testSubscription = Subscription(fileset=testFileset, workflow=testWorkflow)
    #     testSubscription.create()
    #
    #     # Everything above here has to exist before we get started
    #
    #     testJobGroupA = JobGroup(subscription=testSubscription)
    #     testJobGroupB = JobGroup(subscription=testSubscription)
    #
    #     testJobA = Job(name="TestJobA")
    #     testJobA.addFile(testFileA)
    #
    #     testJobB = Job(name="TestJobB")
    #     testJobB.addFile(testFileB)
    #
    #     testJobC = Job(name="TestJobC")
    #     testJobC.addFile(testFileC)
    #
    #     testJobD = Job(name="TestJobD")
    #     testJobD.addFile(testFileD)
    #
    #     testJobGroupA.add(testJobA)
    #     testJobGroupA.add(testJobB)
    #     testJobGroupB.add(testJobC)
    #     testJobGroupB.add(testJobD)
    #
    #     testSubscription.bulkCommit(jobGroups=[testJobGroupA, testJobGroupB])
    #
    #     self.assertGreater(testJobA.exists(), 0)
    #     self.assertGreater(testJobB.exists(), 0)
    #     self.assertGreater(testJobC.exists(), 0)
    #     self.assertGreater(testJobD.exists(), 0)
    #     self.assertGreater(testJobGroupA.exists(), 0)
    #     self.assertGreater(testJobGroupB.exists(), 0)
    #
    #     result = testSubscription.filesOfStatus(status="Acquired")
    #     self.assertEqual(len(result), 4, 'Should have acquired 4 files, instead have %i' % len(result))
    #     self.assertEqual(len(testJobGroupA.jobs), 2)
    #
    #     testJob1 = Job(id=testJobA.exists())
    #     testJob1.loadData()
    #
    #     # Test that work units were created
    #
    #     testRunLumi = Run(1, 45)
    #     loadByFRL = WorkUnit(taskID=testWorkflow.id, fileid=testFileA['id'], runLumi=testRunLumi)
    #     loadByFRL.load()
    #
    #     self.assertGreater(loadByFRL['id'], 0)
    #     self.assertEqual(loadByFRL['taskid'], testWorkflow.id)
    #     self.assertEqual(loadByFRL['fileid'], testFileA['id'])
    #     self.assertEqual(loadByFRL['last_unit_count'], 1)  # One lumi in this job
    #
    #     testRunLumi = Run(2, 48)
    #     loadByFRL = WorkUnit(taskID=testWorkflow.id, fileid=testFileC['id'], runLumi=testRunLumi)
    #     loadByFRL.load()
    #
    #     self.assertGreater(loadByFRL['id'], 0)
    #     self.assertEqual(loadByFRL['taskid'], testWorkflow.id)
    #     self.assertEqual(loadByFRL['fileid'], testFileC['id'])
    #     self.assertEqual(loadByFRL['last_unit_count'], 3)  # Three lumis in this job
    #
    #     return

    def testFilesOfStatusByLimit(self):
        """
        _testFilesOfstatusByLimit_

        Create a subscription and mark a couple files as failed, complete and
        acquired.  Test to make sure that the remainder of the files show up
        as available.
        """
        testWorkflow = Workflow(spec="spec.xml", owner="Simon",
                                name="wf001", task='Test')
        testWorkflow.create()

        testFileA = File(lfn="/this/is/a/lfnA", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileB = File(lfn="/this/is/a/lfnB", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileC = File(lfn="/this/is/a/lfnC", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileD = File(lfn="/this/is/a/lfnD", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileE = File(lfn="/this/is/a/lfnE", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileF = File(lfn="/this/is/a/lfnF", size=1024, events=20,
                         locations={"goodse.cern.ch"})
        testFileA.create()
        testFileB.create()
        testFileC.create()
        testFileD.create()
        testFileE.create()
        testFileF.create()

        testFileset = Fileset(name="TestFileset")
        testFileset.create()

        testFileset.addFile(testFileA)
        testFileset.addFile(testFileB)
        testFileset.addFile(testFileC)
        testFileset.addFile(testFileD)
        testFileset.addFile(testFileE)
        testFileset.addFile(testFileF)
        testFileset.commit()

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=testWorkflow)
        testSubscription.create()

        availableFiles = testSubscription.filesOfStatus("Available")
        self.assertEqual(len(availableFiles), 6)

        testSubscription.acquireFiles([testFileA, testFileB, testFileC, testFileD])
        availableFiles = testSubscription.filesOfStatus("Available")
        self.assertEqual(len(availableFiles), 2)

        files = testSubscription.filesOfStatus("Acquired")
        self.assertEqual(len(files), 4)

        testSubscription.completeFiles([testFileB, testFileC])

        files = testSubscription.filesOfStatus("Completed")

        testSubscription.failFiles([testFileA, testFileE])

        files = testSubscription.filesOfStatus("Failed")
        self.assertEqual(len(files), 2)

        testSubscription.delete()
        testWorkflow.delete()
        testFileset.delete()
        testFileA.delete()
        testFileB.delete()
        testFileC.delete()
        testFileD.delete()
        testFileE.delete()
        testFileF.delete()
        return

    def testJobs(self):
        """
        _testJobs_

        Test the Subscription.AllJob DAO turns the correct number of job
        """
        testFileset = Fileset(name="TestFileset")
        testFileset.create()

        testWorkflow = Workflow(spec="spec.xml", owner="Hassen",
                                name="wf001", task="Test")
        testWorkflow.create()
        testSubscription = Subscription(fileset=testFileset,
                                        workflow=testWorkflow)
        testSubscription.create()

        testJobGroupA = JobGroup(subscription=testSubscription)
        testJobGroupA.create()

        testJobA = Job(name="TestJobA")
        testJobB = Job(name="TestJobB")

        testJobGroupA.add(testJobA)
        testJobGroupA.add(testJobB)
        testJobGroupA.commit()

        getAllJobs = self.daofactory(classname="Subscriptions.Jobs")
        result = getAllJobs.execute(subscription=testSubscription["id"])

        assert len(result) == 2, \
            "Wrong number of jobs."

        testJobC = Job(name="TestJobC")
        testJobGroupA.add(testJobC)
        testJobGroupA.commit()

        result = getAllJobs.execute(subscription=testSubscription["id"])

        assert len(result) == 3, \
            "Wrong number of jobs."

        return

    def testSucceededJobs(self):
        """
        _testSucceededJobs_

        Test the Subscriptions.SucceededJobs DAO turns the correct
        number of succeeded job
        """
        changeJobState = self.daofactory(classname="Jobs.ChangeState")
        getSucceededJobs = self.daofactory(classname="Subscriptions.SucceededJobs")

        testFileset = Fileset(name="TestFileset")
        testFileset.create()

        testWorkflow = Workflow(spec="spec.xml", owner="Hassen",
                                name="wf001", task="Test")

        testWorkflow.create()
        testSubscription = Subscription(fileset=testFileset,
                                        workflow=testWorkflow)
        testSubscription.create()

        testJobGroupA = JobGroup(subscription=testSubscription)
        testJobGroupA.create()

        testJobA = Job(name="TestJobA")
        testJobA.create(testJobGroupA)
        testJobA.changeState("success")
        changeJobState.execute(jobs=[testJobA])
        testJobA["outcome"] = "success"
        testJobA.save()

        testJobB = Job(name="TestJobB")
        testJobB.create(testJobGroupA)
        testJobB.changeState("success")
        changeJobState.execute(jobs=[testJobB])
        testJobB["outcome"] = "success"
        testJobB.save()

        testJobC = Job(name="TestJobC")
        testJobC.create(testJobGroupA)
        testJobC.changeState("jobfailed")
        changeJobState.execute(jobs=[testJobC])
        testJobC["outcome"] = "jobfailed"
        testJobC.save()

        testJobGroupA.add(testJobA)
        testJobGroupA.add(testJobB)
        testJobGroupA.add(testJobC)
        testJobGroupA.commit()

        result = getSucceededJobs.execute(subscription=testSubscription["id"])

        assert len(result) == 2, \
            "Wrong number of jobs."

        testJobD = Job(name="TestJobD")
        testJobD.create(testJobGroupA)
        testJobD.changeState("success")
        changeJobState.execute(jobs=[testJobD])
        testJobD["outcome"] = "success"
        testJobD.save()

        testJobGroupA.add(testJobD)
        testJobGroupA.commit()

        result = getSucceededJobs.execute(subscription=testSubscription["id"])

        assert len(result) == 3, \
            "Wrong number of jobs."

        return

    def testMarkNewFinishedSubscriptions(self):
        """
        _testMarkNewFinishedSubscriptions_

        Verify that the MarkNewFinishedSubscriptions DAO works correctly for
        workflows that don't produce any files.
        """
        testOutputFileset1 = Fileset(name="TestOutputFileset1")
        testOutputFileset1.create()
        testOutputFileset2 = Fileset(name="TestOutputFileset2")
        testOutputFileset2.create()
        testOutputFileset3 = Fileset(name="TestOutputFileset3")
        testOutputFileset3.create()
        testOutputFileset4 = Fileset(name="TestOutputFileset4")
        testOutputFileset4.create()

        testMergedOutputFileset1 = Fileset(name="TestMergedOutputFileset1")
        testMergedOutputFileset1.create()
        testMergedOutputFileset2 = Fileset(name="TestMergedOutputFileset2")
        testMergedOutputFileset2.create()
        testMergedOutputFileset3 = Fileset(name="TestMergedOutputFileset3")
        testMergedOutputFileset3.create()
        testMergedOutputFileset4 = Fileset(name="TestMergedOutputFileset4")
        testMergedOutputFileset4.create()

        testInputFileset = Fileset(name="TestInputFileset")
        testInputFileset.create()
        testInputFileset.markOpen(False)

        testWorkflow1 = Workflow(spec="spec1.xml", owner="Steve",
                                 name="wf001", task="sometask")
        testWorkflow1.create()
        testWorkflow1.addOutput("out1", testOutputFileset1, testMergedOutputFileset1)

        testWorkflow2 = Workflow(spec="spec2.xml", owner="Steve",
                                 name="wf002", task="sometask")
        testWorkflow2.create()
        testWorkflow2.addOutput("out2", testOutputFileset2, testMergedOutputFileset2)

        testWorkflow3 = Workflow(spec="spec3.xml", owner="Steve",
                                 name="wf003", task="sometask")
        testWorkflow3.create()
        testWorkflow3.addOutput("out3", testOutputFileset3, testMergedOutputFileset3)

        testWorkflow4 = Workflow(spec="spec4.xml", owner="Steve",
                                 name="wf004", task="sometask")
        testWorkflow4.create()
        testWorkflow4.addOutput("out4", testOutputFileset4, testMergedOutputFileset4)

        testSubscription1 = Subscription(fileset=testInputFileset,
                                         workflow=testWorkflow1)
        testSubscription1.create()
        testSubscription2 = Subscription(fileset=testOutputFileset1,
                                         workflow=testWorkflow2)
        testSubscription2.create()
        testSubscription3 = Subscription(fileset=testOutputFileset2,
                                         workflow=testWorkflow3)
        testSubscription3.create()

        # Throw in a finished subscription
        testSubscription4 = Subscription(fileset=testOutputFileset3,
                                         workflow=testWorkflow4)
        testSubscription4.create()
        testSubscription4.markFinished()

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger=myThread.logger,
                                dbinterface=myThread.dbi)
        injected = daoFactory(classname="Workflow.MarkInjectedWorkflows")
        injected.execute(names=["wf001", "wf002", "wf003"], injected=True)
        # The first subscription is finished since the input fileset is closed and no jobs are present
        newFinishedDAO = daoFactory(classname="Subscriptions.MarkNewFinishedSubscriptions")
        newFinishedDAO.execute(self.stateID)
        finishedDAO = daoFactory(classname="Subscriptions.GetFinishedSubscriptions")
        finishedSubs = finishedDAO.execute()

        self.assertEqual(len(finishedSubs), 2,
                         "Wrong number of finished subscriptions.")

        self.assertEqual(finishedSubs[0]["id"], testSubscription1["id"],
                         "Wrong subscription id.")

        # Mark all output filesets which are input of another subscription as closed
        # That should make them all candidates for finalization
        testOutputFileset1.markOpen(False)
        testOutputFileset2.markOpen(False)
        testOutputFileset3.markOpen(False)

        newFinishedDAO.execute(self.stateID)
        finishedSubs = finishedDAO.execute()

        self.assertEqual(len(finishedSubs), 4,
                         "Wrong number of finished subscriptions.")

        finishedWorkflowCheckDAO = daoFactory(classname="Subscriptions.CountFinishedSubscriptionsByWorkflow")
        finishedWfs = [testWorkflow1.name, testWorkflow2.name, testWorkflow3.name, testWorkflow4.name]
        result = finishedWorkflowCheckDAO.execute(workflowNames=finishedWfs)
        self.assertEqual(len(result), 4)
        count = reduce(lambda x, y: {'finished': (x['finished'] + y['finished']),
                                     'open': (x['open'] + y['open'])}, result)
        self.assertEqual(count['finished'], 4)
        self.assertEqual(count['open'], 0)
        return

    def testMarkNewFinishedSubscriptionsTimeout(self):
        """
        _testMarkNewFinishedSubscriptionsTimeout_

        Verify that the finished subscriptions timeout works correctly and that
        it only returns subscriptions for workflows that are fully injected.
        """
        (testSubscription, testFileset, dummyWorkflow,
         testFileA, testFileB, testFileC) = self.createSubscriptionWithFileABC()

        testFileset.markOpen(False)
        testSubscription.create()
        testSubscription.completeFiles([testFileA, testFileB, testFileC])
        testJobGroup = JobGroup(subscription=testSubscription)
        testJobGroup.create()

        testJobA = Job(name="testA")
        testJobA.addFile(testFileA)
        testJobA["location"] = "site1"
        testJobA.create(testJobGroup)
        testJobA["state"] = "complete"

        changeJobState = self.daofactory(classname="Jobs.ChangeState")
        changeJobState.execute([testJobA])

        newFinishedDAO = self.daofactory(classname="Subscriptions.MarkNewFinishedSubscriptions")
        finishedDAO = self.daofactory(classname="Subscriptions.GetFinishedSubscriptions")
        newFinishedDAO.execute(self.stateID)
        finishedSubs = finishedDAO.execute()

        # First we have a job not in cleanout and an uninjected workflow
        self.assertEqual(len(finishedSubs), 0,
                         "There should be no finished subs.")

        time.sleep(5)

        # Even taking into account the delay, we have a not injected workflow
        newFinishedDAO.execute(self.stateID, timeOut=3)
        finishedSubs = finishedDAO.execute()
        self.assertEqual(len(finishedSubs), 0,
                         "There should be no finished subs.")

        injected = self.daofactory(classname="Workflow.MarkInjectedWorkflows")
        injected.execute(names=["wf001", "wfBOGUS"], injected=True)

        # Without timeout we still have a not-cleanout job
        newFinishedDAO.execute(self.stateID)
        finishedSubs = finishedDAO.execute()
        self.assertEqual(len(finishedSubs), 0,
                         "There should be no finished subs.")

        # Now put the timeout in the mix
        newFinishedDAO.execute(self.stateID, timeOut=3)
        finishedSubs = finishedDAO.execute()

        self.assertEqual(len(finishedSubs), 1,
                         "There should be one finished subs.")
        self.assertEqual(finishedSubs[0]["id"], testSubscription["id"],
                         "Wrong finished subscription.")

        return

    def testMarkNewFinishedSubscriptionsParentage(self):
        """
        _testMarkNewFinishedSubscriptionsTimeout_

        Verify that the finished subscriptions DAO can handle the scenario
        when an input file for a subscription which matches all the criteria
        to be defined as finished, is the parent of a file in another fileset
        from another workflow. Therefore the subscription should not be marked
        as finished yet.
        """

        # Let's get the building blocks
        elements = self.createParentageScenario()

        # We want to "finish" the supscription 1 first
        workflow1 = elements['Workflows'][0]
        injected = self.daofactory(classname="Workflow.MarkInjectedWorkflows")
        injected.execute(names=[workflow1.name], injected=True)

        fileset1 = elements['Filesets'][0]
        fileset1.markOpen(False)

        (fileA, fileB) = elements['Files'][:2]
        subscription1 = elements['Subscriptions'][0]
        subscription1.completeFiles([fileA, fileB])

        newFinishedDAO = self.daofactory(classname="Subscriptions.MarkNewFinishedSubscriptions")
        finishedDAO = self.daofactory(classname="Subscriptions.GetFinishedSubscriptions")
        deletableWorkflowDAO = self.daofactory(classname="Workflow.GetDeletableWorkflows")
        newFinishedDAO.execute(self.stateID)
        finishedSubs = finishedDAO.execute()

        self.assertEqual(len(finishedSubs), 1,
                         "There should be one finished subs. but %s found" % len(finishedSubs))

        # Marking the subscription 1 as finished would trigger the deletion of
        # file B which is an error since it is the parent of C.
        dwf = deletableWorkflowDAO.execute()
        self.assertEqual(len(dwf), 0,
                         "There should be no deletable workflow. but %s found" % len(dwf))

        # Now let's finish the second subscription
        workflow2 = elements['Workflows'][1]
        injected.execute(names=[workflow2.name], injected=True)

        fileset2 = elements['Filesets'][1]
        fileset2.markOpen(False)

        fileC = elements['Files'][2]
        subscription2 = elements['Subscriptions'][1]
        subscription2.completeFiles([fileC])

        # Now both workflow is finished
        newFinishedDAO.execute(self.stateID)
        finishedSubs = finishedDAO.execute()

        dwf = deletableWorkflowDAO.execute()
        self.assertEqual(len(dwf), 2,
                         "There should be two deletable workflow. but %s found" % len(dwf))

        self.assertEqual(len(finishedSubs), 2,
                         "There should be two finished sub.")
        self.assertEqual(finishedSubs[1]['id'], subscription2['id'],
                         "The finished sub is not the right one")

        # After another pass we pick up the first subscription
        newFinishedDAO.execute(self.stateID)
        finishedSubs = finishedDAO.execute()

        self.assertEqual(len(finishedSubs), 2,
                         "There should be two finished subs.")

    def testWhitelistBlacklist(self):
        """
        _testWhitelistBlacklist_

        Verify that the white list and black list code works.
        """
        (testSubscription, dummyTestFileset, dummyTestWorkflow,
         dummyTestFileA, dummyTestFileB, dummyTestFileC) = self.createSubscriptionWithFileABC()

        testSubscription.create()
        testSubscription.addWhiteBlackList([{"site_name": "site1", "valid": True},
                                            {"site_name": "site2", "valid": True},
                                            {"site_name": "site3", "valid": False}])
        # try to add duplicate entry : should be ignored (shouldn't throw exception)
        testSubscription.addWhiteBlackList([{"site_name": "site1", "valid": True}]),

        results = testSubscription.getWhiteBlackList()

        self.assertEqual(len(results), 3,
                         "Wrong number of items returned")
        for result in results:
            if result["site_name"] == "site1" or result["site_name"] == "site2":
                self.assertTrue(result["valid"], "Valid should be True.")
            else:
                self.assertFalse(result["valid"], "Valid should be False.")
        return

    def testSubTypesInsertion(self):
        """
        _testSubTypesInsertion_

        Test whether or not we can insert new sub types
        """

        testWorkflow = Workflow(spec="spec.xml", owner="Simon",
                                name="wf001", task="Test")
        testWorkflow.create()

        testFileset = Fileset(name="TestFileset")
        testFileset.create()

        testSubscription = Subscription(fileset=testFileset,
                                        workflow=testWorkflow,
                                        type="newType")

        testSubscription.create()

        test2 = Subscription(id=1)
        test2.load()
        self.assertEqual(test2['type'], 'newType')

        return


if __name__ == "__main__":
    unittest.main()
