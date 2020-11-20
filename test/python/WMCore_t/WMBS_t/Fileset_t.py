#!/usr/bin/env python
"""
_Fileset_t_

Unit tests for the WMBS Fileset class.
"""

import threading
import unittest

from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.Run import Run
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMQuality.TestInit import TestInit


class FilesetTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        WMBS tables.
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
        locationAction.execute(siteName="site1", pnn="T2_CH_CERN")
        return

    def tearDown(self):
        """
        _tearDown_

        Drop all the WMBS tables.
        """
        self.testInit.clearDatabase()

    def testCreateDeleteExists(self):
        """
        _testCreateDeleteExists_

        Create a delete a fileset object while also using the exists() method
        to determine the the create() and delete() methods succeeded.
        """
        testFileset = Fileset(name="TestFileset")

        assert testFileset.exists() is False, \
            "ERROR: Fileset exists before it was created"

        testFileset.create()

        assert testFileset.exists() >= 0, \
            "ERROR: Fileset does not exist after it was created"

        testFileset.delete()

        assert testFileset.exists() is False, \
            "ERROR: Fileset exists after it was deleted"

        return

    def testCreateDeleteLongChar(self):
        """
        _testCreateDeleteLongChar_

        Create and delete a fileset name with 750 characters, InnoDB tables
        are limited to 767 chars
        """
        # fileset name with 1000 chars
        fsetName = "test_Fileset_with_50_character_and_anything_else_" * 20
        testFileset = Fileset(name=fsetName)

        self.assertFalse(testFileset.exists())
        testFileset.create()
        self.assertTrue(testFileset.exists() >= 0)
        testFileset.delete()
        self.assertFalse(testFileset.exists())

        # fileset name with 1500 chars
        fsetName = "test_Fileset_with_50_character_and_anything_else_" * 30
        testFileset = Fileset(name=fsetName)

        self.assertFalse(testFileset.exists())
        with self.assertRaises(Exception):
            testFileset.create()

        return

    def testCreateTransaction(self):
        """
        _testCreateTransaction_

        Create a Fileset and commit it to the database and then roll back the
        transaction.  Use the fileset's exists() method to verify that it
        doesn't exist in the database before create() is called, that is does
        exist after create() is called and that it does not exist after the
        transaction is rolled back.
        """
        myThread = threading.currentThread()
        myThread.transaction.begin()

        testFileset = Fileset(name="TestFileset")

        assert testFileset.exists() is False, \
            "ERROR: Fileset exists before it was created"

        testFileset.create()

        assert testFileset.exists() >= 0, \
            "ERROR: Fileset does not exist after it was created"

        myThread.transaction.rollback()

        assert testFileset.exists() is False, \
            "ERROR: Fileset exists after transaction was rolled back."

        return

    def testDeleteTransaction(self):
        """
        _testDeleteTransaction_

        Create a fileset and commit it to the database.  Delete the fileset
        and verify that it is no longer in the database using the exists()
        method.  Rollback the transaction and verify with the exists() method
        that the fileset is in the database.
        """
        testFileset = Fileset(name="TestFileset")

        assert testFileset.exists() is False, \
            "ERROR: Fileset exists before it was created"

        testFileset.create()

        assert testFileset.exists() >= 0, \
            "ERROR: Fileset does not exist after it was created"

        myThread = threading.currentThread()
        myThread.transaction.begin()

        testFileset.delete()

        assert testFileset.exists() is False, \
            "ERROR: Fileset exists after it was deleted"

        myThread.transaction.rollback()

        assert testFileset.exists() >= 0, \
            "ERROR: Fileset doesn't exist after transaction was rolled back."

        return

    def testLoad(self):
        """
        _testLoad_

        Test retrieving fileset metadata via the id and the
        name.
        """
        testFilesetA = Fileset(name="TestFileset")
        testFilesetA.create()

        testFilesetB = Fileset(name=testFilesetA.name)
        testFilesetB.load()
        testFilesetC = Fileset(id=testFilesetA.id)
        testFilesetC.load()

        self.assertTrue(isinstance(testFilesetB.id, int), "Fileset id is not an int.")
        self.assertTrue(isinstance(testFilesetC.id, int), "Fileset id is not an int.")

        assert testFilesetB.id == testFilesetA.id, \
            "ERROR: Load from name didn't load id"

        assert testFilesetC.name == testFilesetA.name, \
            "ERROR: Load from id didn't load name"

        # Verify we can add files to a loaded fileset.
        testFileA = File(lfn="/this/is/a/lfnA", size=1024,
                         events=20, checksums={'cksum': 3})
        testFileA.create()
        testFilesetC.addFile(testFileA)
        testFilesetC.commit()

        testFilesetC.loadData()
        assert len(testFilesetC.files) == 1, \
            "Error: Wrong number of files."

        testFilesetA.delete()
        return

    def testLoadData(self):
        """
        _testLoadData_

        Test saving and loading all fileset information.
        """
        testFileA = File(lfn="/this/is/a/lfnA", size=1024,
                         events=20, checksums={'cksum': 3})
        testFileA.addRun(Run(1, *[45]))
        testFileB = File(lfn="/this/is/a/lfnB", size=1024,
                         events=20, checksums={'cksum': 3})
        testFileB.addRun(Run(1, *[45]))
        testFileC = File(lfn="/this/is/a/lfnC", size=1024,
                         events=20, checksums={'cksum': 3})
        testFileC.addRun(Run(1, *[45]))
        testFileA.create()
        testFileB.create()
        testFileC.create()

        testFilesetA = Fileset(name="TestFileset")
        testFilesetA.create()

        testFilesetA.addFile(testFileA)
        testFilesetA.addFile(testFileB)
        testFilesetA.addFile(testFileC)
        testFilesetA.commit()

        testFilesetB = Fileset(name=testFilesetA.name)
        testFilesetB.loadData()
        testFilesetC = Fileset(id=testFilesetA.id)
        testFilesetC.loadData()

        assert testFilesetB.id == testFilesetA.id, \
            "ERROR: Load from name didn't load id"

        assert testFilesetC.name == testFilesetA.name, \
            "ERROR: Load from id didn't load name"

        goldenFiles = [testFileA, testFileB, testFileC]
        for filesetFile in testFilesetB.files:
            assert filesetFile in goldenFiles, \
                "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile)

        assert len(goldenFiles) == 0, \
            "ERROR: Fileset is missing files"

        goldenFiles = [testFileA, testFileB, testFileC]
        for filesetFile in testFilesetC.files:
            assert filesetFile in goldenFiles, \
                "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile)

        assert len(goldenFiles) == 0, \
            "ERROR: Fileset is missing files"

        testFilesetA.delete()
        testFileA.delete()
        testFileB.delete()
        testFileC.delete()

    def testGetFiles(self):
        """
        _testGetFiles_

        Create a fileset with three files and exercise the getFiles() method to
        make sure that all the results it returns are consistant.
        """
        testFileA = File(lfn="/this/is/a/lfnA", size=1024,
                         events=20, checksums={'cksum': 3})
        testFileA.addRun(Run(1, *[45]))
        testFileB = File(lfn="/this/is/a/lfnB", size=1024,
                         events=20, checksums={'cksum': 3})
        testFileB.addRun(Run(1, *[45]))
        testFileC = File(lfn="/this/is/a/lfnC", size=1024,
                         events=20, checksums={'cksum': 3})
        testFileC.addRun(Run(1, *[45]))
        testFileA.create()
        testFileB.create()
        testFileC.create()

        testFilesetA = Fileset(name="TestFileset")
        testFilesetA.create()

        testFilesetA.addFile(testFileA)
        testFilesetA.addFile(testFileB)
        testFilesetA.addFile(testFileC)
        testFilesetA.commit()

        filesetFiles = testFilesetA.getFiles(type="list")

        goldenFiles = [testFileA, testFileB, testFileC]
        for filesetFile in filesetFiles:
            assert filesetFile in goldenFiles, \
                "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile)

        assert len(goldenFiles) == 0, \
            "ERROR: Not all files in fileset"

        filesetFiles = testFilesetA.getFiles(type="set")

        goldenFiles = [testFileA, testFileB, testFileC]
        for filesetFile in filesetFiles:
            assert filesetFile in goldenFiles, \
                "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile)

        assert len(goldenFiles) == 0, \
            "ERROR: Not all files in fileset"

        filesetLFNs = testFilesetA.getFiles(type="lfn")

        goldenLFNs = [testFileA["lfn"], testFileB["lfn"], testFileC["lfn"]]
        for filesetLFN in filesetLFNs:
            assert filesetLFN in goldenLFNs, \
                "ERROR: Unknown lfn in fileset"
            goldenLFNs.remove(filesetLFN)

        assert len(goldenLFNs) == 0, \
            "ERROR: Not all lfns in fileset"

        filesetIDs = testFilesetA.getFiles(type="id")

        goldenIDs = [testFileA["id"], testFileB["id"], testFileC["id"]]
        for filesetID in filesetIDs:
            assert filesetID in goldenIDs, \
                "ERROR: Unknown id in fileset"
            goldenIDs.remove(filesetID)

        assert len(goldenIDs) == 0, \
            "ERROR: Not all ids in fileset"

    def testFileCreate(self):
        """
        _testFileCreate_

        Create several files and add them to the fileset.  Test to make sure
        that the commit() fileset method will add the files to the database
        if they are not in the database.  Also verify that files are correctly
        marked as available.
        """
        testFileA = File(lfn="/this/is/a/lfnA", size=1024,
                         events=20, checksums={'cksum': 3},
                         locations=set(["T2_CH_CERN"]))
        testFileA.addRun(Run(1, *[45]))
        testFileB = File(lfn="/this/is/a/lfnB", size=1024,
                         events=20, checksums={'cksum': 3},
                         locations=set(["T2_CH_CERN"]))
        testFileB.addRun(Run(1, *[45]))
        testFileC = File(lfn="/this/is/a/lfnC", size=1024,
                         events=20, checksums={'cksum': 3},
                         locations=set(["T2_CH_CERN"]))
        testFileC.addRun(Run(1, *[45]))
        testFileC.create()

        testWorkflowA = Workflow(spec="spec1.xml", owner="Hassen",
                                 name="wf001", task="sometask")
        testWorkflowA.create()

        testFilesetA = Fileset(name="TestFileset")
        testFilesetA.create()

        testSubscriptionA = Subscription(fileset=testFilesetA,
                                         workflow=testWorkflowA)
        testSubscriptionA.create()

        testFilesetA.addFile(testFileA)
        testFilesetA.addFile(testFileB)
        testFilesetA.addFile(testFileC)
        testFilesetA.commit()

        testFilesetB = Fileset(name=testFilesetA.name)
        testFilesetB.loadData()
        testFilesetC = Fileset(id=testFilesetA.id)
        testFilesetC.loadData()

        assert testFilesetB.id == testFilesetA.id, \
            "ERROR: Load from name didn't load id"

        assert testFilesetC.name == testFilesetA.name, \
            "ERROR: Load from id didn't load name"

        goldenFiles = [testFileA, testFileB, testFileC]
        for filesetFile in testFilesetB.files:
            assert filesetFile in goldenFiles, \
                "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile)

        assert len(goldenFiles) == 0, \
            "ERROR: Fileset is missing files"

        goldenFiles = [testFileA, testFileB, testFileC]
        for filesetFile in testSubscriptionA.filesOfStatus("Available"):
            assert filesetFile["lfn"] in goldenFiles, \
                "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile["lfn"])

        assert len(goldenFiles) == 0, \
            "ERROR: Fileset is missing files"

        goldenFiles = [testFileA, testFileB, testFileC]
        for filesetFile in testFilesetC.files:
            assert filesetFile in goldenFiles, \
                "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile)

        assert len(goldenFiles) == 0, \
            "ERROR: Fileset is missing files"

        return

    def testFileCreateTransaction(self):
        """
        _testFileCreateTransaction_

        Create several files and add them to a fileset.  Commit the fileset
        and the files to the database, verifying that they can loaded back
        from the database.  Rollback the transaction to the point after the
        fileset has been created buy before the files have been associated with
        the filset.  Load the filesets from the database again and verify that
        they do not have any files.
        """
        testFileA = File(lfn="/this/is/a/lfnA", size=1024,
                         events=20, checksums={'cksum': 3})
        testFileA.addRun(Run(1, *[45]))
        testFileB = File(lfn="/this/is/a/lfnB", size=1024,
                         events=20, checksums={'cksum': 3})
        testFileB.addRun(Run(1, *[45]))
        testFileC = File(lfn="/this/is/a/lfnC", size=1024,
                         events=20, checksums={'cksum': 3})
        testFileC.addRun(Run(1, *[45]))
        testFileC.create()

        testFilesetA = Fileset(name="TestFileset")
        testFilesetA.create()

        myThread = threading.currentThread()
        myThread.transaction.begin()

        testFilesetA.addFile(testFileA)
        testFilesetA.addFile(testFileB)
        testFilesetA.addFile(testFileC)
        testFilesetA.commit()

        testFilesetB = Fileset(name=testFilesetA.name)
        testFilesetB.loadData()
        testFilesetC = Fileset(id=testFilesetA.id)
        testFilesetC.loadData()

        assert testFilesetB.id == testFilesetA.id, \
            "ERROR: Load from name didn't load id"

        assert testFilesetC.name == testFilesetA.name, \
            "ERROR: Load from id didn't load name"

        goldenFiles = [testFileA, testFileB, testFileC]
        for filesetFile in testFilesetB.files:
            assert filesetFile in goldenFiles, \
                "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile)

        assert len(goldenFiles) == 0, \
            "ERROR: Fileset is missing files"

        goldenFiles = [testFileA, testFileB, testFileC]
        for filesetFile in testFilesetC.files:
            assert filesetFile in goldenFiles, \
                "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile)

        assert len(goldenFiles) == 0, \
            "ERROR: Fileset is missing files"

        myThread.transaction.rollback()

        testFilesetB.loadData()
        testFilesetC.loadData()

        assert len(testFilesetB.files) == 0, \
            "ERROR: Fileset B has too many files"

        assert len(testFilesetC.files) == 0, \
            "ERROR: Fileset C has too many files"

        testFilesetA.delete()
        testFileA.delete()
        testFileB.delete()
        testFileC.delete()

    def testMarkOpen(self):
        """
        _testMarkOpen_

        Test that setting the openess of a fileset in the constructor works as
        well as changing it with the markOpen() method.
        """
        testFilesetA = Fileset(name="TestFileset1", is_open=False)
        testFilesetA.create()
        testFilesetB = Fileset(name="TestFileset2", is_open=True)
        testFilesetB.create()

        testFilesetC = Fileset(name=testFilesetA.name)
        testFilesetC.load()
        testFilesetD = Fileset(name=testFilesetB.name)
        testFilesetD.load()

        assert testFilesetC.open is False, \
            "ERROR: FilesetC should be closed."

        assert testFilesetD.open is True, \
            "ERROR: FilesetD should be open."

        testFilesetA.markOpen(True)
        testFilesetB.markOpen(False)

        testFilesetE = Fileset(name=testFilesetA.name)
        testFilesetE.load()
        testFilesetF = Fileset(name=testFilesetB.name)
        testFilesetF.load()

        assert testFilesetE.open is True, \
            "ERROR: FilesetE should be open."

        assert testFilesetF.open is False, \
            "ERROR: FilesetF should be closed."

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger=myThread.logger,
                                dbinterface=myThread.dbi)
        openFilesetDAO = daoFactory(classname="Fileset.ListOpen")
        openFilesetNames = openFilesetDAO.execute()

        assert len(openFilesetNames) == 1, \
            "ERROR: Too many open filesets."

        assert "TestFileset1" in openFilesetNames, \
            "ERROR: Wrong fileset listed as open."

        return

    def testFilesetClosing(self):
        """
        _testFilesetClosing_

        Verify the proper operation of the closable fileset DAO object.  A
        fileset is closable if:
          - All of the subscriptions that feed it has completed processing all
            files in their input fileset
          _ All of the jobs for feeder subscriptions have completed
          - The fileset that feeds the subscription is closed
          - The workflow for the subscription is fully injected.
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

        testFilesetOpen = Fileset(name="TestFilesetOpen", is_open=True)
        testFilesetOpen.create()
        testFileA = File(lfn="/this/is/a/lfnA", size=1024,
                         events=20, checksums={'cksum': 3})
        testFileB = File(lfn="/this/is/a/lfnB", size=1024,
                         events=20, checksums={'cksum': 3})
        testFilesetOpen.addFile(testFileA)
        testFilesetOpen.addFile(testFileB)
        testFilesetOpen.commit()

        testFilesetClosed = Fileset(name="TestFilesetClosed", is_open=False)
        testFilesetClosed.create()
        testFileC = File(lfn="/this/is/a/lfnC", size=1024,
                         events=20, checksums={'cksum': 3})
        testFileD = File(lfn="/this/is/a/lfnD", size=1024,
                         events=20, checksums={'cksum': 3})
        testFilesetClosed.addFile(testFileC)
        testFilesetClosed.addFile(testFileD)
        testFilesetClosed.commit()

        testWorkflow1 = Workflow(spec="spec1.xml", owner="Steve",
                                 name="wf001", task="sometask")
        testWorkflow1.create()
        testWorkflow1.addOutput("out1", testOutputFileset1, testMergedOutputFileset1)
        testWorkflow1.addOutput("out2", testOutputFileset2, testMergedOutputFileset2)

        testWorkflow2 = Workflow(spec="spec2.xml", owner="Steve",
                                 name="wf002", task="sometask")
        testWorkflow2.create()
        testWorkflow2.addOutput("out3", testOutputFileset3, testMergedOutputFileset3)

        testWorkflow3 = Workflow(spec="spec4.xml", owner="Steve",
                                 name="wf004", task="sometask")
        testWorkflow3.create()
        testWorkflow3.addOutput("out4", testOutputFileset4, testMergedOutputFileset4)

        testSubscription1 = Subscription(fileset=testFilesetClosed,
                                         workflow=testWorkflow1)
        testSubscription1.create()
        testSubscription1.completeFiles([testFileC, testFileD])
        testSubscription2 = Subscription(fileset=testFilesetOpen,
                                         workflow=testWorkflow2)
        testSubscription2.create()
        testSubscription2.completeFiles([testFileA, testFileB])
        testSubscription3 = Subscription(fileset=testFilesetClosed,
                                         workflow=testWorkflow3)
        testSubscription3.create()

        testJobGroup = JobGroup(subscription=testSubscription1)
        testJobGroup.create()

        testJob = Job(name="TestJob1")
        testJob.create(testJobGroup)
        testJob["state"] = "executing"

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger=myThread.logger,
                                dbinterface=myThread.dbi)

        changeStateDAO = daoFactory(classname="Jobs.ChangeState")
        changeStateDAO.execute(jobs=[testJob])

        closableFilesetDAO = daoFactory(classname="Fileset.ListClosable")
        closableFilesets = closableFilesetDAO.execute()

        assert len(closableFilesets) == 0, \
            "Error: There should be no closable filesets."

        testJob["state"] = "cleanout"
        changeStateDAO.execute(jobs=[testJob])
        closableFilesets = closableFilesetDAO.execute()

        assert len(closableFilesets) == 0, \
            "Error: There should be no closable filesets."

        injected = daoFactory(classname="Workflow.MarkInjectedWorkflows")
        injected.execute(names=["wf001", "wf002", "wf003"], injected=True)

        closableFilesets = closableFilesetDAO.execute()
        goldenFilesets = ["TestOutputFileset1", "TestOutputFileset2"]

        for closableFileset in closableFilesets:
            newFileset = Fileset(id=closableFileset)
            newFileset.load()

            assert newFileset.name in goldenFilesets, \
                "Error: Unknown closable fileset"

            goldenFilesets.remove(newFileset.name)

        assert len(goldenFilesets) == 0, \
            "Error: Filesets are missing"
        return

    def testFilesetClosing2(self):
        """
        _testFilesetClosing2_

        Verify that fileset closing works correctly in the case where multiple
        subscriptions feed into a single fileset.
        """
        testOutputFileset1 = Fileset(name="TestOutputFileset1")
        testOutputFileset1.create()
        testOutputFileset2 = Fileset(name="TestOutputFileset2")
        testOutputFileset2.create()

        testMergedOutputFileset1 = Fileset(name="TestMergedOutputFileset1")
        testMergedOutputFileset1.create()
        testMergedOutputFileset2 = Fileset(name="TestMergedOutputFileset2")
        testMergedOutputFileset2.create()

        testFilesetOpen = Fileset(name="TestFilesetOpen", is_open=True)
        testFilesetOpen.create()
        testFileA = File(lfn="/this/is/a/lfnA", size=1024,
                         events=20, checksums={'cksum': 3})
        testFileB = File(lfn="/this/is/a/lfnB", size=1024,
                         events=20, checksums={'cksum': 3})
        testFilesetOpen.addFile(testFileA)
        testFilesetOpen.addFile(testFileB)
        testFilesetOpen.commit()

        testFilesetClosed1 = Fileset(name="TestFilesetClosed1", is_open=False)
        testFilesetClosed1.create()
        testFileC = File(lfn="/this/is/a/lfnC", size=1024,
                         events=20, checksums={'cksum': 3})
        testFileD = File(lfn="/this/is/a/lfnD", size=1024,
                         events=20, checksums={'cksum': 3})
        testFilesetClosed1.addFile(testFileC)
        testFilesetClosed1.addFile(testFileD)
        testFilesetClosed1.commit()

        testFilesetClosed2 = Fileset(name="TestFilesetClosed2", is_open=False)
        testFilesetClosed2.create()
        testFileE = File(lfn="/this/is/a/lfnE", size=1024,
                         events=20, checksums={'cksum': 3})
        testFileF = File(lfn="/this/is/a/lfnF", size=1024,
                         events=20, checksums={'cksum': 3})
        testFilesetClosed2.addFile(testFileE)
        testFilesetClosed2.addFile(testFileF)
        testFilesetClosed2.commit()

        testFilesetClosed3 = Fileset(name="TestFilesetClosed3", is_open=False)
        testFilesetClosed3.create()
        testFileG = File(lfn="/this/is/a/lfnG", size=1024,
                         events=20, checksums={'cksum': 3})
        testFileH = File(lfn="/this/is/a/lfnH", size=1024,
                         events=20, checksums={'cksum': 3})
        testFilesetClosed3.addFile(testFileG)
        testFilesetClosed3.addFile(testFileH)
        testFilesetClosed3.commit()

        testWorkflow1 = Workflow(spec="spec1.xml", owner="Steve",
                                 name="wf001", task="sometask")
        testWorkflow1.create()
        testWorkflow1.addOutput("out1", testOutputFileset1, testMergedOutputFileset1)

        testWorkflow2 = Workflow(spec="spec2.xml", owner="Steve",
                                 name="wf002", task="sometask")
        testWorkflow2.create()
        testWorkflow2.addOutput("out2", testOutputFileset2, testMergedOutputFileset2)

        testSubscription1 = Subscription(fileset=testFilesetOpen,
                                         workflow=testWorkflow1)
        testSubscription1.create()
        testSubscription1.completeFiles([testFileA, testFileB])
        testSubscription2 = Subscription(fileset=testFilesetClosed1,
                                         workflow=testWorkflow1)
        testSubscription2.create()

        testSubscription3 = Subscription(fileset=testFilesetClosed2,
                                         workflow=testWorkflow2)
        testSubscription3.create()
        testSubscription3.completeFiles([testFileE, testFileF])
        testSubscription4 = Subscription(fileset=testFilesetClosed3,
                                         workflow=testWorkflow2)
        testSubscription4.create()
        testSubscription4.completeFiles([testFileG, testFileH])

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger=myThread.logger,
                                dbinterface=myThread.dbi)
        injected = daoFactory(classname="Workflow.MarkInjectedWorkflows")
        injected.execute(names=["wf001", "wf002"], injected=True)
        closableFilesetDAO = daoFactory(classname="Fileset.ListClosable")
        closableFilesets = closableFilesetDAO.execute()

        goldenFilesets = ["TestOutputFileset2"]

        for closableFileset in closableFilesets:
            newFileset = Fileset(id=closableFileset)
            newFileset.load()

            assert newFileset.name in goldenFilesets, \
                "Error: Unknown closable fileset"

            goldenFilesets.remove(newFileset.name)

        assert len(goldenFilesets) == 0, \
            "Error: Filesets are missing"
        return

    def testFilesetClosing3(self):
        """
        _testFilesetClosing3_

        Verify that fileset closing works correctly in the case where multiple
        subscriptions feed into a single fileset and accounts for running jobs
        correctly.
        """
        testOutputFileset1 = Fileset(name="TestOutputFileset1")
        testOutputFileset1.create()
        testOutputFileset2 = Fileset(name="TestOutputFileset2")
        testOutputFileset2.create()

        testMergedOutputFileset1 = Fileset(name="TestMergedOutputFileset1")
        testMergedOutputFileset1.create()
        testMergedOutputFileset2 = Fileset(name="TestMergedOutputFileset2")
        testMergedOutputFileset2.create()

        testFilesetOpen = Fileset(name="TestFilesetOpen", is_open=False)
        testFilesetOpen.create()
        testFileA = File(lfn="/this/is/a/lfnA", size=1024,
                         events=20, checksums={'cksum': 3})
        testFileB = File(lfn="/this/is/a/lfnB", size=1024,
                         events=20, checksums={'cksum': 3})
        testFilesetOpen.addFile(testFileA)
        testFilesetOpen.addFile(testFileB)
        testFilesetOpen.commit()

        testFilesetClosed1 = Fileset(name="TestFilesetClosed1", is_open=False)
        testFilesetClosed1.create()
        testFileC = File(lfn="/this/is/a/lfnC", size=1024,
                         events=20, checksums={'cksum': 3})
        testFileD = File(lfn="/this/is/a/lfnD", size=1024,
                         events=20, checksums={'cksum': 3})
        testFilesetClosed1.addFile(testFileC)
        testFilesetClosed1.addFile(testFileD)
        testFilesetClosed1.commit()

        testFilesetClosed2 = Fileset(name="TestFilesetClosed2", is_open=False)
        testFilesetClosed2.create()
        testFileE = File(lfn="/this/is/a/lfnE", size=1024,
                         events=20, checksums={'cksum': 3})
        testFileF = File(lfn="/this/is/a/lfnF", size=1024,
                         events=20, checksums={'cksum': 3})
        testFilesetClosed2.addFile(testFileE)
        testFilesetClosed2.addFile(testFileF)
        testFilesetClosed2.commit()

        testFilesetClosed3 = Fileset(name="TestFilesetClosed3", is_open=False)
        testFilesetClosed3.create()
        testFileG = File(lfn="/this/is/a/lfnG", size=1024,
                         events=20, checksums={'cksum': 3})
        testFileH = File(lfn="/this/is/a/lfnH", size=1024,
                         events=20, checksums={'cksum': 3})
        testFilesetClosed3.addFile(testFileG)
        testFilesetClosed3.addFile(testFileH)
        testFilesetClosed3.commit()

        testWorkflow1 = Workflow(spec="spec1.xml", owner="Steve",
                                 name="wf001", task="sometask")
        testWorkflow1.create()
        testWorkflow1.addOutput("out1", testOutputFileset1, testMergedOutputFileset1)

        testWorkflow2 = Workflow(spec="spec2.xml", owner="Steve",
                                 name="wf002", task="sometask")
        testWorkflow2.create()
        testWorkflow2.addOutput("out2", testOutputFileset2, testMergedOutputFileset2)

        testSubscription1 = Subscription(fileset=testFilesetOpen,
                                         workflow=testWorkflow1)
        testSubscription1.create()
        testSubscription1.completeFiles([testFileA, testFileB])
        testSubscription2 = Subscription(fileset=testFilesetClosed1,
                                         workflow=testWorkflow1)
        testSubscription2.create()

        testJobGroup = JobGroup(subscription=testSubscription2)
        testJobGroup.create()
        testJob = Job(name="TestJob1")
        testJob.create(testJobGroup)

        testSubscription3 = Subscription(fileset=testFilesetClosed2,
                                         workflow=testWorkflow2)
        testSubscription3.create()
        testSubscription3.completeFiles([testFileE, testFileF])
        testSubscription4 = Subscription(fileset=testFilesetClosed3,
                                         workflow=testWorkflow2)
        testSubscription4.create()
        testSubscription4.completeFiles([testFileG, testFileH])

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger=myThread.logger,
                                dbinterface=myThread.dbi)
        injected = daoFactory(classname="Workflow.MarkInjectedWorkflows")
        injected.execute(names=["wf001", "wf002"], injected=True)
        closableFilesetDAO = daoFactory(classname="Fileset.ListClosable")
        closableFilesets = closableFilesetDAO.execute()

        goldenFilesets = ["TestOutputFileset2"]

        for closableFileset in closableFilesets:
            newFileset = Fileset(id=closableFileset)
            newFileset.load()

            assert newFileset.name in goldenFilesets, \
                "Error: Unknown closable fileset"

            goldenFilesets.remove(newFileset.name)

        assert len(goldenFilesets) == 0, \
            "Error: Filesets are missing"
        return

    def testFilesetClosing4(self):
        """
        _testFilesetClosing4_

        Verify that fileset closing works correctly when a workflow completly
        fails out and does not produce any files.
        """
        testOutputFileset1 = Fileset(name="TestOutputFileset1")
        testOutputFileset1.create()
        testOutputFileset2 = Fileset(name="TestOutputFileset2")
        testOutputFileset2.create()
        testOutputFileset3 = Fileset(name="TestOutputFileset3")
        testOutputFileset3.create()

        testMergedOutputFileset1 = Fileset(name="TestMergedOutputFileset1")
        testMergedOutputFileset1.create()
        testMergedOutputFileset2 = Fileset(name="TestMergedOutputFileset2")
        testMergedOutputFileset2.create()
        testMergedOutputFileset3 = Fileset(name="TestMergedOutputFileset3")
        testMergedOutputFileset3.create()

        testOutputFileset1.markOpen(False)
        testOutputFileset2.markOpen(True)
        testOutputFileset3.markOpen(True)

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

        testSubscription1 = Subscription(fileset=testInputFileset,
                                         workflow=testWorkflow1)
        testSubscription1.create()
        testSubscription2 = Subscription(fileset=testOutputFileset1,
                                         workflow=testWorkflow2)
        testSubscription2.create()
        testSubscription3 = Subscription(fileset=testOutputFileset2,
                                         workflow=testWorkflow3)
        testSubscription3.create()

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger=myThread.logger,
                                dbinterface=myThread.dbi)
        injected = daoFactory(classname="Workflow.MarkInjectedWorkflows")
        injected.execute(names=["wf001", "wf002", "wf003"], injected=True)
        closableFilesetDAO = daoFactory(classname="Fileset.ListClosable")
        closableFilesets = closableFilesetDAO.execute()

        assert len(closableFilesets) == 1, \
            "Error: Wrong number of closable filesets"

        assert closableFilesets[0] == testOutputFileset2.id, \
            "Error: Wrong fileset is marked as closable."

        return

    def testFilesetClosing5(self):
        """
        _testFilesetClosing5_

        Verify that fileset closing works in the case where one cleanup
        subscription is used to cleanup files from all the other merge
        subscriptions in the request.
        """
        inputFileset = Fileset(name="InputFileset")
        inputFileset.create()
        inputFileset.markOpen(False)
        cleanupFileset = Fileset(name="CleanupFileset")
        cleanupFileset.create()
        cleanupFileset.markOpen(True)

        testOutputFileset1 = Fileset(name="TestOutputFileset1")
        testOutputFileset1.create()
        testOutputFileset1.markOpen(True)
        testOutputFileset2 = Fileset(name="TestOutputFileset2")
        testOutputFileset2.create()
        testOutputFileset2.markOpen(True)
        testOutputFileset3 = Fileset(name="TestOutputFileset3")
        testOutputFileset3.create()
        testOutputFileset3.markOpen(True)

        cleanupWorkflow = Workflow(spec="spec1.xml", owner="Steve",
                                   name="wf001", task="cleanup")
        cleanupWorkflow.create()

        testWorkflow1 = Workflow(spec="spec1.xml", owner="Steve",
                                 name="wf001", task="sometask1")
        testWorkflow1.create()
        testWorkflow1.addOutput("out1", testOutputFileset1)
        testWorkflow1.addOutput("out1", cleanupFileset)

        testWorkflow2 = Workflow(spec="spec1.xml", owner="Steve",
                                 name="wf001", task="sometask2")
        testWorkflow2.create()
        testWorkflow2.addOutput("out1", testOutputFileset2)
        testWorkflow2.addOutput("out1", cleanupFileset)

        testWorkflow3 = Workflow(spec="spec1.xml", owner="Steve",
                                 name="wf001", task="sometask3")
        testWorkflow3.create()
        testWorkflow3.addOutput("out1", testOutputFileset3)
        testWorkflow3.addOutput("out1", cleanupFileset)

        cleanupSubscription = Subscription(fileset=cleanupFileset,
                                           workflow=cleanupWorkflow)
        cleanupSubscription.create()

        testSubscription1 = Subscription(fileset=inputFileset,
                                         workflow=testWorkflow1)
        testSubscription1.create()
        testSubscription2 = Subscription(fileset=testOutputFileset1,
                                         workflow=testWorkflow2)
        testSubscription2.create()
        testSubscription3 = Subscription(fileset=testOutputFileset2,
                                         workflow=testWorkflow3)
        testSubscription3.create()

        testFileA = File(lfn="/this/is/a/lfnA", size=1024,
                         events=20, checksums={'cksum': 3},
                         locations=set(["T2_CH_CERN"]))
        testFileA.addRun(Run(1, *[45]))
        testFileA.create()
        inputFileset.addFile(testFileA)
        inputFileset.commit()

        testJobGroupA = JobGroup(subscription=testSubscription1)
        testJobGroupA.create()

        testJobA = Job(name="TestJobA", files=[testFileA])
        testJobA.create(testJobGroupA)
        testJobA["state"] = "executing"

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger=myThread.logger,
                                dbinterface=myThread.dbi)
        injected = daoFactory(classname="Workflow.MarkInjectedWorkflows")
        injected.execute(names=["wf001"], injected=True)
        changeStateDAO = daoFactory(classname="Jobs.ChangeState")
        changeStateDAO.execute(jobs=[testJobA])

        closableFilesetDAO = daoFactory(classname="Fileset.ListClosable")
        closableFilesets = closableFilesetDAO.execute()

        self.assertEqual(len(closableFilesets), 0,
                         "Error: There should be no closable filesets.")

        testSubscription1.completeFiles(testFileA)
        testJobA["state"] = "cleanout"
        changeStateDAO.execute(jobs=[testJobA])

        testFileB = File(lfn="/this/is/a/lfnB", size=1024,
                         events=20, checksums={'cksum': 3},
                         locations=set(["T2_CH_CERN"]))
        testFileB.addRun(Run(1, *[45]))
        testFileB.create()
        testOutputFileset1.addFile(testFileB)
        testOutputFileset1.commit()
        cleanupFileset.addFile(testFileB)
        cleanupFileset.commit()

        closableFilesets = closableFilesetDAO.execute()
        self.assertEqual(len(closableFilesets), 1,
                         "Error: There should only be one closable fileset.")
        self.assertEqual(closableFilesets[0], testOutputFileset1.id,
                         "Error: Output fileset one should be closable.")

        testOutputFileset1.markOpen(False)

        testJobGroupB = JobGroup(subscription=testSubscription2)
        testJobGroupB.create()

        testJobB = Job(name="TestJobB", files=[testFileB])
        testJobB.create(testJobGroupB)
        testJobB["state"] = "cleanout"
        changeStateDAO.execute(jobs=[testJobB])

        testSubscription2.completeFiles([testFileB])
        testFileC = File(lfn="/this/is/a/lfnC", size=1024,
                         events=20, checksums={'cksum': 3},
                         locations=set(["T2_CH_CERN"]))
        testFileC.addRun(Run(1, *[45]))
        testFileC.create()
        testOutputFileset2.addFile(testFileC)
        testOutputFileset2.commit()
        cleanupFileset.addFile(testFileC)
        cleanupFileset.commit()

        closableFilesets = closableFilesetDAO.execute()
        self.assertEqual(len(closableFilesets), 1,
                         "Error: There should only be one closable fileset.")
        self.assertEqual(closableFilesets[0], testOutputFileset2.id,
                         "Error: Output fileset two should be closable.")

        testOutputFileset2.markOpen(False)

        testJobGroupC = JobGroup(subscription=testSubscription3)
        testJobGroupC.create()

        testJobC = Job(name="TestJobC", files=[testFileC])
        testJobC.create(testJobGroupC)
        testJobC["state"] = "cleanout"
        changeStateDAO.execute(jobs=[testJobC])

        testSubscription3.completeFiles([testFileC])
        testFileD = File(lfn="/this/is/a/lfnD", size=1024,
                         events=20, checksums={'cksum': 3},
                         locations=set(["T2_CH_CERN"]))
        testFileD.addRun(Run(1, *[45]))
        testFileD.create()
        testOutputFileset3.addFile(testFileD)
        testOutputFileset3.commit()
        cleanupFileset.addFile(testFileD)
        cleanupFileset.commit()

        closableFilesets = closableFilesetDAO.execute()
        self.assertEqual(len(closableFilesets), 2,
                         "Error: There should only be two closable filesets.")
        self.assertTrue(testOutputFileset3.id in closableFilesets,
                        "Error: Output fileset three should be closable.")
        self.assertTrue(cleanupFileset.id in closableFilesets,
                        "Error: Cleanup fileset should be closable.")
        return

    def testBulkAddDAO(self):
        """
        _testBulkAddDAO_

        Verify that the Fileset.BulkAdd DAO correct adds files to multiple
        filesets.
        """
        testFileA = File(lfn="/this/is/a/lfnA", size=1024,
                         events=20, checksums={'cksum': 3},
                         locations=set(["T2_CH_CERN"]))
        testFileA.addRun(Run(1, *[45]))
        testFileA.create()
        testFileB = File(lfn="/this/is/a/lfnB", size=1024,
                         events=20, checksums={'cksum': 3},
                         locations=set(["T2_CH_CERN"]))
        testFileB.addRun(Run(1, *[45]))
        testFileB.create()
        testFileC = File(lfn="/this/is/a/lfnC", size=1024,
                         events=20, checksums={'cksum': 3},
                         locations=set(["T2_CH_CERN"]))
        testFileC.addRun(Run(1, *[45]))
        testFileC.create()

        testFileD = File(lfn="/this/is/a/lfnD", size=1024,
                         events=20, checksums={'cksum': 3},
                         locations=set(["T2_CH_CERN"]))
        testFileD.addRun(Run(1, *[45]))
        testFileD.create()

        testWorkflowA = Workflow(spec="spec1.xml", owner="Hassen",
                                 name="wf001", task="sometask")
        testWorkflowA.create()

        testFilesetA = Fileset(name="TestFilesetA")
        testFilesetA.create()
        testFilesetB = Fileset(name="TestFilesetB")
        testFilesetB.create()

        testSubscriptionA = Subscription(fileset=testFilesetA,
                                         workflow=testWorkflowA)
        testSubscriptionA.create()
        testSubscriptionB = Subscription(fileset=testFilesetB,
                                         workflow=testWorkflowA)
        testSubscriptionB.create()

        myThread = threading.currentThread()
        myThread.transaction.begin()

        binds = [{"fileid": testFileA["id"], "fileset": testFilesetA.id},
                 {"fileid": testFileB["id"], "fileset": testFilesetA.id},
                 {"fileid": testFileC["id"], "fileset": testFilesetA.id},
                 {"fileid": testFileB["id"], "fileset": testFilesetB.id},
                 {"fileid": testFileC["id"], "fileset": testFilesetB.id},
                 {"fileid": testFileD["id"], "fileset": testFilesetB.id}]

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger=myThread.logger,
                                dbinterface=myThread.dbi)
        bulkAddAction = daoFactory(classname="Fileset.BulkAdd")
        bulkAddAction.execute(binds=binds)

        testFilesetA.loadData()
        testFilesetB.loadData()

        goldenFiles = [testFileA, testFileB, testFileC]
        for filesetFile in testFilesetA.files:
            assert filesetFile in goldenFiles, \
                "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile)

        assert len(goldenFiles) == 0, \
            "ERROR: Fileset is missing files"

        goldenFiles = [testFileA, testFileB, testFileC]
        for filesetFile in testSubscriptionA.filesOfStatus("Available"):
            assert filesetFile["lfn"] in goldenFiles, \
                "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile["lfn"])

        assert len(goldenFiles) == 0, \
            "ERROR: Fileset is missing files"

        goldenFiles = [testFileB, testFileC, testFileD]
        for filesetFile in testFilesetB.files:
            assert filesetFile in goldenFiles, \
                "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile)

        assert len(goldenFiles) == 0, \
            "ERROR: Fileset is missing files"

        goldenFiles = [testFileB, testFileC, testFileD]
        for filesetFile in testSubscriptionB.filesOfStatus("Available"):
            assert filesetFile["lfn"] in goldenFiles, \
                "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile["lfn"])

        assert len(goldenFiles) == 0, \
            "ERROR: Fileset is missing files"

        return

    def testAddBulkByLFN(self):
        """
        _testAddBulkByLFN_

        Verify that the Fileset.BulkAddByLFN DAO correct adds files to multiple
        filesets.
        """
        testFileA = File(lfn="/this/is/a/lfnA", size=1024,
                         events=20, checksums={'cksum': 3},
                         locations=set(["T2_CH_CERN"]))
        testFileA.addRun(Run(1, *[45]))
        testFileA.create()
        testFileB = File(lfn="/this/is/a/lfnB", size=1024,
                         events=20, checksums={'cksum': 3},
                         locations=set(["T2_CH_CERN"]))
        testFileB.addRun(Run(1, *[45]))
        testFileB.create()
        testFileC = File(lfn="/this/is/a/lfnC", size=1024,
                         events=20, checksums={'cksum': 3},
                         locations=set(["T2_CH_CERN"]))
        testFileC.addRun(Run(1, *[45]))
        testFileC.create()

        testFileD = File(lfn="/this/is/a/lfnD", size=1024,
                         events=20, checksums={'cksum': 3},
                         locations=set(["T2_CH_CERN"]))
        testFileD.addRun(Run(1, *[45]))
        testFileD.create()

        testWorkflowA = Workflow(spec="spec1.xml", owner="Hassen",
                                 name="wf001", task="sometask")
        testWorkflowA.create()

        testFilesetA = Fileset(name="TestFilesetA")
        testFilesetA.create()
        testFilesetB = Fileset(name="TestFilesetB")
        testFilesetB.create()

        testSubscriptionA = Subscription(fileset=testFilesetA,
                                         workflow=testWorkflowA)
        testSubscriptionA.create()
        testSubscriptionB = Subscription(fileset=testFilesetB,
                                         workflow=testWorkflowA)
        testSubscriptionB.create()

        myThread = threading.currentThread()
        myThread.transaction.begin()

        binds = [{"lfn": testFileA["lfn"], "fileset": testFilesetA.id},
                 {"lfn": testFileB["lfn"], "fileset": testFilesetA.id},
                 {"lfn": testFileC["lfn"], "fileset": testFilesetA.id},
                 {"lfn": testFileB["lfn"], "fileset": testFilesetB.id},
                 {"lfn": testFileC["lfn"], "fileset": testFilesetB.id},
                 {"lfn": testFileD["lfn"], "fileset": testFilesetB.id}]

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger=myThread.logger,
                                dbinterface=myThread.dbi)
        bulkAddAction = daoFactory(classname="Fileset.BulkAddByLFN")
        bulkAddAction.execute(binds=binds)

        testFilesetA.loadData()
        testFilesetB.loadData()

        goldenFiles = [testFileA, testFileB, testFileC]
        for filesetFile in testFilesetA.files:
            assert filesetFile in goldenFiles, \
                "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile)

        assert len(goldenFiles) == 0, \
            "ERROR: Fileset is missing files"

        goldenFiles = [testFileA, testFileB, testFileC]
        for filesetFile in testSubscriptionA.filesOfStatus("Available"):
            assert filesetFile["lfn"] in goldenFiles, \
                "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile["lfn"])

        assert len(goldenFiles) == 0, \
            "ERROR: Fileset is missing files"

        goldenFiles = [testFileB, testFileC, testFileD]
        for filesetFile in testFilesetB.files:
            assert filesetFile in goldenFiles, \
                "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile)

        assert len(goldenFiles) == 0, \
            "ERROR: Fileset is missing files"

        goldenFiles = [testFileB, testFileC, testFileD]
        for filesetFile in testSubscriptionB.filesOfStatus("Available"):
            assert filesetFile["lfn"] in goldenFiles, \
                "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile["lfn"])

        assert len(goldenFiles) == 0, \
            "ERROR: Fileset is missing files"

        return

    def testAddToWMBSInBulk(self):
        """
        testAddToWMBSInBulk

        test create and add files to fileset in one go
        """
        testFileA = File(lfn="/this/is/a/lfnA", size=1024,
                         events=20, checksums={'cksum': 3},
                         locations=set(["T2_CH_CERN"]))
        testFileA.addRun(Run(1, *[45]))

        testFileB = File(lfn="/this/is/a/lfnB", size=1024,
                         events=20, checksums={'cksum': 3},
                         locations=set(["T2_CH_CERN"]))
        testFileB.addRun(Run(1, *[45]))

        testFileC = File(lfn="/this/is/a/lfnC", size=1024,
                         events=20, checksums={'cksum': 3},
                         locations=set(["T2_CH_CERN"]))
        testFileC.addRun(Run(1, *[45]))

        testFileD = File(lfn="/this/is/a/lfnD", size=1024,
                         events=20, checksums={'cksum': 3},
                         locations=set(["T2_CH_CERN"]))
        testFileD.addRun(Run(1, *[45]))

        testWorkflowA = Workflow(spec="spec1.xml", owner="Hassen",
                                 name="wf001", task="sometask")
        testWorkflowA.create()
        testWorkflowB = Workflow(spec="spec2.xml", owner="Hassen",
                                 name="wf002", task="sometask")
        testWorkflowB.create()

        testFilesetA = Fileset(name="TestFilesetA")
        testFilesetA.create()
        testFilesetB = Fileset(name="TestFilesetB")
        testFilesetB.create()

        testSubscriptionA = Subscription(fileset=testFilesetA,
                                         workflow=testWorkflowA)
        testSubscriptionA.create()
        testSubscriptionB = Subscription(fileset=testFilesetB,
                                         workflow=testWorkflowB)
        testSubscriptionB.create()

        myThread = threading.currentThread()
        myThread.transaction.begin()

        testFilesetA.addFilesToWMBSInBulk([testFileA, testFileB, testFileC],
                                          testWorkflowA.name)
        testFilesetB.addFilesToWMBSInBulk([testFileB, testFileC, testFileD],
                                          testWorkflowB.name)

        testFilesetA.loadData()
        testFilesetB.loadData()

        goldenFiles = [testFileA, testFileB, testFileC]
        for filesetFile in testFilesetA.files:
            assert filesetFile in goldenFiles, \
                "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile)

        assert len(goldenFiles) == 0, \
            "ERROR: Fileset is missing files"

        goldenFiles = [testFileA, testFileB, testFileC]
        for filesetFile in testSubscriptionA.filesOfStatus("Available"):
            assert filesetFile["lfn"] in goldenFiles, \
                "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile["lfn"])

        assert len(goldenFiles) == 0, \
            "ERROR: Fileset is missing files"

        goldenFiles = [testFileB, testFileC, testFileD]
        for filesetFile in testFilesetB.files:
            assert filesetFile in goldenFiles, \
                "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile)

        assert len(goldenFiles) == 0, \
            "ERROR: Fileset is missing files"

        goldenFiles = [testFileB, testFileC, testFileD]
        for filesetFile in testSubscriptionB.filesOfStatus("Available"):
            assert filesetFile["lfn"] in goldenFiles, \
                "ERROR: Unknown file in fileset"
            goldenFiles.remove(filesetFile["lfn"])

        assert len(goldenFiles) == 0, \
            "ERROR: Fileset is missing files"

        return

    def testSetLastUpdate(self):
        """
        _testSetLastUpdate_

        Test that setting the lastUpdate of a fileset works when changing it
        with the setLastUpdate() method.
        """
        testFilesetA = Fileset(name="TestFileset1")
        testFilesetA.create()
        testFilesetA.setLastUpdate(10)
        testFilesetB = Fileset(name="TestFileset2")
        testFilesetB.create()
        testFilesetB.setLastUpdate(20)

        testFilesetC = Fileset(name=testFilesetA.name)
        testFilesetC.load()
        testFilesetD = Fileset(name=testFilesetB.name)
        testFilesetD.load()

        assert testFilesetC.lastUpdate == 10, \
            "ERROR: lastUpdate of FilesetC should be 10."

        assert testFilesetD.lastUpdate == 20, \
            "ERROR: lastUpdate of FilesetD should be 20."

    def testListFilesetByTask(self):
        """
        _testListFilesetByTask_

        Verify that Fileset.ListFilesetByTask DAO correct turns
        the list of fileset by task.
        """
        testWorkflow1 = Workflow(spec="spec1.xml", owner="Hassen",
                                 name="wf001", task="sometask")
        testWorkflow1.create()
        testFilesetA = Fileset(name="TestFileset1")
        testFilesetA.create()
        testSubscription1 = Subscription(fileset=testFilesetA,
                                         workflow=testWorkflow1)
        testSubscription1.create()

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger=myThread.logger,
                                dbinterface=myThread.dbi)
        listFilesetByTaskDAO = daoFactory(classname="Fileset.ListFilesetByTask")
        listFilesetByTask = listFilesetByTaskDAO.execute(task=testWorkflow1.task)

        assert len(listFilesetByTask) == 1, \
            "ERROR: listFilesetByTask should be 1."

        assert listFilesetByTask[0]['name'] == "TestFileset1", \
            "ERROR: the fileset  should be TestFileset1."

        return


if __name__ == "__main__":
    unittest.main()
