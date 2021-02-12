#!/usr/bin/env python
"""
CouchFileset_t.py

Created by Dave Evans on 2010-10-05.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

from builtins import range
import unittest
import random

from WMCore.ACDC.CouchCollection import CouchCollection
from WMCore.ACDC.CouchFileset import CouchFileset

from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.GroupUser.User import makeUser
from WMCore.Services.UUIDLib import makeUUID

from WMCore.DataStructs.File import File


class CouchFileset_t(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Set up couch test environment.
        """
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setupCouch("wmcore-acdc-couchfileset", "GroupUser",
                                 "ACDC")
        self.owner = makeUser("DMWM", "evansde77", self.testInit.couchUrl,
                              self.testInit.couchDbName)
        self.owner.connect()
        self.owner.create()
        return

    def tearDown(self):
        """
        _tearDown_

        Clean up couch instance
        """
        self.testInit.tearDownCouch()

    def testDropCount(self):
        """
        _testDropCount_

        Verify that dropping a fileset and counting the files in a fileset works
        correctly.
        """
        testCollectionA = CouchCollection(database=self.testInit.couchDbName,
                                          url=self.testInit.couchUrl,
                                          name="Thunderstruck")
        testCollectionB = CouchCollection(database=self.testInit.couchDbName,
                                          url=self.testInit.couchUrl,
                                          name="StruckThunder")

        testFiles = []
        for i in range(5):
            testFile = File(lfn=makeUUID(), size=random.randint(1024, 4096),
                            events=random.randint(1024, 4096))
            testFiles.append(testFile)

        testFilesetA = CouchFileset(database=self.testInit.couchDbName,
                                    url=self.testInit.couchUrl,
                                    name="TestFilesetA")
        testFilesetB = CouchFileset(database=self.testInit.couchDbName,
                                    url=self.testInit.couchUrl,
                                    name="TestFilesetB")
        testFilesetC = CouchFileset(database=self.testInit.couchDbName,
                                    url=self.testInit.couchUrl,
                                    name="TestFilesetC")
        testCollectionA.addFileset(testFilesetA)
        testCollectionB.addFileset(testFilesetB)
        testCollectionB.addFileset(testFilesetC)
        testFilesetA.add(testFiles)
        testFilesetB.add(testFiles)
        testFilesetC.add(testFiles)

        testFilesetC.drop()

        testCollectionC = CouchCollection(database=self.testInit.couchDbName,
                                          url=self.testInit.couchUrl,
                                          name="StruckThunder")
        testCollectionC.populate()

        self.assertEqual(len(testCollectionC["filesets"]), 1,
                         "Error: There should be one fileset in this collection.")
        self.assertEqual(testCollectionC["filesets"][0].fileCount(), 5,
                         "Error: Wrong number of files in fileset.")

        testCollectionD = CouchCollection(database=self.testInit.couchDbName,
                                          url=self.testInit.couchUrl,
                                          name="Thunderstruck")
        testCollectionD.populate()

        self.assertEqual(len(testCollectionD["filesets"]), 1,
                         "Error: There should be one fileset in this collection.")
        self.assertEqual(testCollectionD["filesets"][0].fileCount(), 5,
                         "Error: Wrong number of files in fileset.")
        return

    def testListFiles(self):
        """
        _testListFiles_

        Verify that the files iterator works correctly.
        """
        testCollection = CouchCollection(database=self.testInit.couchDbName,
                                         url=self.testInit.couchUrl,
                                         name="Thunderstruck")
        testFileset = CouchFileset(database=self.testInit.couchDbName,
                                   url=self.testInit.couchUrl,
                                   name="TestFileset")
        testCollection.addFileset(testFileset)

        testFiles = {}
        for i in range(5):
            lfn = makeUUID()
            testFile = File(lfn=lfn, size=random.randint(1024, 4096),
                            events=random.randint(1024, 4096))
            testFiles[lfn] = testFile
            testFileset.add([testFile])

        for file in testFileset.listFiles():
            self.assertTrue(file["lfn"] in testFiles,
                            "Error: File missing.")
            self.assertEqual(file["events"], testFiles[file["lfn"]]["events"],
                             "Error: Wrong number of events.")
            self.assertEqual(file["size"], testFiles[file["lfn"]]["size"],
                             "Error: Wrong file size.")
        return

    def testFileset(self):
        """
        _testFileset_

        Verify that converting an ACDC fileset to a DataStructs fileset works
        correctly.
        """
        testCollection = CouchCollection(database=self.testInit.couchDbName,
                                         url=self.testInit.couchUrl,
                                         name="Thunderstruck")
        testFileset = CouchFileset(database=self.testInit.couchDbName,
                                   url=self.testInit.couchUrl,
                                   name="TestFileset")
        testCollection.addFileset(testFileset)

        testFiles = {}
        for i in range(5):
            lfn = makeUUID()
            testFile = File(lfn=lfn, size=random.randint(1024, 4096),
                            events=random.randint(1024, 4096))
            testFiles[lfn] = testFile
            testFileset.add([testFile])

        for file in testFileset.fileset().files:
            self.assertTrue(file["lfn"] in testFiles,
                            "Error: File missing.")
            self.assertEqual(file["events"], testFiles[file["lfn"]]["events"],
                             "Error: Wrong number of events.")
            self.assertEqual(file["size"], testFiles[file["lfn"]]["size"],
                             "Error: Wrong file size.")
        return


if __name__ == '__main__':
    unittest.main()
