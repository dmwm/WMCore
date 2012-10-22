#!/usr/bin/env python
"""
CouchCollection_t.py

Created by Dave Evans on 2010-10-04.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import unittest
import random

from WMQuality.TestInitCouchApp import TestInitCouchApp

from WMCore.ACDC.CouchCollection import CouchCollection
from WMCore.ACDC.CouchFileset import CouchFileset
from WMCore.ACDC.CouchService import CouchService

from WMCore.DataStructs.File import File
from WMCore.GroupUser.User import makeUser
from WMCore.Services.UUID import makeUUID

class CouchCollection_t(unittest.TestCase):
    """
    Unittest for Collection specialised for CouchDB backend
    """
    def setUp(self):
        """setup couch instance"""
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setupCouch("wmcore-acdc-couchcollection", "GroupUser",
                                 "ACDC")

        self.owner = makeUser("DMWM", "evansde77", self.testInit.couchUrl,
                              self.testInit.couchDbName)
        self.owner.connect()
        self.owner.create()

    def tearDown(self):
        """
        _tearDown_

        Clean up couch.
        """
        self.testInit.tearDownCouch()
        return

    def testCreatePopulateDrop(self):
        """
        _testCreatePopulateDrop_

        Test creating, populating and dropping a collection.
        """
        testCollectionA = CouchCollection(database = self.testInit.couchDbName,
                                          url = self.testInit.couchUrl,
                                          name = "Thunderstruck")
        testCollectionB = CouchCollection(database = self.testInit.couchDbName,
                                          url = self.testInit.couchUrl,
                                          name = "StruckThunder")
        testCollectionA.setOwner(self.owner)
        testCollectionB.setOwner(self.owner)
        testCollectionA.create()
        testCollectionB.create()

        # There should be nothing in couch.  Documents are only added for
        # filesets and files.

        testFilesA = []
        for i in range(5):
            testFile = File(lfn = makeUUID(), size = random.randint(1024, 4096),
                            events = random.randint(1024, 4096))
            testFilesA.append(testFile)
        testFilesB = []
        for i in range(10):
            testFile = File(lfn = makeUUID(), size = random.randint(1024, 4096),
                            events = random.randint(1024, 4096))
            testFilesB.append(testFile)

        testFilesetA = CouchFileset(database = self.testInit.couchDbName,
                                    url = self.testInit.couchUrl,
                                    name = "TestFilesetA")
        testFilesetB = CouchFileset(database = self.testInit.couchDbName,
                                    url = self.testInit.couchUrl,
                                    name = "TestFilesetB")
        testFilesetC = CouchFileset(database = self.testInit.couchDbName,
                                    url = self.testInit.couchUrl,
                                    name = "TestFilesetC")
        testCollectionA.addFileset(testFilesetA)
        testCollectionB.addFileset(testFilesetB)
        testCollectionB.addFileset(testFilesetC)
        testFilesetA.add(testFilesA)
        testFilesetB.add(testFilesA)
        testFilesetC.add(testFilesA)
        testFilesetC.add(testFilesB)

        # Drop testCollectionA
        testCollectionA.drop()

        # Try to populate testFilesetA
        testCollectionC = CouchCollection(database = self.testInit.couchDbName,
                                          url = self.testInit.couchUrl,
                                          name = "ThunderStruck")
        testCollectionC.setOwner(self.owner)
        testCollectionC.populate()

        self.assertEqual(len(testCollectionC["filesets"]), 0,
                         "Error: There should be no filesets in this collect.")

        # Try to populate testFilesetB
        testCollectionD = CouchCollection(database = self.testInit.couchDbName,
                                          url = self.testInit.couchUrl,
                                          name = "StruckThunder")
        testCollectionD.setOwner(self.owner)
        testCollectionD.populate()

        for fileset in testCollectionD["filesets"]:
            testFiles = testFilesA
            if fileset["name"] == "TestFilesetC":
                testFiles.extend(testFilesB)

            self.assertEqual(len(testFiles), len(fileset.files.keys()),
                             "Error: Wrong number of files in fileset.")
            for testFile in testFiles:
                self.assertTrue(testFile["lfn"] in fileset.files.keys(),
                                "Error: File is missing.")
                self.assertEqual(testFile["events"],
                                 fileset.files[testFile["lfn"]]["events"],
                                 "Error: Wrong number of events.")
                self.assertEqual(testFile["size"],
                                 fileset.files[testFile["lfn"]]["size"],
                                 "Error: Wrong file size.")

        return

if __name__ == '__main__':
    unittest.main()
