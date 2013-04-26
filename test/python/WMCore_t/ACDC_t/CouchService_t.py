#!/usr/bin/env python
# encoding: utf-8
"""
CouchService_t.py

Created by Dave Evans on 2010-10-04.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import unittest
import random
import time

from WMCore.ACDC.CouchService import CouchService
from WMCore.ACDC.CouchFileset import CouchFileset
from WMCore.ACDC.CouchCollection import CouchCollection

from WMCore.Database.CMSCouch import CouchServer
from WMCore.DataStructs.File import File
from WMCore.Services.UUID import makeUUID

from WMQuality.TestInitCouchApp import TestInitCouchApp

class CouchService_t(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Install the couch apps.
        """
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setupCouch("wmcore-acdc-couchservice", "GroupUser",
                                 "ACDC")
        return

    def tearDown(self):
        """
        _tearDown_

        Clean up couch.
        """
        self.testInit.tearDownCouch()
        return

    def populateCouchDB(self):
        """
        _populateCouchDB_

        Populate the ACDC records
        """
        svc = CouchService(url = self.testInit.couchUrl,
                           database = self.testInit.couchDbName)

        ownerA = svc.newOwner("somegroup", "someuserA")
        ownerB = svc.newOwner("somegroup", "someuserB")

        testCollectionA = CouchCollection(database = self.testInit.couchDbName,
                                          url = self.testInit.couchUrl,
                                          name = "Thunderstruck")
        testCollectionA.setOwner(ownerA)
        testCollectionB = CouchCollection(database = self.testInit.couchDbName,
                                          url = self.testInit.couchUrl,
                                          name = "Struckthunder")
        testCollectionB.setOwner(ownerA)
        testCollectionC = CouchCollection(database = self.testInit.couchDbName,
                                          url = self.testInit.couchUrl,
                                          name = "Thunderstruck")
        testCollectionC.setOwner(ownerB)
        testCollectionD = CouchCollection(database = self.testInit.couchDbName,
                                          url = self.testInit.couchUrl,
                                          name = "Thunderstruck")
        testCollectionD.setOwner(ownerB)

        testFilesetA = CouchFileset(database = self.testInit.couchDbName,
                                    url = self.testInit.couchUrl,
                                    name = "TestFilesetA")
        testCollectionA.addFileset(testFilesetA)
        testFilesetB = CouchFileset(database = self.testInit.couchDbName,
                                    url = self.testInit.couchUrl,
                                    name = "TestFilesetB")
        testCollectionB.addFileset(testFilesetB)
        testFilesetC = CouchFileset(database = self.testInit.couchDbName,
                                    url = self.testInit.couchUrl,
                                    name = "TestFilesetC")
        testCollectionC.addFileset(testFilesetC)
        testFilesetD = CouchFileset(database = self.testInit.couchDbName,
                                    url = self.testInit.couchUrl,
                                    name = "TestFilesetD")
        testCollectionC.addFileset(testFilesetD)

        testFiles = []
        for i in range(5):
            testFile = File(lfn = makeUUID(), size = random.randint(1024, 4096),
                            events = random.randint(1024, 4096))
            testFiles.append(testFile)

        testFilesetA.add(testFiles)
        time.sleep(1)
        testFilesetB.add(testFiles)
        time.sleep(1)
        testFilesetC.add(testFiles)
        time.sleep(2)
        testFilesetD.add(testFiles)

    def testListCollectionsFilesets(self):
        """
        _testListCollectionsFilesets_

        Verify that collections and filesets in ACDC can be listed.
        """
        svc = CouchService(url = self.testInit.couchUrl,
                           database = self.testInit.couchDbName)

        ownerA = svc.newOwner("somegroup", "someuserA")
        ownerB = svc.newOwner("somegroup", "someuserB")

        testCollectionA = CouchCollection(database = self.testInit.couchDbName,
                                          url = self.testInit.couchUrl,
                                          name = "Thunderstruck")
        testCollectionA.setOwner(ownerA)
        testCollectionB = CouchCollection(database = self.testInit.couchDbName,
                                          url = self.testInit.couchUrl,
                                          name = "Struckthunder")
        testCollectionB.setOwner(ownerA)
        testCollectionC = CouchCollection(database = self.testInit.couchDbName,
                                          url = self.testInit.couchUrl,
                                          name = "Thunderstruck")
        testCollectionC.setOwner(ownerB)
        testCollectionD = CouchCollection(database = self.testInit.couchDbName,
                                          url = self.testInit.couchUrl,
                                          name = "Thunderstruck")
        testCollectionD.setOwner(ownerB)

        testFilesetA = CouchFileset(database = self.testInit.couchDbName,
                                    url = self.testInit.couchUrl,
                                    name = "TestFilesetA")
        testCollectionA.addFileset(testFilesetA)
        testFilesetB = CouchFileset(database = self.testInit.couchDbName,
                                    url = self.testInit.couchUrl,
                                    name = "TestFilesetB")
        testCollectionB.addFileset(testFilesetB)
        testFilesetC = CouchFileset(database = self.testInit.couchDbName,
                                    url = self.testInit.couchUrl,
                                    name = "TestFilesetC")
        testCollectionC.addFileset(testFilesetC)
        testFilesetD = CouchFileset(database = self.testInit.couchDbName,
                                    url = self.testInit.couchUrl,
                                    name = "TestFilesetD")
        testCollectionC.addFileset(testFilesetD)

        testFiles = []
        for i in range(5):
            testFile = File(lfn = makeUUID(), size = random.randint(1024, 4096),
                            events = random.randint(1024, 4096))
            testFiles.append(testFile)

        testFilesetA.add(testFiles)
        testFilesetB.add(testFiles)
        testFilesetC.add(testFiles)
        testFilesetD.add(testFiles)

        goldenCollectionNames = ["Thunderstruck", "Struckthunder"]
        for collection in svc.listCollections(ownerA):
            self.assertTrue(collection["name"] in goldenCollectionNames,
                            "Error: Missing collection name.")
            goldenCollectionNames.remove(collection["name"])
        self.assertEqual(len(goldenCollectionNames), 0,
                         "Error: Missing collections.")

        goldenFilesetNames = ["TestFilesetC", "TestFilesetD"]
        for fileset in svc.listFilesets(testCollectionD):
            self.assertTrue(fileset["name"] in goldenFilesetNames,
                            "Error: Missing fileset.")
            goldenFilesetNames.remove(fileset["name"])
        self.assertEqual(len(goldenFilesetNames), 0,
                         "Error: Missing filesets.")

        return

    def testOwners(self):
        """
        _testOwners_

        Verify that owners can be created, listed and removed.
        """
        svc = CouchService(url = self.testInit.couchUrl,
                           database = self.testInit.couchDbName)
        self.assertEqual(svc.listOwners(), [])

        owner = svc.newOwner("somegroup", "someuser")

        self.failUnless(len(svc.listOwners()) == 1 )

        owner2 = svc.listOwners()[0]
        self.assertEqual(str(owner2['group']), owner['group'])
        self.assertEqual(str(owner2['name']), owner['name'])

        svc.removeOwner(owner2)
        self.failUnless(len(svc.listOwners()) == 0)
        return

    def testTimestampAccounting(self):
        """
        _testTimestampAccounting_

        Check the correct functioning of the timestamp view in the ACDC
        couchapp and the function to remove old filesets.
        """
        self.populateCouchDB()
        svc = CouchService(url = self.testInit.couchUrl,
                           database = self.testInit.couchDbName)

        currentTime = time.time()
        database = CouchServer(self.testInit.couchUrl).connectDatabase(self.testInit.couchDbName)
        results = database.loadView("ACDC", "byTimestamp", {"endkey" : currentTime})
        self.assertEqual(len(results["rows"]), 4)
        results = database.loadView("ACDC", "byTimestamp", {"endkey" : currentTime - 2})
        self.assertEqual(len(results["rows"]), 3)
        results = database.loadView("ACDC", "byTimestamp", {"endkey" : currentTime - 3})
        self.assertEqual(len(results["rows"]), 2)
        results = database.loadView("ACDC", "byTimestamp", {"endkey" : currentTime - 4})
        self.assertEqual(len(results["rows"]), 1)
        results = database.loadView("ACDC", "byTimestamp", {"endkey" : currentTime - 5})
        self.assertEqual(len(results["rows"]), 0)
        svc.removeOldFilesets(0)
        results = database.loadView("ACDC", "byTimestamp", {"endkey" : currentTime})
        self.assertEqual(len(results["rows"]), 0)
        return

    def testRemoveByCollectionName(self):
        """
        _testRemoveByCollectionName_

        Check the function to obliterate all the filesets of a collection
        """
        self.populateCouchDB()
        svc = CouchService(url = self.testInit.couchUrl,
                           database = self.testInit.couchDbName)
        database = CouchServer(self.testInit.couchUrl).connectDatabase(self.testInit.couchDbName)

        results = database.loadView("ACDC", "byCollectionName", keys = ["Thunderstruck"])
        self.assertTrue(len(results["rows"]) > 0)
        svc.removeFilesetsByCollectionName("Thunderstruck")
        results = database.loadView("ACDC", "byCollectionName", keys = ["Thunderstruck"])
        self.assertEqual(len(results["rows"]), 0)
        results = database.loadView("ACDC", "byCollectionName", keys = ["Struckthunder"])
        self.assertTrue(len(results["rows"]) > 0)
        svc.removeFilesetsByCollectionName("Struckthunder")
        results = database.loadView("ACDC", "byCollectionName", keys = ["Struckthunder"])
        self.assertEqual(len(results["rows"]), 0)
        return

if __name__ == '__main__':
    unittest.main()
