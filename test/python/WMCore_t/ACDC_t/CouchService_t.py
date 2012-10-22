#!/usr/bin/env python
# encoding: utf-8
"""
CouchService_t.py

Created by Dave Evans on 2010-10-04.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import unittest
import random

from WMCore.ACDC.CouchService import CouchService
from WMCore.ACDC.CouchFileset import CouchFileset
from WMCore.ACDC.CouchCollection import CouchCollection

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

if __name__ == '__main__':
    unittest.main()
