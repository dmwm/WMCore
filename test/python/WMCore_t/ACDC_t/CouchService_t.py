#!/usr/bin/env python
# encoding: utf-8
"""
CouchService_t.py

Created by Dave Evans on 2010-10-04.
Copyright (c) 2010 Fermilab. All rights reserved.
"""
from builtins import range
import unittest
import random
import time

from WMCore.ACDC.CouchService import CouchService
from WMCore.ACDC.CouchFileset import CouchFileset
from WMCore.ACDC.CouchCollection import CouchCollection

from WMCore.Database.CMSCouch import CouchServer
from WMCore.DataStructs.File import File
from WMCore.Services.UUIDLib import makeUUID

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
        testCollectionA = CouchCollection(database=self.testInit.couchDbName,
                                          url=self.testInit.couchUrl,
                                          name="Thunderstruck")
        testCollectionB = CouchCollection(database=self.testInit.couchDbName,
                                          url=self.testInit.couchUrl,
                                          name="Struckthunder")
        testCollectionC = CouchCollection(database=self.testInit.couchDbName,
                                          url=self.testInit.couchUrl,
                                          name="Thunderstruck")
        testCollectionD = CouchCollection(database=self.testInit.couchDbName,
                                          url=self.testInit.couchUrl,
                                          name="Thunderstruck")

        testFilesetA = CouchFileset(database=self.testInit.couchDbName,
                                    url=self.testInit.couchUrl,
                                    name="TestFilesetA")
        testCollectionA.addFileset(testFilesetA)
        testFilesetB = CouchFileset(database=self.testInit.couchDbName,
                                    url=self.testInit.couchUrl,
                                    name="TestFilesetB")
        testCollectionB.addFileset(testFilesetB)
        testFilesetC = CouchFileset(database=self.testInit.couchDbName,
                                    url=self.testInit.couchUrl,
                                    name="TestFilesetC")
        testCollectionC.addFileset(testFilesetC)
        testFilesetD = CouchFileset(database=self.testInit.couchDbName,
                                    url=self.testInit.couchUrl,
                                    name="TestFilesetD")
        testCollectionD.addFileset(testFilesetD)

        testFiles = []
        for i in range(5):
            testFile = File(lfn=makeUUID(), size=random.randint(1024, 4096),
                            events=random.randint(1024, 4096))
            testFiles.append(testFile)

        testFilesetA.add(testFiles)
        time.sleep(1)
        testFilesetB.add(testFiles)
        time.sleep(1)
        testFilesetC.add(testFiles)
        time.sleep(1)
        testFilesetD.add(testFiles)
        # Alan: unsure why to return this specific collection
        return testCollectionD

    def testListCollectionsFilesets(self):
        """
        _testListCollectionsFilesets_

        Verify that collections and filesets in ACDC can be listed.
        """
        testCollection = self.populateCouchDB()

        svc = CouchService(url=self.testInit.couchUrl,
                           database=self.testInit.couchDbName)

        goldenFilesetNames = ["TestFilesetA", "TestFilesetC", "TestFilesetD"]
        for fileset in svc.listFilesets(testCollection):
            self.assertTrue(fileset["name"] in goldenFilesetNames, "Error: Missing fileset.")
            goldenFilesetNames.remove(fileset["name"])
        self.assertEqual(len(goldenFilesetNames), 0, "Error: Missing filesets.")

        return

    def testTimestampAccounting(self):
        """
        _testTimestampAccounting_

        Check the correct functioning of the timestamp view in the ACDC
        couchapp and the function to remove old filesets.
        """
        self.populateCouchDB()
        svc = CouchService(url=self.testInit.couchUrl,
                           database=self.testInit.couchDbName)

        currentTime = time.time()
        database = CouchServer(self.testInit.couchUrl).connectDatabase(self.testInit.couchDbName)
        results = database.loadView("ACDC", "byTimestamp", {"endkey": currentTime})
        self.assertEqual(len(results["rows"]), 4)
        results = database.loadView("ACDC", "byTimestamp", {"endkey": currentTime - 2})
        self.assertEqual(len(results["rows"]), 2)
        results = database.loadView("ACDC", "byTimestamp", {"endkey": currentTime - 3})
        self.assertEqual(len(results["rows"]), 1)
        results = database.loadView("ACDC", "byTimestamp", {"endkey": currentTime - 5})
        self.assertEqual(len(results["rows"]), 0)
        svc.removeOldFilesets(0)
        results = database.loadView("ACDC", "byTimestamp", {"endkey": currentTime})
        self.assertEqual(len(results["rows"]), 0)
        return

    def testRemoveByCollectionName(self):
        """
        _testRemoveByCollectionName_

        Check the function to obliterate all the filesets of a collection
        """
        self.populateCouchDB()
        svc = CouchService(url=self.testInit.couchUrl,
                           database=self.testInit.couchDbName)
        database = CouchServer(self.testInit.couchUrl).connectDatabase(self.testInit.couchDbName)

        results = database.loadView("ACDC", "byCollectionName", keys=["Thunderstruck"],
                                    options={"reduce": False})
        self.assertTrue(len(results["rows"]) > 0)
        svc.removeFilesetsByCollectionName("Thunderstruck")
        results = database.loadView("ACDC", "byCollectionName", keys=["Thunderstruck"],
                                    options={"reduce": False})
        self.assertEqual(len(results["rows"]), 0)

        results = database.loadView("ACDC", "byCollectionName", keys=["Struckthunder"],
                                    options={"reduce": False})
        self.assertTrue(len(results["rows"]) > 0)
        svc.removeFilesetsByCollectionName("Struckthunder")
        results = database.loadView("ACDC", "byCollectionName", keys=["Struckthunder"],
                                    options={"reduce": False})
        self.assertEqual(len(results["rows"]), 0)
        return


if __name__ == '__main__':
    unittest.main()
