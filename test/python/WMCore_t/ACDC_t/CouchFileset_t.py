#!/usr/bin/env python
# encoding: utf-8
"""
CouchFileset_t.py

Created by Dave Evans on 2010-10-05.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import unittest
import random
import os
import nose

from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.ACDC.CouchCollection import CouchCollection
from WMCore.GroupUser.User import makeUser
from WMCore.ACDC.CouchService import CouchService
from WMCore.ACDC.CouchFileset import CouchFileset
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Run import Run
from WMCore.Services.UUID import makeUUID
from WMCore.DataStructs.Mask import Mask


class CouchFileset_t(unittest.TestCase):
    def setUp(self):
        """Set up couch test environment"""
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setupCouch("wmcore-acdc-couchfileset", "GroupUser", "ACDC")
        
        #create a test owner for the collections in this test
        self.owner = makeUser("DMWM", "evansde77", self.testInit.couchUrl, self.testInit.couchDbName)
        self.owner.connect()
        self.owner.create()
        
        
        # create a collection for the filesets in this test
        self.collection = CouchCollection(url = self.testInit.couchUrl, database = self.testInit.couchDbName, name = "Thunderstruck")
        self.collection.setOwner(self.owner)
        self.collection.create()
        
        
    def tearDown(self):
        """Clean up couch instance"""
        self.testInit.tearDownCouch()



    def testA(self):

        fs1 = CouchFileset()

        fs2 = CouchFileset(_id = "sample-fileset-id", database = self.testInit.couchDbName, url = self.testInit.couchUrl)
        fs2.setCollection(self.collection)
        self.assertEqual(fs2.exists(), False)
        fs2.create()
        self.assertEqual(fs2.exists(), True)

        fs2.makeFilelist()
        fs2.makeFilelist()
        self.assertEqual(len(fs2.filelistDocuments()), 2)
        fs2.drop()


    def testB(self):
        fs = CouchFileset(database = self.testInit.couchDbName, url = self.testInit.couchUrl)
        fs.setCollection(self.collection)
        fs.create()

        files1 = []
        files2 = []
        mask = Mask()
        numberOfFiles = 10
        run = Run(10000000, 1,2,3,4,5,6,7,8,9,10)
        for i in range(0, numberOfFiles):
            f = File( lfn = "/store/test/some/dataset/%s.root" % makeUUID(), size = random.randint(100000000,50000000000), 
                      events = random.randint(1000, 5000))
            f.addRun(run)
            files1.append(f)
            f2 = File( lfn = "/store/test/some/dataset/%s.root" % makeUUID(), size = random.randint(100000000,50000000000), 
                      events = random.randint(1000, 5000))
            f2.addRun(run)
            files2.append(f2)

        fs.add(files1, mask)
        fs.add(files2, mask)
        # Something's wrong here, but I don't know what
        # Commenting out the print statements
        # Possibly nullifying the test in the process
        self.assertEqual(len(fs.filelistDocuments()), 2)
        #print len(fs.fileset())
        fs.drop()

if __name__ == '__main__':
    unittest.main()
