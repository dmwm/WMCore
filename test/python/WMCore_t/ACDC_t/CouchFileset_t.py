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




from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.ACDC.CouchCollection import CouchCollection
from WMCore.GroupUser.User import makeUser
from WMCore.ACDC.CouchService import CouchService
from WMCore.ACDC.CouchFileset import CouchFileset
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Run import Run
from WMCore.Services.UUID import makeUUID


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
        """make a fileset"""
        
        fileset = CouchFileset(url = self.testInit.couchUrl, database = self.testInit.couchDbName, 
                               dataset = "/MinimumBias/BeamCommissioning09_v1/RAW")
        fileset.setCollection(self.collection)
        
        try:
            fileset.create()
        except Exception, ex:
            msg = "Failed to create CouchFileset:\n%s" % str(ex)
            self.fail(msg)

        service = CouchService(url = self.testInit.couchUrl, database = self.testInit.couchDbName)
        fsets = [ x for x in service.listFilesets(self.collection)]
        self.assertEqual(len(fsets), 1)
        self.assertEqual(fsets[0]['dataset'], fileset['dataset'])

        try:
            fileset.drop()
        except Exception, ex:
            msg = "Failed to drop CouchFileset:\n%s" % str(ex)
            self.fail(msg)
        fsets = [ x for x in service.listFilesets(self.collection)]
        self.assertEqual(len(fsets), 0)
        
    def testB(self):
        """put files in the fileset"""
        fileset = CouchFileset(url = self.testInit.couchUrl, database = self.testInit.couchDbName, 
                               dataset = "/MinimumBias/BeamCommissioning09_v1/RAW")
        fileset.setCollection(self.collection)
        fileset.create()
        
        files = []
        numberOfFiles = 10
        run = Run(10000000, 1,2,3,4,5,6,7,8,9,10)
        for i in range(0, numberOfFiles):
            f = File( lfn = "/store/test/some/dataset/%s.root" % makeUUID(), size = random.randint(100000000,50000000000), 
                      events = random.randint(1000, 5000))
            f.addRun(run)
            files.append(f)
        
        fileset.add(*files)

        self.assertEqual(fileset.filecount(), numberOfFiles)
        # create DataStructs.Fileset from this fileset
        try:
            dsFileset = fileset.fileset()
        except Exception, ex:
            msg = "Failed to create DataStructs.Fileset from CouchFileset: %s" % str(ex)
            self.fail(msg)
        self.assertEqual(len(dsFileset.files), numberOfFiles)
        
         
if __name__ == '__main__':
    unittest.main()