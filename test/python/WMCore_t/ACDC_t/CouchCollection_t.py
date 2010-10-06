#!/usr/bin/env python
# encoding: utf-8
"""
CouchCollection_t.py

Created by Dave Evans on 2010-10-04.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import unittest
import os




from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.ACDC.CouchCollection import CouchCollection
from WMCore.GroupUser.User import makeUser
from WMCore.ACDC.CouchService import CouchService

class CouchCollection_t(unittest.TestCase):
    """
    Unittest for Collection specialised for CouchDB backend
    """
    def setUp(self):
        """setup couch instance"""
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setupCouch("wmcore-acdc-couchcollection", "GroupUser", "ACDC")
        
        #create a test owner for the collections in this test
        self.owner = makeUser("DMWM", "evansde77", self.testInit.couchUrl, self.testInit.couchDbName)
        self.owner.connect()
        self.owner.create()
        

        
        
    def tearDown(self):
        """clean up couch instance"""
        self.testInit.tearDownCouch()
        
    def testA(self):
        """
        create a collection & read it back
        """
        collection = CouchCollection(database = self.testInit.couchDbName, url = self.testInit.couchUrl, 
                                    name = "Thunderstruck")
        collection.setOwner(self.owner)
        
        try:
            collection.create()
        except Exception, ex:
            msg = "Error creating collection in couch:\n %s" % str(ex)
            self.fail(msg)

        try:
            collection.getCollectionId()
        except Exception, ex:
            msg = "Error calling getCollectionId:\n%s" % str(ex)
            self.fail(msg)
            
        service = CouchService(url = self.testInit.couchUrl, database = self.testInit.couchDbName)
        colls = [x for x in service.listCollections(self.owner)]
        self.assertEqual(len(colls), 1)
        self.assertEqual(colls[0]['name'], collection['name'])
        
        try:
            collection.drop()
        except Exception, ex:
            msg = "Error calling CouchCollection.drop:\n%s" % str(ex)
            self.fail(msg)
        colls = [x for x in service.listCollections(self.owner)]
        self.assertEqual(len(colls), 0)
    
if __name__ == '__main__':
	unittest.main()