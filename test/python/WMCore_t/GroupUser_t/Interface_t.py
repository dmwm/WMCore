#!/usr/bin/env python
# encoding: utf-8
"""
Interface_t.py

Created by Dave Evans on 2010-07-29.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

from builtins import range
import unittest

from WMCore.GroupUser.Interface import Interface
from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.GroupUser.User import makeUser
from WMCore.Database.CMSCouch import Document, Database

class Interface_t(unittest.TestCase):

    def setUp(self):
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setupCouch("wmcore-groupuser-interface", "GroupUser")

        #create a test owner for the collections in this test
        self.owner1 = makeUser("DMWM", "evansde77", self.testInit.couchUrl, self.testInit.couchDbName)
        self.owner1.connect()
        self.owner1.create()

        self.owner2 = makeUser("DMWM", "drsm79", self.testInit.couchUrl, self.testInit.couchDbName)
        self.owner2.connect()
        self.owner2.create()

        #self.url = os.getenv("COUCHURL", "http://127.0.0.1:5984")
        #self.database = "awesome_acdc"

    def tearDown(self):
        self.testInit.tearDownCouch()

    def testA(self):
        """ make some documents and own them"""
        guInt = Interface(self.testInit.couchUrl, self.testInit.couchDbName)


        #create a couple of docs
        couch = Database(self.testInit.couchDbName, self.testInit.couchUrl)
        for x in range(10):
            doc = Document("document%s" % x, {"Test Data": [1,2,3,4] })
            couch.queue(doc)
        couch.commit()

        self.assertEqual(len(guInt.documentsOwned(self.owner1.group.name, self.owner1.name)), 0)
        self.assertEqual(len(guInt.documentsOwned(self.owner2.group.name, self.owner2.name)), 0)

        guInt.callUpdate("ownthis","document1", group = self.owner1.group.name, user = self.owner1.name)

        self.assertTrue("document1" in guInt.documentsOwned(self.owner1.group.name, self.owner1.name))
        self.assertEqual(len(guInt.documentsOwned(self.owner1.group.name, self.owner1.name)), 1)
        self.assertEqual(len(guInt.documentsOwned(self.owner2.group.name, self.owner2.name)), 0)

        guInt.callUpdate("ownthis","document2", group = self.owner2.group.name, user = self.owner2.name)

        self.assertTrue("document2" in guInt.documentsOwned(self.owner2.group.name, self.owner2.name))
        self.assertEqual(len(guInt.documentsOwned(self.owner1.group.name, self.owner1.name)), 1)
        self.assertEqual(len(guInt.documentsOwned(self.owner2.group.name, self.owner2.name)), 1)


        guInt.callUpdate("newgroup", "group-DataOps", group = "DataOps")

        self.assertTrue(couch.documentExists("group-DataOps") )

        guInt.callUpdate("newuser", "user-damason", group = "DataOps", user = "damason")

        self.assertTrue(couch.documentExists("user-damason") )



if __name__ == '__main__':
    unittest.main()
