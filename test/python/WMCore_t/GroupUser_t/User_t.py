#!/usr/bin/env python
# encoding: utf-8
"""
User_t.py

Created by Dave Evans on 2010-07-29.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import unittest
import os

from WMCore.Database.CMSCouch import Document
from WMCore.GroupUser.User import User
from WMCore.GroupUser.Group import Group


class User_t(unittest.TestCase):
    def setUp(self):
        self.database = "groupuser"
        self.url = os.getenv("COUCHURL", "http://127.0.0.1:5984")

    def testA(self):
        """instantiate & jsonise"""

        u1 = User(name = "evansde77")
        g1 = Group(name = "DMWM", administrators = ["evansde77", "drsm79"])
        g1.setCouch(self.url, self.database)
        g1.connect()
        u1. setGroup(g1)

        u1.create()

        u2 = User(name = "evansde77")
        u2.setCouch(self.url, self.database)
        u2.get()

        u1.drop()
        g1.drop()


    def testB(self):
        """test owning some sample documents"""

        u1 = User(name = "evansde77")
        g1 = Group(name = "DMWM", administrators = ["evansde77", "drsm79"])
        g1.setCouch(self.url, self.database)
        g1.connect()
        u1.setGroup(g1)
        u1.create()

        doc1 = Document()
        doc1['test-data'] = {"key1" : "value1"}
        doc2 = Document()
        doc2['test-data'] = {"key2" : "value2"}
        id1 = g1.couch.commitOne(doc1)[0]
        id2 = g1.couch.commitOne(doc2)[0]
        doc1['_id'] = id1[u'id']
        doc1['_rev'] = id1[u'rev']
        doc2['_id'] = id2[u'id']
        doc2['_rev'] = id2[u'rev']

        u1.ownThis(doc1)
        u1.ownThis(doc2)

        self.assertTrue("owner" in doc1)
        self.assertTrue("owner" in doc2)
        self.assertTrue('user' in doc1['owner'])
        self.assertTrue('group' in doc1['owner'])
        self.assertTrue(doc1['owner']['user'] == u1['name'])
        self.assertTrue(doc1['owner']['group'] == u1['group'])
        self.assertTrue('user' in doc2['owner'])
        self.assertTrue('group' in doc2['owner'])
        self.assertTrue(doc2['owner']['user'] == u1['name'])
        self.assertTrue(doc2['owner']['group'] == u1['group'])


        g1.couch.delete_doc(id1[u'id'])
        g1.couch.delete_doc(id2[u'id'])
        u1.drop()
        g1.drop()


if __name__ == '__main__':
    unittest.main()
