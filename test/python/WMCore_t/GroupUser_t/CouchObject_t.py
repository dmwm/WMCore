#!/usr/bin/env python
# encoding: utf-8
"""
CouchObject_t.py

Created by Dave Evans on 2010-07-29.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import unittest
import os

from WMCore.GroupUser.CouchObject import CouchObject

class CouchObject_t(unittest.TestCase):
    def setUp(self):
        self.database = "groupuser"
        self.url = os.getenv("COUCHURL", "http://127.0.0.1:5984")

    def testA(self):
        """test connection"""
        cObj = CouchObject()
        cObj.cdb_database = self.database
        cObj.cdb_url = self.url

        self.assertEqual(cObj.connected, False)

        cObj.connect()

        self.assertEqual(cObj.connected, True)


    def testB(self):
        """test create/drop of document"""

        class TestObject(CouchObject):
            def __init__(self):
                CouchObject.__init__(self)
                self.cdb_document_data = "testicle"
                self.setdefault("name", "noname")
                self.setdefault("Right", "Nut")
                self.setdefault("Left", "Nut")
            document_id = property(lambda x : "test-%s" % x['name'] )


        cObj = TestObject()
        cObj.setCouch(self.url, self.database)


        cObj['name'] = "CouchObjectUnitTest"
        cObj['data'] = {  "key1": "value1", "key2": "value2"}
        cObj['listOfStuff'] = [1,2,3,4,5,6,7]


        cObj.create()


        cObj.get()

        cObj2 = TestObject()
        cObj2.setCouch(self.url, self.database)
        cObj2['name'] = "CouchObjectUnitTest"
        cObj2.get()

        cObj.drop()



if __name__ == '__main__':
    unittest.main()
