#!/usr/bin/env python
# encoding: utf-8
"""
CouchService_t.py

Created by Dave Evans on 2010-10-04.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import unittest
import os




from WMCore.ACDC.CouchService import CouchService
from WMQuality.TestInitCouchApp import TestInitCouchApp

class CouchService_t(unittest.TestCase):
    def setUp(self):
        self.testInit = TestInitCouchApp(__file__)
        
        self.testInit.setupCouch("wmcore-acdc-couchservice", "GroupUser", "ACDC")
        
    def tearDown(self):
        self.testInit.tearDownCouch()


    def testA(self):
        """init and basic population"""
        try:
            svc = CouchService(url = self.testInit.couchUrl, database = self.testInit.couchDbName)
        except Exception, ex:
            msg = "Failed to init a CouchService; \n %s" % str(ex)
            self.fail(msg)
            
        self.assertEqual(svc.listOwners(), [])
        
        owner = svc.newOwner("somegroup", "someuser")
        
        self.failUnless(len(svc.listOwners()) == 1 )
        
        owner2 = svc.listOwners()[0]
        self.assertEqual(str(owner2['group']), owner['group'])
        self.assertEqual(str(owner2['name']), owner['name'])
        
        colls = [ x for x in svc.listCollections(owner)]
        self.assertEqual(colls, [])
        
        
    
if __name__ == '__main__':
    unittest.main()