#!/usr/bin/env python
# encoding: utf-8
"""
Fileset_t.py

Created by Dave Evans on 2010-10-04.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import unittest
from WMCore.GroupUser.User import makeUser
from WMCore.ACDC.Collection import Collection
from WMCore.ACDC.Fileset import Fileset


class Fileset_t(unittest.TestCase):


    def testA(self):
        """instantiate & set collection"""
        
        user = makeUser("somegroup", "someuser")
        coll = Collection(name = "collection", collection_id = "12345")
        coll.setOwner(user)
        
        try:
            fileset = Fileset(name = "somedataset")
        except Exception, ex: 
            msg = "Failed to instantiate Fileset: %s" % str(ex)
            self.fail(msg)
            
        fileset.setCollection(coll)
        self.assertEqual(fileset.collection, coll)
        self.assertEqual(fileset.owner, user)
        
            
        

    
if __name__ == '__main__':
	unittest.main()