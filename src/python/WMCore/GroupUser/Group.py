#!/usr/bin/env python
# encoding: utf-8
"""
Group.py

Created by Dave Evans on 2010-07-14.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import sys
import os
import unittest
import json

from WMCore.GroupUser.CouchObject import CouchObject


class Group(CouchObject):
    """
    _Group_
    
    Dictionary object containing attributes of a group
    
    """
    def __init__(self, **options):
        CouchObject.__init__(self)
        self.cdb_document_data = "group"
        self.setdefault('name', None)
        self.setdefault('administrators', [])
        self.setdefault('associated_sites', {})
        self.update(options)

    document_id = property(lambda x : "group-%s" % x['name'] )
    name = property(lambda x: x['name'])
                
                
                
class GroupTest(unittest.TestCase):
    """docstring for GroupTest"""
        
    def setUp(self):
        self.database = "groupuser"
        self.url = "127.0.0.1:5984"
    
    
    def testA(self):
        """instantiate and jsonise"""
        
        g1 = Group(name = "DMWM", administrators = ['evansde77', 'drsm79'])
        
        g1.setCouch(self.url, self.database)
        g1.create()
        
        g2 = Group(name = "DMWM")
        g2.setCouch(self.url, self.database)
        g2.get()
        print g2
        
        
        g1.drop()
        
if __name__ == '__main__':
    unittest.main()