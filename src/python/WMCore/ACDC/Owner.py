#!/usr/bin/env python
# encoding: utf-8
"""
Owner.py

Created by Dave Evans on 2010-03-11.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import sys
import os
import unittest
from WMCore.DataStructs.WMObject import WMObject

class Owner(dict, WMObject):
    def __init__(self, **options):
        dict.__init__(self)
        WMObject.__init__(self)
        self.setdefault("name")
        self.setdefault("group")
        self.setdefault("owner_id")
        self.update(options)

    def create(self):
        """
        _create_
        
        Create this owner in the backend
        """
        pass
        
    def get(self):
        """
        _get_
        
        Get the record for this owner/find the owner_id
        """
        pass
        
    def drop(self):
        """
        _drop_
        
        remove this owner from the backend
        """
        pass
        
        
    def listCollections(self):
        """
        _listCollections_
        
        List collection names belonging to this owner
        """
        return []


class OwnerTests(unittest.TestCase):
    def setUp(self):
        pass

    def testA(self):
        """instantiation"""
        try:
            owner = Owner()
        except Exception as ex:
            msg = "Failure to instantiate Owner with no args\n"
            msg += str(ex)
            self.fail(msg)
        

if __name__ == '__main__':
    unittest.main()