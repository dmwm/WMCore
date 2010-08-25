#!/usr/bin/env python
# encoding: utf-8
"""
Collection.py

Created by Dave Evans on 2010-03-11.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import sys
import os
import unittest
from WMCore.DataStructs.WMObject import WMObject
import WMCore.ACDC.CollectionTypes as CollectionTypes

from WMCore.GroupUser.User import User
import WMCore.GroupUser.Decorators as GUDecorators





class Collection(dict, WMObject):
    def __init__(self, **options):
        dict.__init__(self)
        WMObject.__init__(self)
        self.setdefault("name", None)
        self.setdefault("collection_id", None)
        self.setdefault("collection_type", CollectionTypes.GenericCollection)
        self.setdefault("associated_filesets", {}) 
        self.setdefault("filesets", {})
        self.update(options)
        self.owner = None   


    def setOwner(self, userInstance):
        """
        _setOwner_
        
        Provide a WMCore.GroupUser.User instance that will act as the owner of this 
        collection
        
        """
        self.owner = userInstance

    def create(self):
        """
        _create_
        
        Create this Collection in the back end
        
        """
        pass
    
    def populate(self):
        """
        _populate_
        
        Pull in all filesets & file entries
        
        """

class CollectionTests(unittest.TestCase):
    def setUp(self):
        pass

    def testA(self):
        """instantiation"""
        try:
            coll = Collection()
        except Exception as  ex:
            msg = "Unable to instantiate Collection with no args"
            msg += "\n%s" % str(ex)
            self.fail(msg)
        

if __name__ == '__main__':
    unittest.main()