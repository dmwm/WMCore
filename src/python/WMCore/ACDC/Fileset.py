#!/usr/bin/env python
# encoding: utf-8
"""
Fileset.py

Created by Dave Evans on 2010-03-18.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import unittest
from WMCore.DataStructs.WMObject import WMObject 


class Fileset(dict, WMObject):
    """
    _Fileset_
    
    """
    def __init__(self, **options):
        dict.__init__(self)
        WMObject.__init__(self)
        self.setdefault("collection_id", None)
        self.setdefault("fileset_id", None)
        self.setdefault("dataset", None)
        self.setdefault("files", {})
        self.update(options)
        self.owner = None
        self.collection = None
    

    
    def setCollection(self, collectionInstance):
        """
        _setCollection_
        
        """
        self.collection = collectionInstance
        self['collection_id'] = collectionInstance['collection_id']
        self.owner = collectionInstance.owner
        return
    
    
    def create(self):
        """
        create a new fileset within a collection
        """
        pass
    
    def get(self):
        """
        _get_
        
        populate information about this fileset
        """
        pass
        
    def drop(self):
        """
        _drop_
        
        Remove the fileset from its collection
        """
        pass
        
    def add(self, *files):
        """
        _add_
        
        Add files to this fileset 
        files should be a list of WMCore.DataStruct.File objects
        
        """
        pass
        
    def files(self):
        """
        _files_
        
        Iterate/yield the files in this fileset
        """
        pass
        
    def filecount(self):
        """
        _filecount_
        
        Total number of files in this fileset
        """
        pass
        
    def fileset(self):
        """
        _fileset_
        
        Create and return an instance of a WMCore.DataStructs.Fileset
        containing the files in this (ACDC) fileset
        
        """
        pass
        

        

class FilesetTest(unittest.TestCase):
    def setUp(self):
        pass
        
        
    def testA(self):
        """instantiation"""
        
        fs = Fileset()

    
if __name__ == '__main__':
    unittest.main()