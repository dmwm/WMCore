#!/usr/bin/env python
"""
_Fileset_

Data object that contains a set of files

"""
__all__ = []
__revision__ = "$Id: Fileset.py,v 1.4 2008/07/21 17:25:06 metson Exp $"
__version__ = "$Revision: 1.4 $"
from sets import Set
from WMCore.DataStructs.Pickleable import Pickleable 

class Fileset(Pickleable):
    """
    _Fileset_
    Data object that contains a set of files
    """
    def __init__(self, name=None, files = Set()):
        self.files = files
        self.name = name
        self.newfiles = Set()
                
    def addFile(self, file):
        """
        Add a (set of) file(s) to the fileset
        """
        updated = self.files & Set(self.makelist(file))
        self.files = self.files | updated
        self.newfiles = self.newfiles | (Set(self.makelist(file)) - updated)
    
    def listFiles(self):
        """
        List all files in the fileset - returns a set
        """
        return self.files | self.newfiles
    
    def listNewFiles(self):  
        """
        List all files in the fileset that are new - e.g. not in the DB - returns a set
        """       
        return self.newfiles
    
    def commit(self):
        """
        Add contents of self.newfiles to self, empty self.newfiles
        """
        self.files = self.files | self.newfiles
        self.newfiles = Set()