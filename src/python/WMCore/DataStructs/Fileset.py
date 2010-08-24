#!/usr/bin/env python
"""
_Fileset_

Data object that contains a set of files

"""
__all__ = []
__revision__ = "$Id: Fileset.py,v 1.5 2008/08/05 17:56:04 metson Exp $"
__version__ = "$Revision: 1.5 $"
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
        update = False
        for i in self.files:
            for j in self.makelist(file):
                update = update + bool(i == j)
        #print "should be updated: ", bool(update)
        updated = self.files & Set(self.makelist(file))
        #print "Updated:", len(updated)
        self.files = self.files | updated
        self.newfiles = self.newfiles | (Set(self.makelist(file)) - updated)
    
    def listFiles(self):
        """
        List all files in the fileset - returns a set of file objects
        """
        return self.files | self.newfiles
    
    def listLFNs(self):
        """
        All the lfn's for files in the filesets 
        """
        def getLFN(file):
             return file.dict["lfn"]
        return map(getLFN, self.listFiles())   
            
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