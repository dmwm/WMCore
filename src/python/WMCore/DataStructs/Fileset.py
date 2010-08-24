#!/usr/bin/env python
"""
_Fileset_

Data object that contains a set of files

"""
__all__ = []
__revision__ = "$Id: Fileset.py,v 1.6 2008/08/09 22:14:44 metson Exp $"
__version__ = "$Revision: 1.6 $"
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
        If the file is already in self.files update that entry 
            e.g. to handle updated location
        If the file is already in self.newfiles update that entry 
            e.g. to handle updated location
        Else add the file to self.newfiles
        """
        
        new = Set(self.makelist(file)) - self.listFiles()
        updated = self.files & Set(self.makelist(file))
        self.files = self.files | updated
        self.newfiles = self.newfiles | new
        print "u n f nf", len(updated), len(new), len(self.files), len(self.newfiles)
    
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