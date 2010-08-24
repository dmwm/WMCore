#!/usr/bin/env python
"""
_Fileset_

Data object that contains a set of files

"""
__all__ = []
__revision__ = "$Id: Fileset.py,v 1.13 2008/09/19 15:05:34 metson Exp $"
__version__ = "$Revision: 1.13 $"
from sets import Set
from WMCore.DataStructs.WMObject import WMObject 

class Fileset(WMObject):
    """
    _Fileset_
    Data object that contains a set of files
    """
    def __init__(self, name=None, files = Set(), logger=None):
        self.files = files
        self.name = name
        self.newfiles = Set()
        self.logger = logger        
                
    def addFile(self, file):
        """
        Add a (set of) file(s) to the fileset
        If the file is already in self.files update that entry 
            e.g. to handle updated location
        If the file is already in self.newfiles update that entry 
            e.g. to handle updated location
        Else add the file to self.newfiles
        """
        if not isinstance(file, Set):
            file = Set(self.makelist(file))
        new = file - self.listFiles()
        self.newfiles = self.newfiles | new
        
        updated = Set(self.makelist(file)) & self.listFiles()
        "updated contains the original location information for updated files"
        
        self.files = self.files.union(updated)
        
        if self.logger != None:
            self.logger.debug ( "u n f nf %s %s %s %s" % (len(updated), len(new), 
                            len(self.files), len(self.newfiles)))
        else:
            print "u n f nf", len(updated), len(new), \
                            len(self.files), len(self.newfiles)
        #self.commit()
    
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
    
    def __len__(self):
        return len(self.listFiles())
    
    def __iter__(self):
        for file in self.listFiles():
            yield file