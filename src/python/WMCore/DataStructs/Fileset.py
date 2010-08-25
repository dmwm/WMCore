#!/usr/bin/env python
"""
_Fileset_

Data object that contains a set of files

"""
__all__ = []
__revision__ = "$Id: Fileset.py,v 1.24 2009/12/15 14:13:50 spiga Exp $"
__version__ = "$Revision: 1.24 $"
from WMCore.DataStructs.WMObject import WMObject 

class Fileset(WMObject):
    """
    _Fileset_
    Data object that contains a set of files
    """
    def __init__(self, name=None, files = None):
        """
        Assume input files are new
        """
        self.name = name
        self.files = set()
        
        if files == None:
            self.newfiles = set()
        else:
            self.newfiles = files
            
        # assume that the fileset is open at first
        self.open = True
                
    def addFile(self, file):
        """
        Add a (set of) file(s) to the fileset
        If the file is already in self.files update that entry 
            e.g. to handle updated location
        If the file is already in self.newfiles update that entry 
            e.g. to handle updated location
        Else add the file to self.newfiles
        """
        file = self.makeset(file)
        new = file - self.getFiles(type='set')
        self.newfiles = self.makeset(self.newfiles) | new
        
        updated = self.makeset(file) & self.getFiles(type='set')
        "updated contains the original location information for updated files"
        
        self.files = self.files.union(updated)
        
    def getFiles(self, type='list'):
        if type == 'list':
            """
            List all files in the fileset - returns a set of file objects 
            sorted by lfn.
            """
            files = list(self.getFiles(type='set'))

            try:
                files.sort(lambda x, y: cmp(x['lfn'], y['lfn']))
            except Exception, e:
                print 'Problem with listFiles for fileset:', self.name
                print files.pop()
                raise e
            return files
        elif type == 'set':
            return self.makeset(self.files) | self.makeset(self.newfiles)
        elif type == 'lfn':
            """
            All the lfn's for files in the filesets 
            """
            def getLFN(file):
                return file["lfn"]
            files = map(getLFN, self.getFiles(type='list'))
            return files
        elif type == 'id':
            """
            All the id's for files in the filesets 
            """
            def getID(file):
                return file["id"]
            
            files = map(getID, self.getFiles(type='list'))
            return files
            
    def listNewFiles(self):  
        """
        List all files in the fileset that are new - e.g. not in the DB - returns a set
        """       
        return self.newfiles
    
    def commit(self):
        """
        Add contents of self.newfiles to self, empty self.newfiles
        """
        self.files = self.makeset(self.files) | self.makeset(self.newfiles)
        self.newfiles = set()
    
    def __len__(self):
        return len(self.getFiles(type='set'))
    
    def __iter__(self):
        for file in self.getFiles():
            yield file
    
    def markOpen(self, isOpen):
        """
        _markOpen_

        Change the open status of this fileset.  The isOpen parameter is a bool
        representing whether or not the fileset is open.
        """
        self.open = isOpen
