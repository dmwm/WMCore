#!/usr/bin/env python
"""
_Fileset_

Data object that contains a set of files

"""
__all__ = []
__revision__ = "$Id: Fileset.py,v 1.16 2008/09/29 16:04:57 metson Exp $"
__version__ = "$Revision: 1.16 $"
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
        file = self.makeset(file)
        new = file - self.getFiles(type='set')
        self.newfiles = self.newfiles | new
        
        updated = self.makeset(file) & self.getFiles(type='set')
        "updated contains the original location information for updated files"
        
        self.files = self.files.union(updated)
        
        if self.logger != None:
            self.logger.debug ( "u n f nf %s %s %s %s" % (len(updated), len(new), 
                            len(self.files), len(self.newfiles)))
        #else:
        #    print "u n f nf", len(updated), len(new), \
        #                    len(self.files), len(self.newfiles)
        #self.commit()
    
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
                return file.dict["lfn"]
            files = map(getLFN, self.getFiles(type='set'))
            files.sort()
            return files
    
    def listFiles(self):
        """
        To be deprecated
        """
        return self.getFiles()
    
    def listLFNs(self):
        """
        To be deprecated
        """
        return self.getFiles(type='lfn')
            
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
        self.newfiles = Set()
    
    def __len__(self):
        return len(self.getFiles(type='set'))
    
    def __iter__(self):
        for file in self.listFiles():
            yield file