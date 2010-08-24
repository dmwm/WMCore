
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
        self.newfiles = self.newfiles | Set(self.makelist(file))
    
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