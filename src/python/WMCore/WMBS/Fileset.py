#!/usr/bin/env python
#Turn off to many arguments
#pylint: disable-msg=R0913
#Turn off over riding built in id 
#pylint: disable-msg=W0622
"""
_Fileset_

A simple object representing a Fileset in WMBS.

A fileset is a collection of files for processing. This could be a 
complete block, a block in transfer, some user defined dataset etc.

workflow + fileset = subscription
"""

__revision__ = "$Id: Fileset.py,v 1.32 2009/01/08 21:52:40 sfoulkes Exp $"
__version__ = "$Revision: 1.32 $"

from sets import Set

from WMCore.WMBS.File import File
from WMCore.WMBS.WMBSBase import WMBSBase
from WMCore.DataStructs.Fileset import Fileset as WMFileset

class Fileset(WMBSBase, WMFileset):
    """
    A simple object representing a Fileset in WMBS.

    A fileset is a collection of files for processing. This could be a 
    complete block, a block in transfer, some user defined dataset, a 
    many file lumi-section etc.
    
    workflow + fileset = subscription
    """
    def __init__(self, name=None, id=-1, is_open=True, files=None, 
                 parents=None, parents_open=True, source=None, sourceUrl=None):
        WMBSBase.__init__(self)
        WMFileset.__init__(self, name = name, files=files)

        if parents == None:
            parents = Set()
        
        # Create a new fileset
        self.id = id
        self.open = is_open
        self.parents = parents
        self.setParentage(parents, parents_open)
        self.source = source
        self.sourceUrl = sourceUrl 
        self.lastUpdate = 0
    
    def addFile(self, file):
        """
        Add the file object to the set, but don't commit to the database
        Call commit() to do that - enables bulk operations
        """
        WMFileset.addFile(self, file)
    
    def setParentage(self, parents, parents_open):
        """
        Set parentage for this fileset - set parents to closed
        """
        if parents:
            for parent in parents:
                if isinstance(parent, Fileset):
                    self.parents.add(parent)
                else:
                    self.parents.add(Fileset(name=parent, 
                                             is_open=parents_open, 
                                             parents_open=False))
    
    def exists(self):
        """
        Does a fileset exist with this name in the database
        """
        existsAction = self.daofactory(classname='Fileset.Exists')
        return existsAction.execute(self.name, conn = self.getReadDBConn(),
                                    transaction = self.existingTransaction())
        
    def create(self):
        """
        Add the new fileset to WMBS, and commit the files
        """
        createAction = self.daofactory(classname='Fileset.New')
        createAction.execute(self.name, conn = self.getWriteDBConn(),
                             transaction = self.existingTransaction())
        self.commit()
        self.commitIfNew()
        return self
    
    def delete(self):
        """
        Remove this fileset from WMBS
        """
        action = self.daofactory(classname='Fileset.Delete')
        result = action.execute(name = self.name, conn = self.getWriteDBConn(),
                                transaction = self.existingTransaction())
        self.commitIfNew()
        return result
    
    def load(self, method='Fileset.LoadFromName'): 
        """
        Load up the files in the file set from the database, this drops new 
        files that aren't in the database. If you want to keep them call commit,
        which will then populate the fileset for you.
        """
        action = self.daofactory(classname = method)    
        values = None
        #get my details
        if method == 'Fileset.LoadFromName':
            values = action.execute(fileset = self.name,
                                    conn = self.getReadDBConn(),
                                    transaction = self.existingTransaction())
            self.id, self.open, self.lastUpdate = values
        elif method == 'Fileset.LoadFromID':
            values = action.execute(fileset=self.id,
                                    conn = self.getReadDBConn(),
                                    transaction = self.existingTransaction())                                    
            self.name, self.open, self.lastUpdate = values
        else:
            raise TypeError, 'Chosen populate method not supported'
        
        self.newfiles = Set()
        self.files = Set()
        action = self.daofactory(classname='Files.InFileset')
        values = action.execute(fileset = self.name,
                                conn = self.getReadDBConn(),
                                transaction = self.existingTransaction())

        for v in values:
            file = File(id=v[0])
            file.load()
            self.files.add(file)

        return self
    
    def commit(self):
        """
        Add contents of self.newfiles to the database, 
        empty self.newfiles, reload self
        """
        self.beginTransaction()
        
        if not self.exists():
            self.create()
        lfns = []
        
        while len(self.newfiles) > 0:
            #Check file objects exist in the database, save those that don't
            f = self.newfiles.pop()
            if not f.exists():
                f.create()
            lfns.append(f["lfn"])

        #Add Files to DB only if there are any files on newfiles            
        if len(lfns) > 0:
            addAction = self.daofactory(classname='Files.AddToFileset')
            addAction.execute(file = lfns, fileset = self.name,
                              conn = self.getWriteDBConn(),
                              transaction = self.existingTransaction())
        self.load()
        self.commitIfNew()
        return
