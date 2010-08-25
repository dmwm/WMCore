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

__revision__ = "$Id: Fileset.py,v 1.40 2009/04/27 13:43:41 sfoulkes Exp $"
__version__ = "$Revision: 1.40 $"

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
        existsAction = self.daofactory(classname = "Fileset.Exists")
        return existsAction.execute(self.name, conn = self.getReadDBConn(),
                                    transaction = self.existingTransaction())
        
    def create(self):
        """
        Add the new fileset to WMBS, and commit the files
        """
        if self.exists() != False:
            self.load()
            return
        
        createAction = self.daofactory(classname = "Fileset.New")
        createAction.execute(self.name, self.open, conn = self.getWriteDBConn(),
                             transaction = self.existingTransaction())
        self.commit()
        self.load()
        self.commitIfNew()
        
        return
    
    def delete(self):
        """
        Remove this fileset from WMBS
        """
        action = self.daofactory(classname='Fileset.Delete')
        result = action.execute(name = self.name, conn = self.getWriteDBConn(),
                                transaction = self.existingTransaction())
        self.commitIfNew()
        return result
    
    def load(self): 
        """
        _load_

        Load the name, id and time that fileset was last updated in the
        database.
        """
        if self.id > 0:
            action = self.daofactory(classname = "Fileset.LoadFromID")
            result = action.execute(fileset = self.id,
                                    conn = self.getReadDBConn(),
                                    transaction = self.existingTransaction())                                    
        else:
            action = self.daofactory(classname = "Fileset.LoadFromName")
            result = action.execute(fileset = self.name,
                                    conn = self.getReadDBConn(),
                                    transaction = self.existingTransaction())

        self.id = result["id"]
        self.name = result["name"]
        self.open = result["open"]
        self.lastUpdate = result["last_update"]

        self.newfiles = Set()
        self.files = Set()
        return self

    def loadData(self): 
        """
        _loadData_

        Load all the files that belong to this fileset.   
        """
        if self.name == None or self.id < 0:
            self.load()

        action = self.daofactory(classname = "Files.InFileset")
        results = action.execute(fileset = self.id,
                                 conn = self.getReadDBConn(),
                                 transaction = self.existingTransaction())

        for result in results:
            file = File(id = result["fileid"])
            file.loadData(parentage = 1)
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
        ids = []
        
        while len(self.newfiles) > 0:
            #Check file objects exist in the database, save those that don't
            f = self.newfiles.pop()
            if not f.exists():
                f.create()
            ids.append(f["id"])
            self.files.add(f)

        #Add Files to DB only if there are any files on newfiles            
        if len(ids) > 0:
            addAction = self.daofactory(classname='Files.AddToFilesetByIDs')
            addAction.execute(file = ids, fileset = self.name,
                              conn = self.getWriteDBConn(),
                              transaction = self.existingTransaction())
        self.commitIfNew()
        return

    def markOpen(self, isOpen):
        """
        _markOpen_

        Change the open status of this fileset.  The isOpen parameter is a bool
        representing whether or not the fileset is open.
        """
        self.beginTransaction()

        closeAction = self.daofactory(classname = "Fileset.MarkOpen")
        closeAction.execute(fileset = self.name, isOpen = isOpen,
                            conn = self.getWriteDBConn(),
                            transaction = self.existingTransaction())

        self.commitIfNew()
