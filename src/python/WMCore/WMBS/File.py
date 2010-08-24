#!/usr/bin/env python
"""
_File_

A simple object representing a file in WMBS.
"""

__revision__ = "$Id: File.py,v 1.43 2009/02/03 22:32:12 sryu Exp $"
__version__ = "$Revision: 1.43 $"

from sets import Set

from WMCore.DataStructs.File import File as WMFile
from WMCore.DataStructs.Run import Run

from WMCore.WMBS.WMBSBase import WMBSBase

class File(WMBSBase, WMFile):
    """
    A simple object representing a file in WMBS
    """
    #pylint: disable-msg=R0913
    def __init__(self, lfn = '', id = -1, size = 0, events = 0, cksum = 0,
                 parents = None, locations = None, first_event = 0,
                 last_event = 0):
        WMBSBase.__init__(self)
        WMFile.__init__(self, lfn=lfn, size=size, events=events, 
                        cksum=cksum, parents=parents)

        if locations == None:
            self.setdefault("newlocations", Set())
        else:
            if type(locations) == str:
                self.setdefault("newlocations", Set())
                self['newlocations'].add(locations)
            else:
                self.setdefault("newlocations", locations)

        self.setdefault("first_event", first_event)
        self.setdefault("last_event", last_event)
        self.setdefault("id", id)
        self['locations'] = Set()

    def exists(self):
        """
        Does a file exist with this lfn, return the id
        """
        action = self.daofactory(classname='Files.Exists')
        return action.execute(lfn = self['lfn'],
                              conn = self.getReadDBConn(),
                              transaction = self.existingTransaction())
        
    def getInfo(self):
        """
        Return the files attributes as a tuple
        """
        return self['lfn'], self['id'], self['size'], self['events'], \
               self['cksum'], list(self['runs']), list(self['locations']), \
               list(self['parents'])

    def getLocations(self):
        """
        Get a list of locations for this file
        """
        return list(self['locations'])

    def getRuns(self):
        """
	    Get a list of run lumi objects (List of Set() of type
        WMCore.DataStructs.Run)
	    """
        return list(self['runs'])
                                    
    def getParentLFNs(self):
        """
        Get a flat list of parent LFNs
        """
        result = []
        parents = self['parents']
        while parents:
            result.extend(parents)
            temp = []
            for parent in parents:
                temp.extend(parent["parents"])
            parents = temp
        result.sort()   # ensure SecondaryInputFiles are in order
        return [x['lfn'] for x in result]
    
    def getAncestors(self, level=2, type="id"):
        """
        Get ancestorLFNs. it will access directly DAO.
        level indicates the level of ancestors. default value is 2 
        (grand parents). level should be bigger than >= 1
        """
        def _getAncestorIDs(ids, level):
            action = self.daofactory(classname = "Files.GetParentIDsByID")
            parentIDs = action.execute(ids, conn = self.getReadDBConn(),
                                       transaction = self.existingTransaction())
            parentIDs.sort()
            if level == 1 or len(parentIDs) == 0:
                return parentIDs
            else:
                return _getAncestorIDs(parentIDs, level-1)
        
        if self['id'] < 0:
            self.load()
        idList = _getAncestorIDs(self['id'], level)
        
        if type == "id":
            return idList
        elif type == "lfn":
            ancestorLFNs = []
            for fileID in idList:
                anceFile = File(id=fileID)
                anceFile.load()
                ancestorLFNs.append(anceFile['lfn'])
                
            return ancestorLFNs
        elif type == "file":
            ancestors = []
            for fileID in idList:
                anceFile = File(id=fileID)
                anceFile.load()
                ancestors.append(anceFile)
                
            return ancestors

        return idList
    
    def getDescendants(self, level=2, type="id"):
        """
        Get descendants. it will access directly DAO.
        level indicates the level of ancestors. default value is 2 
        (grand parents). level should be bigger than >= 1
        """
        def _getDescendantIDs(ids, level):
            action = self.daofactory(classname = "Files.GetChildIDsByID")
            childIDs = action.execute(ids, conn = self.getReadDBConn(),
                                       transaction = self.existingTransaction())
            childIDs.sort()
            if level == 1 or len(childIDs) == 0:
                return childIDs
            else:
                return _getDescendantIDs(childIDs, level-1)
        
        if self['id'] < 0:
            self.load()
        idList = _getDescendantIDs(self['id'], level)
        
        if type == "id":
            return idList
        
        elif type == "lfn":
            descendantLFNs = []
            for fileID in idList:
                descFile = File(id=fileID)
                descFile.load()
                descendantLFNs.append(descFile['lfn'])
            return descendantLFNs
        
        elif type == "file":
            descendants = []
            for fileID in idList:
                descFile = File(id=fileID)
                descFile.load()
                descendants.append(descFile)
            return descendants
        
        return idList
    
    def load(self):
        """
        _load_

        Load any meta data that is associated with a file.  This currently
        includes id, lfn, size, events and cksum.
        """
        if self["id"] > 0:
            action = self.daofactory(classname = "Files.GetByID")
            result = action.execute(self["id"], conn = self.getReadDBConn(),
                                    transaction = self.existingTransaction())
        else:
            action = self.daofactory(classname = "Files.GetByLFN")
            result = action.execute(self["lfn"], conn = self.getReadDBConn(),
                                    transaction = self.existingTransaction())

        self.update(result)
        return self

    def loadData(self, parentage = 0):
        """
        _loadData_

        Load all information about a file.  This currently includes meta data,
        the run and lumi information, all the locations that where the file
        is stored and any parentage information.  The parentage parameter to
        this method will determine how many generations to load.
        """
        if self["id"] < 0 or self["lfn"] == "":
            self.load()
            
        action = self.daofactory(classname = "Files.GetRunLumiFile")
        runs = action.execute(self["lfn"], conn = self.getReadDBConn(), 
                              transaction = self.existingTransaction())
        [self.addRun(run=Run(r, *runs[r])) for r in runs.keys()]

        action = self.daofactory(classname = "Files.GetLocation")
        self["locations"] = action.execute(self["lfn"], conn = self.getReadDBConn(),
                                           transaction = self.existingTransaction())
        self["newlocations"].clear()
        self["parents"] = Set()
        
        if parentage > 0:
            action = self.daofactory(classname = "Files.GetParents")
            parentLFNs = action.execute(self["lfn"],  conn = self.getReadDBConn(),
                                        transaction = self.existingTransaction())
            for lfn in parentLFNs:
                f = File(lfn = lfn).load()
                f.loadData(parentage = parentage - 1)
                self["parents"].add(f)

        return

    def create(self):
        """
        _create_

        Create a file.  If no transaction is passed in this will wrap all
        statements in a single transaction.
        """
        if self.exists() != False:
            self.load()
            return

        addAction = self.daofactory(classname="Files.Add")
        addAction.execute(files = self["lfn"], size = self["size"],
                          events = self["events"], cksum = self["cksum"],
                          first_event = self["first_event"],
                          last_event = self["last_event"],
                          conn = self.getWriteDBConn(),
                          transaction = self.existingTransaction())

        if len(self["runs"]) > 0:
            lumiAction = self.daofactory(classname="Files.AddRunLumi")
            lumiAction.execute(file = self["lfn"], runs = self["runs"],
                                   conn = self.getWriteDBConn(),
                                   transaction = self.existingTransaction())
            
        self.updateLocations()
        self.load()
        self.commitIfNew()
        return
    
    def delete(self):
        """
        Remove a file from WMBS
        """
        self.daofactory(classname='Files.Delete').execute(file=self['lfn'],
                                                          conn = self.getWriteDBConn(),
                                                          transaction = self.existingTransaction())

        self.commitIfNew()
        return
        
    def addChild(self, lfn):
        """
        Set an existing file (lfn) as a child of this file
        """
        child = File(lfn=lfn)
        child.load()
        if not self['id'] > 0:
            raise Exception, "Parent file doesn't have an id %s" % self['lfn']
        if not child['id'] > 0:
            raise Exception, "Child file doesn't have an id %s" % child['lfn']

        heritageAction = self.daofactory(classname='Files.Heritage')
        heritageAction.execute(child=child['id'], parent=self['id'], conn = self.getWriteDBConn(),
                               transaction = self.existingTransaction())
        self.commitIfNew()
        return
        
    def addParent(self, lfn):
        """
        Set an existing file (lfn) as a parent of this file
        """
        parent = File(lfn=lfn)
        parent.load()
        self['parents'].add(parent)
        if not self['id'] > 0:
            raise Exception, "Child file doesn't have an id %s" % self['lfn']
        if not parent['id'] > 0:
            raise Exception, "Parent file doesn't have an id %s" % \
                        parent['lfn']
        
        action = self.daofactory(classname='Files.Heritage')
        action.execute(child=self['id'], parent=parent['id'], conn = self.getWriteDBConn(),
                       transaction = self.existingTransaction())
        self.commitIfNew()
        return
    
    def addRunSet(self, runSet):
        """
        add the set of runs.  This should be called after a file is created,
        unlike addRun which should be called before the file was created.
        runSet should be set of DataStruct.Run
        also there should be no duplicate entries in runSet.
        (May need to change in schema level not to allow duplicate record)
        """
        lumiAction = self.daofactory(classname="Files.AddRunLumi")
        lumiAction.execute(file = self["lfn"], runs = runSet,
                           conn = self.getWriteDBConn(),
                           transaction = self.existingTransaction())
        
        # update the self["runs"]
        self["runs"].clear()
        action = self.daofactory(classname = "Files.GetRunLumiFile")
        runs = action.execute(self["lfn"], conn = self.getReadDBConn(), 
                              transaction = self.existingTransaction())
        [self.addRun(run=Run(r, *runs[r])) for r in runs.keys()]

        self.commitIfNew()
        return
    
    def updateLocations(self):
        """
        _updateLocations_
        
        Write any new locations to the database.  After any new locations are
        written to the database all locations will be reloaded from the
        database.
        """
        # Add new locations if required
        if len(self["newlocations"]) > 0:
            addAction = self.daofactory(classname = "Files.SetLocation")
            addAction.execute(file = self["lfn"], location = self["newlocations"],
                              conn = self.getWriteDBConn(),
                              transaction = self.existingTransaction())

        # Update locations from the DB    
        getAction = self.daofactory(classname = "Files.GetLocation")
        self["locations"] = getAction.execute(self["lfn"], conn = self.getWriteDBConn(),
                                              transaction = self.existingTransaction())
        self["newlocations"].clear()

        self.commitIfNew()
        return
        
    def setLocation(self, se, immediateSave = True):
        """
        Sets the location of a file. If immediateSave is True, commit change to
        the DB immediately, otherwise queue for addition when save() is called.
        Also removes previous error where a file would have to be saved before
        locations could be added - confusing when file requires locations on its
        first creation (breaks transaction model in Fileset commits etc)
        """
        if isinstance(se, str):
            self['newlocations'].add(se)
            self['locations'].add(se)
        else:
            self['newlocations'].update(se)
            self['locations'].update(se)

        if immediateSave:
            self.updateLocations()
