#!/usr/bin/env python
"""
_File_

A simple object representing a file in WMBS.
"""

__revision__ = "$Id: File.py,v 1.47 2009/05/08 16:04:10 sfoulkes Exp $"
__version__ = "$Revision: 1.47 $"

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
        if id is exist (not -1) check with id first
        Does a file exist with this lfn, return the id
        """
        if self["id"] != -1:
            action = self.daofactory(classname = "Files.ExistsByID")
            result = action.execute(id = self["id"], conn = self.getDBConn(),
                                    transaction = self.existingTransaction())
        else:
            action = self.daofactory(classname = "Files.Exists")
            result = action.execute(lfn = self["lfn"], conn = self.getDBConn(),
                                    transaction = self.existingTransaction())
            if result != False:
                self["id"] = result

        return result
        
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
    
    def getAncestors(self, level = 2, type = "id"):
        """
        Get ancestorLFNs. it will access directly DAO.
        level indicates the level of ancestors. default value is 2 
        (grand parents). level should be bigger than >= 1
        """
        existingTransaction = self.beginTransaction()

        def _getAncestorIDs(ids, level):
            action = self.daofactory(classname = "Files.GetParentIDsByID")
            parentIDs = action.execute(ids, conn = self.getDBConn(),
                                       transaction = self.existingTransaction())

            parentIDs.sort()
            if level == 1 or len(parentIDs) == 0:
                return parentIDs
            else:
                return _getAncestorIDs(parentIDs, level - 1)
        
        if self["id"] < 0:
            self.load()

        idList = _getAncestorIDs(self["id"], level)
        
        if type == "id":
            results = idList
        elif type == "lfn":
            results = []
            for fileID in idList:
                anceFile = File(id=fileID)
                anceFile.load()
                results.append(anceFile["lfn"])
        elif type == "file":
            results = []
            for fileID in idList:
                anceFile = File(id=fileID)
                anceFile.load()
                results.append(anceFile)
                
        self.commitTransaction(existingTransaction)
        return results
    
    def getDescendants(self, level=2, type="id"):
        """
        Get descendants. it will access directly DAO.
        level indicates the level of ancestors. default value is 2 
        (grand parents). level should be bigger than >= 1
        """
        existingTransaction = self.existingTransaction()

        def _getDescendantIDs(ids, level):
            action = self.daofactory(classname = "Files.GetChildIDsByID")
            childIDs = action.execute(ids, conn = self.getDBConn(),
                                       transaction = self.existingTransaction())

            childIDs.sort()
            if level == 1 or len(childIDs) == 0:
                return childIDs
            else:
                return _getDescendantIDs(childIDs, level - 1)
        
        if self["id"] < 0:
            self.load()

        idList = _getDescendantIDs(self["id"], level)
        
        if type == "id":
            results = idList
        elif type == "lfn":
            results = []
            for fileID in idList:
                descFile = File(id=fileID)
                descFile.load()
                results.append(descFile['lfn'])
        elif type == "file":
            results = []
            for fileID in idList:
                descFile = File(id=fileID)
                descFile.load()
                results.append(descFile)
        
        self.commitTransaction(existingTransaction)
        return results
    
    def load(self):
        """
        _load_

        Load any meta data that is associated with a file.  This currently
        includes id, lfn, size, events and cksum.
        """
        if self["id"] > 0:
            action = self.daofactory(classname = "Files.GetByID")
            result = action.execute(self["id"], conn = self.getDBConn(),
                                    transaction = self.existingTransaction())
        else:
            action = self.daofactory(classname = "Files.GetByLFN")
            result = action.execute(self["lfn"], conn = self.getDBConn(),
                                    transaction = self.existingTransaction())

        self.update(result)
        return

    def loadData(self, parentage = 0):
        """
        _loadData_

        Load all information about a file.  This currently includes meta data,
        the run and lumi information, all the locations that where the file
        is stored and any parentage information.  The parentage parameter to
        this method will determine how many generations to load.
        """
        existingTransaction = self.beginTransaction()

        if self["id"] < 0 or self["lfn"] == "":
            self.load()
            
        action = self.daofactory(classname = "Files.GetRunLumiFile")
        runs = action.execute(self["lfn"], conn = self.getDBConn(), 
                              transaction = self.existingTransaction())

        [self.addRun(run = Run(r, *runs[r])) for r in runs.keys()]

        action = self.daofactory(classname = "Files.GetLocation")
        self["locations"] = action.execute(self["lfn"], conn = self.getDBConn(),
                                           transaction = self.existingTransaction())
        self["newlocations"].clear()
        self["parents"] = Set()
        
        if parentage > 0:
            action = self.daofactory(classname = "Files.GetParents")
            parentLFNs = action.execute(self["lfn"],  conn = self.getDBConn(),
                                        transaction = self.existingTransaction())

            for lfn in parentLFNs:
                f = File(lfn = lfn)
                f.load()
                f.loadData(parentage = parentage - 1)
                self["parents"].add(f)

        self.commitTransaction(existingTransaction)
        return

    def create(self):
        """
        _create_

        Create a file.  If no transaction is passed in this will wrap all
        statements in a single transaction.
        """
        existingTransaction = self.beginTransaction()

        if self.exists() != False:
            self.load()
            return

        addAction = self.daofactory(classname = "Files.Add")
        addAction.execute(files = self["lfn"], size = self["size"],
                          events = self["events"], cksum = self["cksum"],
                          first_event = self["first_event"],
                          last_event = self["last_event"],
                          conn = self.getDBConn(),
                          transaction = self.existingTransaction())

        if len(self["runs"]) > 0:
            lumiAction = self.daofactory(classname="Files.AddRunLumi")
            lumiAction.execute(file = self["lfn"], runs = self["runs"],
                                   conn = self.getDBConn(),
                                   transaction = self.existingTransaction())
            
        self.updateLocations()
        self.load()
        self.commitTransaction(existingTransaction)
        return
    
    def delete(self):
        """
        Remove a file from WMBS
        """
        action = self.daofactory(classname = "Files.Delete")
        action.execute(file =self["lfn"], conn = self.getDBConn(),
                       transaction = self.existingTransaction())
        return
        
    def addChild(self, lfn):
        """
        Set an existing file (lfn) as a child of this file
        """
        existingTransaction = self.beginTransaction()

        child = File(lfn = lfn)
        child.load()

        if not self['id'] > 0:
            raise Exception, "Parent file doesn't have an id %s" % self['lfn']
        if not child['id'] > 0:
            raise Exception, "Child file doesn't have an id %s" % child['lfn']

        heritageAction = self.daofactory(classname = "Files.Heritage")
        heritageAction.execute(child = child["id"], parent = self["id"], 
                               conn = self.getDBConn(),
                               transaction = self.existingTransaction())

        self.commitTransaction(existingTransaction)
        return
        
    def addParent(self, lfn):
        """
        Set an existing file (lfn) as a parent of this file
        """
        existingTransaction = self.beginTransaction()

        parent = File(lfn = lfn)
        parent.load()
        self["parents"].add(parent)

        if not self["id"] > 0:
            raise Exception, "Child file doesn't have an id %s" % self["lfn"]
        if not parent["id"] > 0:
            raise Exception, "Parent file doesn't have an id %s" % \
                        parent["lfn"]
        
        action = self.daofactory(classname = "Files.Heritage")
        action.execute(child = self["id"], parent = parent["id"],
                       conn = self.getDBConn(),
                       transaction = self.existingTransaction())

        self.commitTransaction(existingTransaction)
        return
    
    def addRunSet(self, runSet):
        """
        add the set of runs.  This should be called after a file is created,
        unlike addRun which should be called before the file was created.
        runSet should be set of DataStruct.Run
        also there should be no duplicate entries in runSet.
        (May need to change in schema level not to allow duplicate record)
        """
        existingTransaction = self.beginTransaction()

        lumiAction = self.daofactory(classname = "Files.AddRunLumi")
        lumiAction.execute(file = self["lfn"], runs = runSet,
                           conn = self.getDBConn(),
                           transaction = self.existingTransaction())
        
        action = self.daofactory(classname = "Files.GetRunLumiFile")
        runs = action.execute(self["lfn"], conn = self.getDBConn(), 
                              transaction = self.existingTransaction())

        self["runs"].clear()
        [self.addRun(run=Run(r, *runs[r])) for r in runs.keys()]

        self.commitTransaction(existingTransaction)
        return
    
    def updateLocations(self):
        """
        _updateLocations_
        
        Write any new locations to the database.  After any new locations are
        written to the database all locations will be reloaded from the
        database.
        """
        existingTransaction = self.beginTransaction()

        # Add new locations if required
        if len(self["newlocations"]) > 0:
            addAction = self.daofactory(classname = "Files.SetLocation")
            addAction.execute(file = self["lfn"], location = self["newlocations"],
                              conn = self.getDBConn(),
                              transaction = self.existingTransaction())

        # Update locations from the DB    
        getAction = self.daofactory(classname = "Files.GetLocation")
        self["locations"] = getAction.execute(self["lfn"], conn = self.getDBConn(),
                                              transaction = self.existingTransaction())
        self["newlocations"].clear()

        self.commitTransaction(existingTransaction)
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

        return
