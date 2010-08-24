#!/usr/bin/env python
"""
_File_

A simple object representing a file in WMBS.
"""

__revision__ = "$Id: File.py,v 1.36 2009/01/08 21:58:27 sfoulkes Exp $"
__version__ = "$Revision: 1.36 $"

from sets import Set

from WMCore.DataStructs.File import File as WMFile
from WMCore.DataStructs.Run import Run

from WMCore.WMBS.WMBSBase import WMBSBase

class File(WMBSBase, WMFile):
    """
    A simple object representing a file in WMBS
    """
    #pylint: disable-msg=R0913
    def __init__(self, lfn='', id=-1, size=0, events=0, cksum=0,
                 parents=None, locations=None):
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
    
    def load(self, parentage=0):
        """
        use lfn to load file info from db
        """
        result = None 
        if self['id'] > 0:
            action = self.daofactory(classname='Files.GetByID')
            result = action.execute(self['id'], conn = self.getReadDBConn(),
                                    transaction = self.existingTransaction())
        else:
            action = self.daofactory(classname='Files.GetByLFN')
            result = action.execute(self['lfn'], conn = self.getReadDBConn(),
                                    transaction = self.existingTransaction())
            
        assert len(result) == 1, "Found %s files, not one" % len(result)

	result = result[0]
        self['id'] = result[0]
        self['lfn'] = result[1]
        self['size'] = result[2]
        self['events'] = result[3]
	self['cksum'] = result[4]
       
	#Get the Run/Lumis
	action = self.daofactory(classname='Files.GetRunLumiFile')
	runs = action.execute(self['lfn'], conn = self.getReadDBConn(), 
                              transaction = self.existingTransaction())
	[self.addRun(run=Run(r, *runs[r])) for r in runs.keys()]

        action = self.daofactory(classname='Files.GetLocation')
        self['locations'] = action.execute(self['lfn'], conn = self.getReadDBConn(),
                                           transaction = self.existingTransaction())
        self['newlocations'].clear()
        self['parents'] = Set()
        
        if parentage > 0:
            action = self.daofactory(classname='Files.GetParents')
            parentLFNs = action.execute(self['lfn'],  conn = self.getReadDBConn(),
                                        transaction = self.existingTransaction())
            for lfn in parentLFNs:
                f = File(lfn=lfn).load(parentage=parentage-1)
                self['parents'].add(f)
        return self
    
    def create(self):
        """
        _create_

        Create a file.  If no transaction is passed in this will wrap all
        statements in a single transaction.
        """
        if self.exists() != False:
            return

        addAction = self.daofactory(classname="Files.Add")
        addAction.execute(files = self["lfn"], size = self["size"],
                          events = self["events"], cksum= self["cksum"],
                          conn = self.getWriteDBConn(),
                          transaction = self.existingTransaction())

	if len(self["runs"]) > 0:
        	lumiAction = self.daofactory(classname="Files.AddRunLumi")
        	lumiAction.execute(file = self["lfn"], runs = self["runs"],
                                   conn = self.getWriteDBConn(),
                                   transaction = self.existingTransaction())
        
        # Add new locations if required
        self.updateLocations()
        self["id"] = self.exists()

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
