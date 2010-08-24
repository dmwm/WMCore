#!/usr/bin/env python
"""
_File_

A simple object representing a file in WMBS

"""

__revision__ = "$Id: File.py,v 1.35 2009/01/05 23:00:50 sfoulkes Exp $"
__version__ = "$Revision: 1.35 $"

from WMCore.DataStructs.File import File as WMFile
from WMCore.Database.Transaction import Transaction
from WMCore.DAOFactory import DAOFactory
from sqlalchemy.exceptions import IntegrityError

from sets import Set

from WMCore.DataStructs.Run import Run

import threading

class File(WMFile):
    """
    A simple object representing a file in WMBS
    """
    #pylint: disable-msg=R0913
    def __init__(self, lfn='', id=-1, size=0, events=0, cksum=0,
                 parents=None, locations=None):
        WMFile.__init__(self, lfn=lfn, size=size, events=events, 
                        cksum=cksum, parents=parents)

        myThread = threading.currentThread()
        self.logger = myThread.logger
        self.dialect = myThread.dialect
        self.dbi = myThread.dbi
        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = self.logger,
                                     dbinterface = self.dbi)

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
        return action.execute(lfn = self['lfn'])
        
    def getInfo(self):
        """
        Return the files attributes as a tuple
        """
        return self['lfn'], self['id'], self['size'], self['events'], \
               self['cksum'], list(self['runs']), list(self['locations']), \
               list(self['parents'])


    def getLocations(self):
	"""
	get a list of locations for this file
	"""

	return list(self['locations'])

    def getRuns(self):
	"""
	get a list of run lumi objects (List of Set() of type WMCore.DataStructs.Run)
	"""
	return list(self['runs'])
                                    
    def getParentLFNs(self):
        """
        get a flat list of parent LFN's
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
            result = action.execute(self['id'])
        else:
            action = self.daofactory(classname='Files.GetByLFN')
            result = action.execute(self['lfn'])
        assert len(result) == 1, "Found %s files, not one" % len(result)

	result = result[0]
        self['id'] = result[0]
        self['lfn'] = result[1]
        self['size'] = result[2]
        self['events'] = result[3]
	self['cksum'] = result[4]
       
	#Get the Run/Lumis
	action = self.daofactory(classname='Files.GetRunLumiFile')
	runs = action.execute(self['lfn']) 	
	[self.addRun(run=Run(r, *runs[r])) for r in runs.keys()]

        action = self.daofactory(classname='Files.GetLocation')
        self['locations'] = action.execute(self['lfn']) 
        self['newlocations'].clear()
        
        self['parents'] = Set()
        
        if parentage > 0:
            action = self.daofactory(classname='Files.GetParents')
            for lfn in action.execute(self['lfn']):
                f = File(lfn=lfn).load(parentage=parentage-1)
                self['parents'].add(f)
        return self
    
    def create(self, trans = None):
        """
        _create_

        Create a file.  If no transaction is passed in this will wrap all
        statements in a single transaction.
        """
        if self.exists() != False:
            return

        myThread = threading.currentThread()
        if myThread.transaction == None:
            newtrans = True
            myThread.transaction = Transaction(self.dbi)
        else:
            newtrans = False

        addAction = self.daofactory(classname="Files.Add")
        addAction.execute(files = self["lfn"], size = self["size"],
                          events = self["events"], cksum= self["cksum"],
                          transaction = True)

	if len(self["runs"]) > 0:
        	lumiAction = self.daofactory(classname="Files.AddRunLumi")
        	lumiAction.execute(file = self["lfn"], runs = self["runs"],
                                   transaction = True)
        
        # Add new locations if required
        self.updateLocations()

        if newtrans:
            myThread.transaction.commit()
            myThread.transaction = None            
        
        self["id"] = self.exists()
        return
    
    def delete(self):
        """
        Remove a file from WMBS
        """
        myThread = threading.currentThread()
        if myThread.transaction == None:
            newtrans = True
            myThread.transaction = Transaction(self.dbi)
        else:
            newtrans = False
            
        self.daofactory(classname='Files.Delete').execute(file=self['lfn'],
                                                          transaction = True)

        if newtrans:
            myThread.transaction.commit()
            myThread.transaction = None            
        
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

        myThread = threading.currentThread()
        if myThread.transaction == None:
            newtrans = True
            myThread.transaction = Transaction(self.dbi)
        else:
            newtrans = False
            
        self.daofactory(classname='Files.Heritage').execute(
                                                        child=child['id'], 
                                                        parent=self['id'],
                                                        transaction = True)

        if newtrans:
            myThread.transaction.commit()
            myThread.transaction = None            
        
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

        myThread = threading.currentThread()
        if myThread.transaction == None:
            newtrans = True
            myThread.transaction = Transaction(self.dbi)
        else:
            newtrans = False
        
        action = self.daofactory(classname='Files.Heritage')
        action.execute(child=self['id'], parent=parent['id'], transaction = True)

        if newtrans:
            myThread.transaction.commit()
            myThread.transaction = None
    
    def updateLocations(self):
        """
        _updateLocations_
        
        Write any new locations to the database.  After any new locations are
        written to the database all locations will be reloaded from the
        database.
        """
        myThread = threading.currentThread()
        if myThread.transaction == None:
            newtrans = True
            myThread.transaction = Transaction(self.dbi)
        else:
            newtrans = False
        
        # Add new locations if required
        if len(self["newlocations"]) > 0:
            addAction = self.daofactory(classname = "Files.SetLocation")
            addAction.execute(file = self["lfn"], location = self["newlocations"],
                              transaction = True)

        # Update locations from the DB    
        getAction = self.daofactory(classname = "Files.GetLocation")
        self["locations"] = getAction.execute(self["lfn"], transaction = True)
        self["newlocations"].clear()

        if newtrans:
            myThread.transaction.commit()
            myThread.transaction = None            
            
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
