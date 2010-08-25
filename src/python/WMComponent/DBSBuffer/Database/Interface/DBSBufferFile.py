#!/usr/bin/env python
"""
_DBSBufferFile_

A simple object representing a file in WMBS

"""

__revision__ = "$Id: DBSBufferFile.py,v 1.1 2009/05/08 11:06:22 afaq Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.DataStructs.File import File as WMFile
from WMCore.Database.Transaction import Transaction
from WMCore.DAOFactory import DAOFactory
from sqlalchemy.exceptions import IntegrityError

from sets import Set

from WMCore.DataStructs.Run import Run

import threading

class DBSBufferFile(WMFile):
    """
    A simple object representing a file in WMBS
    """
    #pylint: disable-msg=R0913
    def __init__(self, lfn='', id=-1, size=0, events=0, cksum=0,
		 dataset=0, status=0,
                 parents=None, locations=None):
        WMFile.__init__(self, lfn=lfn, size=size, events=events, 
                        cksum=cksum, parents=parents)
	self.setdefault("dataset", dataset)
	self.setdefault("status", status)

        myThread = threading.currentThread()
        self.logger = myThread.logger
        self.dialect = myThread.dialect
        self.dbi = myThread.dbi
        self.daofactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                     logger = self.logger,
                                     dbinterface = self.dbi)
        # Create the file object
        if locations == None:
            self.setdefault("newlocations", Set())
        else:
            self.setdefault("newlocations", locations)
            
        self.setdefault("id", id)
        self['locations'] = Set()

    def exists(self):
        """
        Does a file exist with this lfn, return the id
        """
        action = self.daofactory(classname='DBSBufferFiles.Exists')
        return action.execute(lfn = self['lfn'])
        
    def getInfo(self):
        """
        Return the files attributes as a tuple
        """
        return self['lfn'], self['id'], self['size'], self['events'], \
               self['cksum'], self['dataset'], self['status'], list(self['runs']), list(self['locations']), \
               list(self['parents'])

    def getDataset(self):
	"""
	Returns the dataset this File belongs to
	"""
	return self['dataset']

    def getStatus(self):
	"""
	Returns the status of this file (UPLOADED etc.)
	"""
	return self['status']

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
            action = self.daofactory(classname='DBSBufferFiles.GetByID')
            result = action.execute(self['id'])
        else:
            action = self.daofactory(classname='DBSBufferFiles.GetByLFN')
            result = action.execute(self['lfn'])
        assert len(result) == 1, "Found %s files, not one" % len(result)

	result = result[0]
        self['id'] = result[0]
        self['lfn'] = result[1]
        self['size'] = result[2]
        self['events'] = result[3]
	self['cksum'] = result[4]
	self['dataset'] = result[4]
	self['status'] = result[4]
       
	#Get the Run/Lumis
	action = self.daofactory(classname='DBSBufferFiles.GetRunLumiFile')
	runs = action.execute(self['lfn']) 	
	[self.addRun(run=Run(r, *runs[r])) for r in runs.keys()]

        action = self.daofactory(classname='DBSBufferFiles.GetLocation')
        self['locations'] = action.execute(self['lfn']) 
        self['newlocations'].clear()
        
        self['parents'] = Set()
        
        if parentage > 0:
            action = self.daofactory(classname='DBSBufferFiles.GetParents')
            for lfn in action.execute(self['lfn']):
                f = DBSBufferFile(lfn=lfn).load(parentage=parentage-1)
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

        if trans == None:
            newtrans = True            
            trans = Transaction(self.dbi)
        else:
            newtrans = False

        conn = trans.conn

        addAction = self.daofactory(classname="DBSBufferFiles.Add")
        addAction.execute(files = self["lfn"], size = self["size"],
                          events = self["events"], cksum= self["cksum"], dataset=self["dataset"],
			  conn = conn,
                          transaction = True)

	if len(self["runs"]) > 0:
        	lumiAction = self.daofactory(classname="DBSBufferFiles.AddRunLumi")
        	lumiAction.execute(file = self["lfn"], runs = self["runs"],
                           conn = conn, transaction = True)
        
        # Add new locations if required
        self.updateLocations(trans)

        if newtrans:
            trans.commit()
        
        self["id"] = self.exists()
        return
    
    def delete(self):
        """
        Remove a file from WMBS
        """
        self.daofactory(classname='DBSBufferFiles.Delete').execute(file=self['lfn'])
        
    def addChild(self, lfn):
        """
        Set an existing file (lfn) as a child of this file
        """
        child = DBSBufferFile(lfn=lfn)
        child.load()
        if not self['id'] > 0:
            raise Exception, "Parent file doesn't have an id %s" % self['lfn']
        if not child['id'] > 0:
            raise Exception, "Child file doesn't have an id %s" % child['lfn']
        
        self.daofactory(classname='DBSBufferFiles.Heritage').execute(
                                                        child=child['id'], 
                                                        parent=self['id'])
        
    def addParent(self, lfn):
        """
        Set an existing file (lfn) as a parent of this file
        """

        parent = DBSBufferFile(lfn=lfn)
        parent.load()
        self['parents'].add(parent)
        if not self['id'] > 0:
            raise Exception, "Child file doesn't have an id %s" % self['lfn']
        if not parent['id'] > 0:
            raise Exception, "Parent file doesn't have an id %s" % \
                        parent['lfn']
        
        action = self.daofactory(classname='DBSBufferFiles.Heritage')

        action.execute(child=self['id'], parent=parent['id'])
    
    def updateLocations(self, trans = None):
        """
        _updateLocations_
        
        Write any new locations to the database.  After any new locations are
        written to the database all locations will be reloaded from the
        database.
        """
        if trans == None:
            trans = Transaction(self.dbi)
            newtrans = True
        else:
            newtrans = False

        conn = trans.conn
            
        # Add new locations if required
        if len(self["newlocations"]) > 0:
	    #Lets first add the location, if its not already in database (duplicates will be avoided automatically)

	    insertAction = self.daofactory(classname = "DBSBufferFiles.AddLocation")
	    insertAction.execute(location = self["newlocations"], conn = conn, transaction = True)
            #
            addAction = self.daofactory(classname = "DBSBufferFiles.SetLocation")
            addAction.execute(file = self["lfn"], location = self["newlocations"],
                              conn = conn, transaction = True)

        # Update locations from the DB    
        getAction = self.daofactory(classname = "DBSBufferFiles.GetLocation")
        self["locations"] = getAction.execute(self["lfn"], conn = conn,
                                              transaction = True)
        self["newlocations"].clear()

        if newtrans:
            trans.commit()
            
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
