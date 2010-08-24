#!/usr/bin/env python
"""
_File_

A simple object representing a file in WMBS

"""

__revision__ = "$Id: File.py,v 1.30 2008/11/25 17:23:22 sfoulkes Exp $"
__version__ = "$Revision: 1.30 $"

from WMCore.DataStructs.File import File as WMFile
from WMCore.Database.Transaction import Transaction
from WMCore.DAOFactory import DAOFactory
from sqlalchemy.exceptions import IntegrityError

from sets import Set
import threading

class File(WMFile):
    """
    A simple object representing a file in WMBS
    """
    #pylint: disable-msg=R0913
    def __init__(self, lfn='', id=-1, size=0, events=0, run=0, lumi=0,
                 parents=None, locations=None):
        WMFile.__init__(self, lfn=lfn, size=size, events=events, run=run,
                        lumi=lumi, parents=parents)

        myThread = threading.currentThread()
        self.logger = myThread.logger
        self.dialect = myThread.dialect
        self.dbi = myThread.dbi
        self.daofactory = DAOFactory(package = "WMCore.WMBS",
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
        action = self.daofactory(classname='Files.Exists')
        return action.execute(lfn = self['lfn'])
        
    def getInfo(self):
        """
        Return the files attributes as a tuple
        """
        return self['lfn'], self['id'], self['size'], self['events'], \
               self['run'], self['lumi'], list(self['locations']), \
               list(self['parents'])
                                    
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
        self['run'] = result[4]
        self['lumi'] = result[5]
        
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
        Create a file.
        """
        if not trans:
            trans = Transaction(self.dbi)
            conn = trans.conn
            newtrans = True
        else:
            newtrans = False
            conn = None
        try:
            try:
                self.daofactory(classname='Files.Add').execute(
                                                       files=self['lfn'], 
                                                       size=self['size'], 
                                                       events=self['events'],
                                                       conn = conn,
                                                       transaction = True)
            except IntegrityError, e:
                self.logger.exception('File %s exists' % (self['lfn']))
            except Exception, e:
                raise e
            try:
                self.daofactory(classname='Files.AddRunLumi').execute(
                                                       files=self['lfn'],  
                                                       run=self['run'], 
                                                       lumi=self['lumi'],
                                                       conn = conn,
                                                       transaction = True)
            except IntegrityError, e:
                pass #Ignore that the file exists
            except Exception, e:
                raise e
            
            # Add new locations if required
            self.updateLocations(trans)
        
            if newtrans:
                trans.commit()
        except Exception, e:
            # Only roll back transaction if it was created in this scope
            if newtrans:
                trans.rollback()
            raise e

        self["id"] = self.exists()
        return
    
    def delete(self):
        """
        Remove a file from WMBS
        """
        self.daofactory(classname='Files.Delete').execute(file=self['lfn'])
        
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
        
        self.daofactory(classname='Files.Heritage').execute(
                                                        child=child['id'], 
                                                        parent=self['id'])
        
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
        action.execute(child=self['id'], parent=parent['id'])
    
    def updateLocations(self, trans = None):
        """
        Saves any new locations, and refreshes location list from DB
        """
        if not trans:
            trans = Transaction(self.dbi)
            conn = trans.conn
            newtrans = True
        else:
            newtrans = False
            conn = None
            
        try:
            # Add new locations if required
            if len(self['newlocations']) > 0:
                self.daofactory(classname='Files.SetLocation').execute(
                    file=self['lfn'],
                    sename=self['newlocations'],
                    conn = conn,
                    transaction = True)
            
                # Update locations from the DB    
                action = self.daofactory(classname='Files.GetLocation')
                self['locations'] = action.execute(self['lfn'], conn = conn,
                                                   transaction = True)
                self['newlocations'].clear()
        
                if newtrans:
                    trans.commit()
        except Exception, e:
            # Only roll back transaction if it was created in this scope
            if newtrans:
                trans.rollback()
            raise e
        
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
