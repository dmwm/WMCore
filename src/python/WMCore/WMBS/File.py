#!/usr/bin/env python
"""
_File_

A simple object representing a file in WMBS

"""

__revision__ = "$Id: File.py,v 1.25 2008/10/22 19:09:51 sfoulkes Exp $"
__version__ = "$Revision: 1.25 $"

from WMCore.WMBS.BusinessObject import BusinessObject
from WMCore.DataStructs.File import File as WMFile
from WMCore.Database.Transaction import Transaction
from sqlalchemy.exceptions import IntegrityError

from sets import Set

class File(BusinessObject, WMFile):
    """
    A simple object representing a file in WMBS
    """
    #pylint: disable-msg=R0913
    def __init__(self, lfn='', id=-1, size=0, events=0, run=0, lumi=0,
                 parents=Set(), locations=None, logger=None, dbfactory=None):
        BusinessObject.__init__(self, logger=logger, dbfactory=dbfactory)
        WMFile.__init__(self, lfn=lfn, id=id, size=size, events=events, run=run,
                        lumi=lumi, parents=parents)
        # Create the file object
        if locations != None:
            self.setdefault("locations", locations)
        self.dict = self

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
                temp.extend(parent.dict["parents"])
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
        
        self['parents'] = Set()
        
        if parentage > 0:
            action = self.daofactory(classname='Files.GetParents')
            for lfn in action.execute(self['lfn']):
                f = File(lfn=lfn, 
                    logger=self.logger, 
                    dbfactory=self.dbfactory).load(parentage=parentage-1)
                self['parents'].add(f)
        self.dict = self
        return self
    
    def save(self):
        """
        Save a file to the database 
        """
        trans = Transaction(dbinterface = self.dbfactory.connect())
        try:
            try:
                self.daofactory(classname='Files.Add').execute(
                                                       files=self['lfn'], 
                                                       size=self['size'], 
                                                       events=self['events'],
                                                       conn = trans.conn, 
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
                                                       conn = trans.conn, 
                                                       transaction = True)
            except IntegrityError, e:
                pass #Ignore that the file exists
            except Exception, e:
                raise e
            trans.commit()
        except Exception, e:
            trans.rollback()
            raise e
    
    def delete(self):
        """
        Remove a file from WMBS
        """
        self.daofactory(classname='Files.Delete').execute(file=self['lfn'])
        
    def addChild(self, lfn):
        """
        Set an existing file (lfn) as a child of this file
        """
        child = File(lfn=lfn, logger=self.logger, dbfactory=self.dbfactory)
        child.load()
        if not self['id'] > 0:
            raise Exception, "Parent file doesn't have an id %s" % self['lfn']
        if not child.dict['id'] > 0:
            raise Exception, "Child file doesn't have an id %s" % child['lfn']
        
        self.daofactory(classname='Files.Heritage').execute(
                                                        child=child.dict['id'], 
                                                        parent=self['id'])
        
    def addParent(self, lfn):
        """
        Set an existing file (lfn) as a parent of this file
        """
        parent = File(lfn=lfn, logger=self.logger, dbfactory=self.dbfactory)
        parent.load()
        self['parents'].add(parent)
        if not self['id'] > 0:
            raise Exception, "Child file doesn't have an id %s" % self['lfn']
        if not parent.dict['id'] > 0:
            raise Exception, "Parent file doesn't have an id %s" % \
                        parent.dict['lfn']
        
        action = self.daofactory(classname='Files.Heritage')
        action.execute(child=self['id'], parent=parent.dict['id'])
        self.dict = self
        
    def setLocation(self, se):
        self.daofactory(classname='Files.SetLocation').execute(file=self['lfn'],
                                                               sename=se)
        action = self.daofactory(classname='Files.GetLocation')
        self['locations'] = action.execute(self['lfn']) 
        self.dict = self
