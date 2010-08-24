#!/usr/bin/env python
"""
_File_

A simple object representing a file in WMBS

"""

__revision__ = "$Id: File.py,v 1.19 2008/09/01 16:11:31 metson Exp $"
__version__ = "$Revision: 1.19 $"

from WMCore.WMBS.BusinessObject import BusinessObject
from WMCore.DataStructs.File import File as WMFile
from sqlalchemy.exceptions import IntegrityError

from sets import Set

class File(BusinessObject, WMFile):
    """
    A simple object representing a file in WMBS
    """
    def __init__(self, lfn='', id=-1, size=0, events=0, run=0, lumi=0,
                 parents=Set(), locations=Set(), logger=None, dbfactory=None):
        BusinessObject.__init__(self, logger=logger, dbfactory=dbfactory)
        WMFile.__init__(self, lfn=lfn, size=size, events=events, run=run, lumi=lumi, parents=parents)
        """
        Create the file object
        """
        self.dict["id"] = int(id)
        self.dict["locations"] = locations
        
    def getInfo(self):
        """
        Return the files attributes as a tuple
        """
        return self.dict['lfn'], self.dict['id'], self.dict['size'], self.dict['events'], self.dict['run'], \
                                    self.dict['lumi'], list(self.dict['locations']), list(self.dict['parents'])
                                    
    def getParentLFNs(self):
        """
        get a flat list of parent LFN's
        """
        result = []
        parents = self.dict['parents']
        while parents:
            result.extend(parents)
            temp = []
            for parent in parents:
                temp.extend(parent.dict["parents"])
            parents = temp
        result.sort()   # ensure SecondaryInputFiles are in order
        return [x.dict['lfn'] for x in result]
    
    def load(self, parentage=0):
        """
        use lfn to load file info from db
        """
        result = None 
        if self.dict['id'] > 0:
            result = self.daofactory(classname='Files.GetByID').execute(self.dict['id'])
        else:
            result = self.daofactory(classname='Files.GetByLFN').execute(self.dict['lfn'])
        self.dict['id'] = result[0]
        self.dict['lfn'] = result[1]
        self.dict['size'] = result[2]
        self.dict['events'] = result[3]
        self.dict['run'] = result[4]
        self.dict['lumi'] = result[5]
        
        self.dict['locations'] = self.daofactory(classname='Files.GetLocation').execute(self.dict['lfn']) 
        
        self.dict['parents'] = Set()
        
        if parentage > 0:
            for lfn in self.daofactory(classname='Files.GetParents').execute(self.dict['lfn']):
                self.dict['parents'].add( \
                        File(lfn=lfn, logger=self.logger, dbfactory=self.dbfactory).load(parentage=parentage-1))
        
        return self
    
    def save(self):
        """
        Save a file to the database 
        """
        try:
            self.daofactory(classname='Files.Add').execute(files=self.dict['lfn'], 
                                                       size=self.dict['size'], 
                                                       events=self.dict['events'])
        except IntegrityError, e:
            self.logger.exception('File %s exists' % (self.dict['lfn']))
        except Exception, e:
            raise e
        try:
            self.daofactory(classname='Files.AddRunLumi').execute(files=self.dict['lfn'],  
                                                       run=self.dict['run'], 
                                                       lumi=self.dict['lumi'])
        except IntegrityError, e:
            pass #Ignore that the file exists
        except Exception, e:
            raise e
        
        self.load()
    
    def delete(self):
        """
        Remove a file from WMBS
        """
        self.daofactory(classname='Files.Delete').execute(file=self.dict['lfn'])
        
    def addChild(self, lfn):
        """
        Set an existing file (lfn) as a child of this file
        """
        child = File(lfn=lfn, logger=self.logger, dbfactory=self.dbfactory)
        child.load()
        if not self.dict['id'] > 0:
            raise Exception, "Parent file doesn't have an id %s" % self.dict['lfn']
        if not child.dict['id'] > 0:
            raise Exception, "Child file doesn't have an id %s" % child.lfn
        
        self.daofactory(classname='Files.Heritage').execute(child=child.dict['id'], parent=self.dict['id'])
        
    def addParent(self, lfn):
        """
        Set an existing file (lfn) as a parent of this file
        """
        parent = File(lfn=lfn, logger=self.logger, dbfactory=self.dbfactory)
        parent.load()
        self.dict['parents'].add(parent)
        if not self.dict['id'] > 0:
            raise Exception, "Child file doesn't have an id %s" % self.dict['lfn']
        if not parent.dict['id'] > 0:
            raise Exception, "Parent file doesn't have an id %s" % parent.dict['lfn']
        
        self.daofactory(classname='Files.Heritage').execute(child=self.dict['id'], parent=parent.dict['id'])

    def setLocation(self, se):
        self.daofactory(classname='Files.SetLocation').execute(file=self.dict['lfn'], sename=se)
        self.dict['locations'] = self.daofactory(classname='Files.GetLocation').execute(self.dict['lfn']) 
