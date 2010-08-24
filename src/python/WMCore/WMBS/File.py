#!/usr/bin/env python
"""
_File_

A simple object representing a file in WMBS

"""

__revision__ = "$Id: File.py,v 1.7 2008/06/16 16:04:32 metson Exp $"
__version__ = "$Revision: 1.7 $"
from WMCore.DAOFactory import DAOFactory


class File(object):
    """
    A simple object representing a file in WMBS
    """
    def __init__(self, lfn='', id=-1, size=0, events=0, run=0, lumi=0,
                 parents=set(), locations=set(), logger=None, dbfactory=None):
        """
        Create the file object
        """
        self.id = id
        self.lfn = lfn
        self.size = size 
        self.events = events
        self.run = run
        self.lumi = lumi
        self.parents = parents
        self.locations = locations
        self.dbfactory = dbfactory
        self.logger = logger
        self.daofactory = DAOFactory(package='WMCore.WMBS', 
                                     logger=self.logger, 
                                     dbinterface=self.dbfactory.connect())
        
    def getInfo(self):
        """
        Return the files attributes as a tuple
        """
        return self.lfn, self.id, self.size, self.events, self.run, \
                                    self.lumi, list(self.locations), list(self.parents)
                                    
    def getParentLFNs(self):
        """
        get a flat list of parent LFN's
        """
        result = []
        parents = self.parents
        while parents:
            result.extend([x.lfn for x in parents])
            temp = []
            for parent in parents:
                temp.extend(parent.parents)
            parents = temp
        return result
    
    def load(self, parentage=0):
        """
        use lfn to load file info from db
        """
        result = self.daofactory(classname='Files.Get').execute(self.lfn)
        self.id = result[0]
        self.lfn = result[1]
        self.size = result[2]
        self.events = result[3]
        self.run = result[4]
        self.lumi = result[5]
        
        self.locations = self.daofactory(classname='Files.GetLocation').execute(self.lfn) 
        
        self.parents = set()
        
        if not parentage > 0:
            return self

        for lfn in self.daofactory(classname='Files.GetParents').execute():
            self.parents.add( \
                    File(lfn=lfn, logger=self.logger, dbfactory=self.dbfactory).load(parentage=parentage-1))
        
        return self
    
    def save(self):
        """
        Save a file to the database 
        """
        self.daofactory(classname='Files.Add').execute(files=self.lfn, 
                                                       size=self.size, 
                                                       events=self.events, 
                                                       run=self.run, 
                                                       lumi=self.lumi)
        self.load()
    
    def delete(self):
        """
        Remove a file from WMBS
        """
        self.daofactory(classname='Files.Delete').execute(files=self.lfn)
        
    def addChild(self, lfn):
        """
        Set an existing file (lfn) as a child of this file
        """
        child = File(lfn=lfn, logger=self.logger, dbfactory=self.dbfactory).load(parentage=parentage-1)
        child.load()
        if not self.id > 0:
            raise Exception, "Parent file doesn't have an id %s" % self.lfn
        if not child.id > 0:
            raise Exception, "Child file doesn't have an id %s" % child.lfn
        
        self.daofactory(classname='Files.Heritage').execute(child=child.id, parent=self.id)
        
    def addParent(self, lfn):
        """
        Set an existing file (lfn) as a parent of this file
        """
        parent = File(lfn=lfn, logger=self.logger, dbfactory=self.dbfactory)
        parent.load()
        self.parents.add(parent)
        if not self.id > 0:
            raise Exception, "Child file doesn't have an id %s" % self.lfn
        if not parent.id > 0:
            raise Exception, "Parent file doesn't have an id %s" % parent.lfn
        
        self.daofactory(classname='Files.Heritage').execute(child=self.id, parent=parent.id)

    def setLocation(self, se):
        self.daofactory(classname='Files.SetLocation').execute(file=self.lfn, sename=se)
        self.locations = self.daofactory(classname='Files.GetLocation').execute(self.lfn) 
        
    def __cmp__(self, rhs):
        """
        Sort files in run number and lumi section order
        """
        if self.run == rhs.run:
            return cmp(self.lumi, rhs.lumi)
        return cmp(self.run, rhs.run)
    
    def __eq__(self, rhs):
        """
        File is equal if it has the same name, size, runs events and lumi
        """
        eq = self.lfn == rhs.lfn 
        eq = eq and self.size == rhs.size 
        eq = eq and self.events == rhs.events
        eq = eq and self.run == rhs.run
        eq = eq and self.lumi == rhs.lumi
        return eq
    
    def __ne__(self, rhs):
        return not self.__eq__(rhs)
