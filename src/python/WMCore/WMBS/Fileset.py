#!/usr/bin/env python
"""
_Fileset_

A simple object representing a Fileset in WMBS.

A fileset is a collection of files for processing. This could be a 
complete block, a block in transfer, some user defined dataset etc.

workflow + fileset = subscription

"""

__revision__ = "$Id: Fileset.py,v 1.4 2008/05/02 17:29:24 metson Exp $"
__version__ = "$Revision: 1.4 $"

from sets import Set
from sqlalchemy.exceptions import IntegrityError
from WMCore.WMBS.File import File
from WMCore.WMBS.Subscription import Subscription

class Fileset(object):
    """
    A simple object representing a Fileset in WMBS.

    A fileset is a collection of files for processing. This could be a 
    complete block, a block in transfer, some user defined dataset etc.
    
    workflow + fileset = subscription
    
    """
    def __init__(self, name, wmbs):
        """
        Create an empty fileset
        """
        self.name = name
        self.files = Set()
        self.newfiles = Set()
        self.wmbs = wmbs
    
    def exists(self):
        """
        Does a fileset exist with this name
        """
        result = -1
        for f in self.wmbs.filesetExists(self.name):
            for i in f.fetchall():
                result = i[0]
        if result > 0:
            return True
        else:
            return False
        
    def create(self):
        """
        Add the new fileset to WMBS
        """
        try:
            self.wmbs.insertFileset(self.name)
        except Exception, e:
            self.wmbs.logger.exception('Fileset %s exists' % self.name)
            raise e
    
    def delete(self):
        """
        Remove this fileset from WMBS
        """
        self.wmbs.logger.warning('you are removing the following fileset from WMBS %s %s'
                                 % (self.name))
        self.wmbs.deleteFileset(self.name)
    
    def populate(self):
        """
        Load up the files in the file set from the database
        """
        for f in self.wmbs.showFilesInFileset(self.name):
            for i in f.fetchall():
                id, lfn, size, events, run, lumi = i
                file = File(lfn, id, size, events, run, lumi)
                self.files.add(file)
                
    def addFile(self, file):
        """
        Add a file to the fileset
        """
        if file not in self.files:
            self.newfiles.add(file)
    
    def listFiles(self):
        """
        List all files in the fileset
        """
        l = list(self.files)
        l.extend(list(self.newfiles))
        return l
    
    def listNewFiles(self):  
        """
        List all files in the fileset that are new - e.g. not in the DB
        """       
        return self.newfiles
    
    def commit(self):
        """
        Commit changes to the fileset
        """
        comfiles = []
        for f in self.newfiles:
            comfiles.append(f.getInfo())
        self.wmbs.logger.debug ( "commiting : %s" % comfiles )  
        try:
            self.wmbs.insertFilesForFileset(files=comfiles, fileset=self.name)
        except IntegrityError:
            self.wmbs.logger.exception('File already exists in the database')
            for i in self.newfiles:
                print i.getInfo()
            raise IntegrityError, 'File already exists in the database'
        self.newfiles = Set()
        self.populate()
    
    def createSubscription(self, workflow=None, subtype='processing'):
        """
        Create a subscription for the fileset using the given workflow 
        """
        s = Subscription(fileset = self, workflow = workflow, 
                         type = subtype, wmbs = self.wmbs)
        s.create()
        return s
        
    def subscriptions(self, subtype="processing"):
        """
        Return all subscriptions for a fileset
        """
        subtype = subtype.lower()
        #TODO: types should come from DB
        if subtype in ("merge", "processing"):
            #TODO: change subscriptionsForFileset to return the workflow spec
            subscriptions = self.wmbs.subscriptionsForFileset(self.name, 
                                                              subtype)
            for i in subscriptions:
                print i.fetchall()
        else:
            self.wmbs.logger.exception('%s is an unknown subscription type' % 
                                       subtype)
            raise TypeError, '%s is an unknown subscription type' % subtype
        