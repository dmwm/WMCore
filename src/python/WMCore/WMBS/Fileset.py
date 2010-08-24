#!/usr/bin/env python
"""
_Fileset_

A simple object representing a Fileset in WMBS.

A fileset is a collection of files for processing. This could be a 
complete block, a block in transfer, some user defined dataset etc.

workflow + fileset = subscription

"""

__revision__ = "$Id: Fileset.py,v 1.13 2008/06/24 17:00:18 metson Exp $"
__version__ = "$Revision: 1.13 $"

from sets import Set
from sqlalchemy.exceptions import IntegrityError

from WMCore.WMBS.File import File
#from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.BusinessObject import BusinessObject

class Fileset(BusinessObject):
    """
    A simple object representing a Fileset in WMBS.

    A fileset is a collection of files for processing. This could be a 
    complete block, a block in transfer, some user defined dataset, a 
    many file lumi-section etc.
    
    workflow + fileset = subscription
    
    """
    def __init__(self, name=None, id=-1, is_open=True, parents=None,
                 parents_open=True, source=None, sourceUrl=None,
                 logger=None, dbfactory = None):
        BusinessObject.__init__(self, logger=logger, dbfactory=dbfactory)
        """
        Create a new fileset
        """
        self.id = id
        self.name = name
        self.files = Set()
        self.newfiles = Set()
        self.open = is_open
        self.parents = set()
        self.setParentage(parents, parents_open)
        self.source = source
        self.sourceUrl = sourceUrl 
        self.lastUpdate = 0
    
    def setParentage(self, parents, parents_open):
        """
        Set parentage for this fileset - set parents to closed
        """
        if parents:
            for parent in parents:
                if isinstance(parent, Fileset):
                    self.parents.add(parent)
                else:
                    self.parents.add(Fileset(name=parent, db_factory=self.dbfactory, 
                            is_open=parents_open, parents_open=False))
    
    def exists(self):
        """
        Does a fileset exist with this name
        """
        return self.daofactory(classname='Fileset.Exists').execute(self.name)
        
    def create(self):
        """
        Add the new fileset to WMBS
        """
        self.daofactory(classname='Fileset.New').execute(self.name)
        self.populate()
        return self
    
    def delete(self):
        """
        Remove this fileset from WMBS
        """
        self.logger.warning('you are removing the following fileset from WMBS %s %s'
                                 % (self.name))
        
        return self.daofactory(classname='Fileset.Delete').execute(name=self.name)
    
    def populate(self): #, parentageLevel=0):
        """
        Load up the files in the file set from the database
        """
        #recursively go through parents
        #for parent in self.wmbs.getFilesetParents(self.name):
        #    self.parents.add(Fileset(parent[0], self.wmbs, bool(parent[1])).populate())
            
        #get my details
        values = self.daofactory(classname='Fileset.Load').execute(fileset=self.name)
        self.id, self.open, self.lastUpdate = values
        
        
        values = self.daofactory(classname='Files.InFileset').execute(fileset=self.name)
        for id, lfn, size, events, run, lumi in values:
            #id, lfn, size, events, run, lumi = f
            file = File(lfn, id, size, events, run, lumi, \
                        logger=self.logger, dbfactory=self.dbfactory)
            self.files.add(file)

        return self
            
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
            #comfiles.append(f.getInfo())
            self.logger.debug ( "commiting : %s" % f.lfn )  
            try:
                f.save()
                self.daofactory(classname='File.AddToFileset').execute(file=f.lfn, name=self.name)
                self.newfiles.remove(f)
            except IntegrityError, ex:
                self.logger.exception('File already exists in the database %s' % f.lfn)
                self.logger.exception(str(ex))
                raise IntegrityError, 'File %s already exists in the database' % f.lfn
        #self.newfiles = Set()
        self.populate()
#    
#    def createSubscription(self, workflow=None, subtype='Processing', parentage=0):
#        """
#        Create a subscription for the fileset using the given workflow 
#        """
#        s = Subscription(fileset = self, workflow = workflow, 
#                         type = subtype, parentage=parentage,
#                         logger=self.logger, dbfactory=self.dbfactory)
#        s.create()
#        return s
#        
#    def subscriptions(self, subtype=None):
#        """
#        Return all subscriptions for a fileset
#        """
#        #TODO: types should come from DB
#        if not type in (None, "Merge", "Processing", "Job"):
#            self.logger.exception('%s is an unknown subscription type' % 
#                                       subtype)
#            raise TypeError, '%s is an unknown subscription type' % subtype
#        
#        result = []
#        #TODO: change subscriptionsForFileset to return the workflow spec
#        #TODO: Add query for subtype
#        temp = self.daofactory(classname='Subscriptions.ForFileset').execute(fileset=self.name) #type=subtype)
#        for sub in temp:
#            result.append(Subscription(id=sub,
#                            logger=self.logger, dbfactory=self.dbfactory))
#        return result
