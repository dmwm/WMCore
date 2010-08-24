#!/usr/bin/env python
"""
_Fileset_

A simple object representing a Fileset in WMBS.

A fileset is a collection of files for processing. This could be a 
complete block, a block in transfer, some user defined dataset etc.

workflow + fileset = subscription

"""

__revision__ = "$Id: Fileset.py,v 1.6 2008/05/29 16:36:54 metson Exp $"
__version__ = "$Revision: 1.6 $"

from sets import Set
from sqlalchemy.exceptions import IntegrityError

from WMCore.WMBS.Actions.LoadFileset import LoadFilesetAction

from WMCore.WMBS.File import File
from WMCore.WMBS.Subscription import Subscription

class Fileset(object):
    """
    A simple object representing a Fileset in WMBS.

    A fileset is a collection of files for processing. This could be a 
    complete block, a block in transfer, some user defined dataset etc.
    
    workflow + fileset = subscription
    
    """
    def __init__(self, name, wmbs, id=0, is_open=True,
                    parents=None, parents_open=True, source=None, sourceUrl=None):
        """
        Create a new fileset
        """
        self.id = id
        self.name = name
        self.files = Set()
        self.newfiles = Set()
        self.wmbs = wmbs
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
                    self.parents.add(Fileset(parent, self.wmbs, 
                            is_open=parents_open, parents_open=False))
    
    
    def exists(self):
        """
        Does a fileset exist with this name
        """
        return self.wmbs.filesetExists(self.name)[0][0] > 0
        
    def create(self):
        """
        Add the new fileset to WMBS
        """
        for parent in self.parents:
            try:
                parent.create()
            except IntegrityError:
                self.wmbs.logger.warning('Fileset parent %s exists' % \
                                                         parent.name)
        try:
            self.wmbs.insertFileset(self.name, self.open,
                                    [x.name for x in self.parents])
        except IntegrityError:
            self.wmbs.logger.exception('Fileset %s exists' % self.name)
            #raise
        return self
    
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
        #recursively go through parents
        #for parent in self.wmbs.getFilesetParents(self.name):
        #    self.parents.add(Fileset(parent[0], self.wmbs, bool(parent[1])).populate())
            
        #get my details
        action = LoadFilesetAction(logger)
        values = action.execute(name=myfs, 
                   dbinterface=dbfactory.connect())
        
        self.open = values[2]
        self.lastUpdate = values[3]
        self.id = values[0]
        
        for f in self.wmbs.showFilesInFileset(self.name):
            id, lfn, size, events, run, lumi = f
            file = File(lfn, id, size, events, run, lumi)
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
            self.wmbs.logger.debug ( "commiting : %s" % comfiles )  
            try:
            #self.wmbs.insertFilesForFileset(files=comfiles, fileset=self.name)
                self.wmbs.addNewFileToNewLocation(f.getInfo(), fileset=self.name)
            except IntegrityError, ex:
                self.wmbs.logger.exception('File already exists in the database %s' % f)
                self.wmbs.logger.exception(str(ex))
                    #for i in self.newfiles:
                        #print i.getInfo()
                raise IntegrityError, 'File already exists in the database'
        self.newfiles = Set()
        self.populate()
    
    def createSubscription(self, workflow=None, subtype='Processing', parentage=0):
        """
        Create a subscription for the fileset using the given workflow 
        """
        s = Subscription(fileset = self, workflow = workflow, 
                         type = subtype, parentage=parentage, wmbs = self.wmbs)
        s.create()
        return s
        
    def subscriptions(self, subtype=None):
        """
        Return all subscriptions for a fileset
        """
        #TODO: types should come from DB
        if subtype in (None, "Merge", "Processing", "Job"):
            #TODO: change subscriptionsForFileset to return the workflow spec
            subscriptions = self.wmbs.subscriptionsForFileset(self.name, 
                                                              subtype)
        else:
            self.wmbs.logger.exception('%s is an unknown subscription type' % 
                                       subtype)
            raise TypeError, '%s is an unknown subscription type' % subtype
        return subscriptions
