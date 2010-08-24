#!/usr/bin/env python
"""
_Fileset_

A simple object representing a Fileset in WMBS.

A fileset is a collection of files for processing. This could be a 
complete block, a block in transfer, some user defined dataset etc.

workflow + fileset = subscription

"""

__revision__ = "$Id: Fileset.py,v 1.7 2008/06/09 16:37:47 metson Exp $"
__version__ = "$Revision: 1.7 $"

from sets import Set
from sqlalchemy.exceptions import IntegrityError

from WMCore.WMBS.Actions.Fileset.Load import LoadFilesetAction
from WMCore.WMBS.Actions.Fileset.Exists import FilesetExistsAction
from WMCore.WMBS.Actions.Fileset.Delete import DeleteFilesetAction
from WMCore.WMBS.Actions.Fileset.New import NewFilesetAction
from WMCore.WMBS.File import File
from WMCore.WMBS.Subscription import Subscription

class Fileset(object):
    """
    A simple object representing a Fileset in WMBS.

    A fileset is a collection of files for processing. This could be a 
    complete block, a block in transfer, some user defined dataset etc.
    
    workflow + fileset = subscription
    
    """
    def __init__(self, name=name, dbinterface=None, logger=None, id=0, is_open=True,
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
        self.dbfactory = dbinterface
        self.logger = none
    
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
        conn = self.dbfactory.connect()
        action = FilesetExistsAction(self.logger)
        return action.execute(name=self.name,
                               dbinterface=conn)
        
    def create(self, conn = None):
        """
        Add the new fileset to WMBS
        """
        if not conn:
            conn = self.dbfactory.connect()
            
        for parent in self.parents:
            try:
                #todo: do in a single transaction
                parent.create(conn)
            except IntegrityError:
                self.wmbs.logger.warning('Fileset parent %s exists' % \
                                                         parent.name)
        try:
            action = NewFilesetAction(self.logger)
            return action.execute(name=self.name,
                               dbinterface=conn)
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
        conn = self.dbfactory.connect()
        action = DeleteFilesetAction(self.logger)
        return action.execute(name=self.name,
                               dbinterface=conn)
    
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
