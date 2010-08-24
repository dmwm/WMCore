#!/usr/bin/env python
"""
_Fileset_

A simple object representing a Fileset in WMBS.

A fileset is a collection of files for processing. This could be a 
complete block, a block in transfer, some user defined dataset etc.

workflow + fileset = subscription

"""

__revision__ = "$Id: Fileset.py,v 1.16 2008/07/03 16:39:49 metson Exp $"
__version__ = "$Revision: 1.16 $"

from sets import Set
from sqlalchemy.exceptions import IntegrityError

from WMCore.WMBS.File import File
#from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.BusinessObject import BusinessObject
from WMCore.DataStructs.Fileset import Fileset as WMFileset

class Fileset(BusinessObject, WMFileset):
    """
    A simple object representing a Fileset in WMBS.

    A fileset is a collection of files for processing. This could be a 
    complete block, a block in transfer, some user defined dataset, a 
    many file lumi-section etc.
    
    workflow + fileset = subscription
    
    """
    def __init__(self, name=None, id=-1, is_open=True, files=Set(), parents=Set(), 
                 parents_open=True, source=None, sourceUrl=None,
                 logger=None, dbfactory = None):
        BusinessObject.__init__(self, logger=logger, dbfactory=dbfactory)
        WMFileset.__init__(self, name = name, files=files)
        """
        Create a new fileset
        """
        self.id = id
        self.open = is_open
        self.parents = parents
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
        Does a fileset exist with this name in the database
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
    
    def populate(self, method='Fileset.LoadFromName'): #, parentageLevel=0):
        """
        Load up the files in the file set from the database
        """
        action = self.daofactory(classname=method)    
        values = None
        #get my details
        if method == 'Fileset.LoadFromName':
            values = action.execute(fileset=self.name)
            self.id, self.open, self.lastUpdate = values
        elif method == 'Fileset.LoadFromID':
            values = action.execute(fileset=self.id)
            self.name, self.open, self.lastUpdate = values
        else:
            raise TypeError, 'populate method not supported'
        
        
        
        values = self.daofactory(classname='Files.InFileset').execute(fileset=self.name)
        for id, lfn, size, events, run, lumi in values:
            #id, lfn, size, events, run, lumi = f
            file = File(lfn, id, size, events, run, lumi, \
                        logger=self.logger, dbfactory=self.dbfactory)
            self.add(file)

        return self
    
    def commit(self):
        """
        Add contents of self.newfiles to the database, empty self.newfiles, reload self
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
