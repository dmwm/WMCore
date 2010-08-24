#!/usr/bin/env python
"""
_Subscription_

A simple object representing a Subscription in WMBS.

A subscription is just a way to link many sets of jobs to a 
fileset and track the process of the associated jobs. It is 
associated to a single fileset and a single workflow.

workflow + fileset = subscription

subscription + application logic = jobs

TABLE wmbs_subscription
    id      INT(11) NOT NULL AUTO_INCREMENT,
    fileset INT(11) NOT NULL,
    workflow INT(11) NOT NULL,
    type    ENUM("merge", "processing")
"""

__revision__ = "$Id: Subscription.py,v 1.5 2008/06/24 11:45:32 metson Exp $"
__version__ = "$Revision: 1.5 $"

from sets import Set
from sqlalchemy.exceptions import IntegrityError
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.File import File
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.BusinessObject import BusinessObject

class Subscription(BusinessObject):
    def __init__(self, fileset = None, workflow = None, id = -1,
                  type = "Processing", logger=None, dbfactory = None):
        BusinessObject.__init__(self, logger=logger, dbfactory=dbfactory)
        
        self.fileset = fileset
        self.workflow = workflow
        self.type = type
        self.id = id
        
    def create(self):
        try:
            action = self.daofactory(classname="Subscription.New")
            action.execute(fileset = self.fileset.id, 
                           type = self.type,
                           workflow = self.workflow.id)
            
        except IntegrityError:
            self.logger.exception('Subcription %s:%s exists' % (self.fileset, self.workflow))
        
        action = self.daofactory(classname="Subscription.Exists")
        for i in action.execute(fileset = self.fileset.id, 
                                type = self.type,
                                workflow = self.workflow.id):
            self.id = i[0]
        return self

    def load(self, id=None):
        if id:
            self.id = id
        action = self.daofactory(classname='Subscription.Load')
        result = action.execute(fileset = self.fileset.id, 
                                workflow = self.workflow.id, 
                                id = self.id, 
                                type = self.type)
        if not result:
            raise RuntimeError, "Subscription for %s:%s unknown" % \
                                    (self.fileset.name, self.workflow.spec)
        self.fileset = result['fileset']
        self.workflow = result['workflow']
        self.type = result['type']
        self.id = result['id']
             
    def availableFiles(self, parents=0):
        """
        Return a Set of file ids that are available for processing 
        (e.g. not already in use)
        """
        files = []
        for f in self.daofactory:
            files.append(File(lfn=f[0], wmbs=self.wmbs).load(parentage=parents))
        return files
            
    def acquiredFiles(self):
        """
        Return a Set of file ids that have been processed
        """
        files = Set()
        for f in self.wmbs.listAcquiredFiles(self.id):
            files.add(File(lfn=f[0], wmbs=self.wmbs).load())
        return files
                     
    def acquireFiles(self, files = [], size = 0):
        """
        Acquire size files as active for the subscription. If size = 0 
        acquire all files (default behaviour).
        """
        if len(files):
            self.wmbs.acquireNewFiles(self.id, [x.id for x in files])
        elif size == 0:
            files = self.availableFiles()
            self.wmbs.acquireNewFiles(self.id, [x.id for x in files])
        else:
            files = self.availableFiles()
            l = []
            for i in range(size):
                l.append(files.pop())
                i = i + 1
            self.wmbs.acquireNewFiles(self.id, [x.id for x in l])
    
    def completeFiles(self, files):
        """
        Mark a (set of) file(s) as completed.
        """
        if not isinstance(files, list) and not isinstance(files, set):
            files=[files]
        self.wmbs.completeFiles(self.id, [x.id for x in files])
    
    def failFiles(self, files):
        """
        Mark a (set of) file(s) as failed. 
        """
        if not isinstance(files, list) and not isinstance(files, set):
            files=[files]
        self.wmbs.failFiles(self.id, [x.id for x in files])

    
    def completedFiles(self):
        """
        Mark a (set of) file(s) as completed.
        """
        files = Set()
        for f in self.wmbs.listCompletedFiles(self.id):
            files.add(File(lfn=f[0], wmbs=self.wmbs).load(parentage=self.parentage))
        return list(files)
    
    def failedFiles(self):
        """
        Mark a (set of) file(s) as failed. 
        """
        files = Set()
        for f in self.wmbs.listFailedFiles(self.id):
            files.add(File(lfn=f[0], wmbs=self.wmbs).load(parentage=self.parentage))
        return list(files)