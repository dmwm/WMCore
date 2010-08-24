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

__revision__ = "$Id: Subscription.py,v 1.6 2008/06/24 17:00:53 metson Exp $"
__version__ = "$Revision: 1.6 $"

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
            action = self.daofactory(classname="Subscriptions.New")
            action.execute(fileset = self.fileset.id, 
                           type = self.type,
                           workflow = self.workflow.id)
            
        except IntegrityError:
            self.logger.exception('Subcription %s:%s exists' % (self.fileset, self.workflow))
        
        action = self.daofactory(classname="Subscriptions.Exists")
        for i in action.execute(fileset = self.fileset.id, 
                                type = self.type,
                                workflow = self.workflow.id):
            self.id = i
        return self
    
    def exists(self):
        action = self.daofactory(classname="Subscriptions.Exists")
        value = action.execute(fileset = self.fileset.id, 
                                type = self.type,
                                workflow = self.workflow.id)
        return value
    
    def load(self, id=None):
        if not id and self.id > 0:
            id = self.id
        action = self.daofactory(classname='Subscriptions.Load')
        result = action.execute(fileset = self.fileset.id, 
                                workflow = self.workflow.id, 
                                id = id, 
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
        Return a Set of files that are available for processing 
        (e.g. not already in use)
        """
        files = Set()
        action = self.daofactory(classname='Subscriptions.GetAvailableFiles')
        for f in action.execute(self.id):
            files.append(File(lfn=f).load(parentage=parents))
        return files
            
    def acquiredFiles(self):
        """
        Set of files marked as acquired.
        """
        files = Set()
        action = self.daofactory(classname='Subscriptions.GetAcquiredFiles')
        for f in action.execute(self.id):
            files.add(File(lfn=f, wmbs=self.wmbs).load())
        return files

    def completedFiles(self):
        """
        Set of files marked as completed.
        """
        files = Set()
        action = self.daofactory(classname='Subscriptions.GetCompletedFiles')
        for f in action.execute(self.id):
            files.add(File(lfn=f[0], wmbs=self.wmbs).load(parentage=self.parentage))
        return list(files)
    
    def failedFiles(self):
        """
        Set of files marked as failed. 
        """
        files = Set()
        action = self.daofactory(classname='Subscriptions.GetFailedFiles')
        for f in action.execute(self.id):
            files.add(File(lfn=f[0], wmbs=self.wmbs).load(parentage=self.parentage))
        return list(files)
                     
    def acquireFiles(self, files = [], size = 0):
        """
        Acquire size files as active for the subscription. If size = 0 
        acquire all files (default behaviour).
        """
        action = self.daofactory(classname='Subscriptions.AcquireFiles')
        if len(files):
            action.execute(self.id, [x.id for x in files])
        elif size == 0:
            files = self.availableFiles()
            action.execute(self.id, [x.id for x in files])
        else:
            files = self.availableFiles()
            l = []
            for i in range(size):
                l.append(files.pop())
                i = i + 1
            action.execute(self.id, [x.id for x in l])
    
    def completeFiles(self, files):
        """
        Mark a (set of) file(s) as completed.
        """
        if not isinstance(files, list) and not isinstance(files, set):
            files=[files]
        self.daofactory(classname='Subscriptions.CompleteFiles').execute(self.id, [x.id for x in files])
    
    def failFiles(self, files):
        """
        Mark a (set of) file(s) as failed. 
        """
        if not isinstance(files, list) and not isinstance(files, set):
            files=[files]
        self.daofactory(classname='Subscriptions.FailFiles').execute(self.id, [x.id for x in files])