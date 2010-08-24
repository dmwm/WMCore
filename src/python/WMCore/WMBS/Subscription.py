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

__revision__ = "$Id: Subscription.py,v 1.12 2008/07/03 17:16:56 metson Exp $"
__version__ = "$Revision: 1.12 $"

from sets import Set
from sqlalchemy.exceptions import IntegrityError
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.BusinessObject import BusinessObject
from WMCore.WMBS.Actions.Subscriptions.ChangeState import ChangeStateAction
from WMCore.DataStructs.Subscription import Subscription as WMSubscription

class Subscription(BusinessObject, WMSubscription):
    def __init__(self, fileset = None, workflow = None, id = -1,
                  type = "Processing", split_algo = 'File', 
                  logger=None, dbfactory = None):
        BusinessObject.__init__(self, logger=logger, dbfactory=dbfactory)
        WMSubscription.__init__(self, fileset=fileset, workflow=workflow, 
                                type=type, split_algo = split_algo)
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
        self.fileset = Fileset(id = result['fileset'], 
                               logger=self.logger, 
                               dbfactory=self.dbfactory).populate('Fileset.LoadFromID')
        self.workflow = Workflow(id = result['workflow'], 
                                 logger=self.logger, 
                                 dbfactory=self.dbfactory).load('Workflow.LoadFromID')
        self.type = result['type']
        self.id = result['id']
        self.split_algo = result['split_algo']
             
    def availableFiles(self, parents=0):
        """
        Return a Set of files that are available for processing 
        (e.g. not already in use)
        """
        files = Set()
        action = self.daofactory(classname='Subscriptions.GetAvailableFiles')
        for f in action.execute(self.id):
            files.add(f[0])
        return files
            
    def acquiredFiles(self):
        """
        Set of files marked as acquired.
        """
        files = Set()
        action = self.daofactory(classname='Subscriptions.GetAcquiredFiles')
        for f in action.execute(self.id):
            files.add(f[0])
        return files

    def completedFiles(self):
        """
        Set of files marked as completed.
        """
        files = Set()
        action = self.daofactory(classname='Subscriptions.GetCompletedFiles')
        for f in action.execute(self.id):
            files.add(f[0])
        return files
    
    def failedFiles(self):
        """
        Set of files marked as failed. 
        """
        files = Set()
        action = self.daofactory(classname='Subscriptions.GetFailedFiles')
        for f in action.execute(self.id):
            files.add(f[0])
        return files
                     
    def acquireFiles(self, files = [], size = 0):
        """
        Acquire size files as active for the subscription. If size = 0 
        acquire all files (default behaviour).
        """
        action = self.daofactory(classname='Subscriptions.AcquireFiles')
        if len(files):
            action.execute(self.id, [x.id for x in files])
            return [x.id for x in files]
        else:
            files = self.availableFiles()
            l = []
            if len(files) < size or size == 0:
                size = len(files)
            for i in range(size):
                l.append(files.pop())
                i = i + 1
            action.execute(self.id, [x for x in l])
            return l
    
    def completeFiles(self, files):
        """
        Mark a (set of) file(s) as completed.
        """
        if not isinstance(files, list) and not isinstance(files, set):
            files=[files]
        statechanger = ChangeStateAction(self.logger)
        statechanger.execute(subscription=self.id, 
                                  file=[x for x in files], 
                                  daofactory = self.daofactory)
    
    def failFiles(self, files):
        """
        Mark a (set of) file(s) as failed. 
        """
        if not isinstance(files, list) and not isinstance(files, set):
            files=[files]
        statechanger = ChangeStateAction(self.logger)
        statechanger.execute(subscription=self.id, 
                                  file=[x for x in files], 
                                  state="FailFiles",
                                  daofactory = self.daofactory)
