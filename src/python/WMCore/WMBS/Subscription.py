#!/usr/bin/env python
#pylint: disable-msg=W0231
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

__revision__ = "$Id: Subscription.py,v 1.20 2008/10/28 17:42:17 metson Exp $"
__version__ = "$Revision: 1.20 $"

from sets import Set
from sqlalchemy.exceptions import IntegrityError
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.File import File
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.BusinessObject import BusinessObject
from WMCore.WMBS.Actions.Subscriptions.ChangeState import ChangeStateAction
from WMCore.DataStructs.Subscription import Subscription as WMSubscription

class Subscription(BusinessObject, WMSubscription):
    def __init__(self, fileset = None, workflow = None, id = -1,
                 whitelist = Set(), blacklist = Set(),
                 type = "Processing", split_algo = 'FileBased', 
                 logger=None, dbfactory = None):
        BusinessObject.__init__(self, logger=logger, dbfactory=dbfactory)
        self.fileset = fileset
        self.workflow = workflow
        self.type = type
        self.split_algo = split_algo
        self.id = id
        self.whitelist = whitelist
        self.blacklist = blacklist
        
    def create(self):
        """
        Add the subscription to the database
        """
        try:
            action = self.daofactory(classname="Subscriptions.New")
            action.execute(fileset = self.fileset.id, 
                           type = self.type,
                           workflow = self.workflow.id)
            
        except IntegrityError:
            self.logger.exception('Subcription %s:%s exists' % (self.fileset, 
                                                                self.workflow))
        
        action = self.daofactory(classname="Subscriptions.Exists")
        for i in action.execute(fileset = self.fileset.id, 
                                type = self.type,
                                workflow = self.workflow.id):
            self.id = i
        return self
    
    def exists(self):
        """
        See if the subscription is in the database
        """
        action = self.daofactory(classname="Subscriptions.Exists")
        value = action.execute(fileset = self.fileset.id, 
                                type = self.type,
                                workflow = self.workflow.id)
        return value
    
    def load(self, id=None):
        """
        Load the subscription and it's workflow and fileset from the database
        """
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
    
    def markLocation(self, location, whitelist = True):
        """
        Add a location to the subscriptions white or black list
        """
        # Check the location exists, add it if not
        try:
            self.daofactory(classname='Locations.New').execute(location)
        except IntegrityError:
            # location exists, do nothing
            pass
        
        # Mark the location as appropriate
        action = self.daofactory(classname='Subscriptions.MarkLocation')
        action.execute(self.id, location, whitelist)
          
    def filesOfStatus(self, status=None):
        """
        fids will be a set of id's, we'll then load the corresponding file 
        objects.
        """
        fids = Set()
        files = Set()
        action = self.daofactory(classname='Subscriptions.Get%s' % status)
        for f in action.execute(self.id):
            fids.add(f[0])
            fl = File(id=f[0], 
                           logger=self.logger, 
                           dbfactory=self.dbfactory)
            fl.load()
            files.add(fl)
        return files 
                     
    def acquireFiles(self, files = None, size = 0):
        """
        Acquire size files, activating them for the subscription. If size = 0 
        acquire all files (default behaviour). Return a list of files objects 
        for those acquired.
        """
        action = self.daofactory(classname='Subscriptions.AcquireFiles')
        if files:
            files = self.makelist(files)
            action.execute(self.id, [x.id for x in files])
            return files
        else:
            acq = self.acquiredFiles()
            files = self.availableFiles()
            l = Set()
            if len(files) < size or size == 0:
                size = len(files)
            i = 0
            while i < size:
                l.add(files.pop()['id'])
                i = i + 1
            action.execute(self.id, [x for x in l])
            ret = self.acquiredFiles() - acq
            
            return ret
    
    def completeFiles(self, files):
        """
        Mark a (set of) file(s) as completed.
        """
        if files and not isinstance(files, list) and not isinstance(files, set):
            files = [files]
        statechanger = ChangeStateAction(self.logger)
        statechanger.execute(subscription = self.id, 
                                  file = [x['id'] for x in files], 
                                  daofactory = self.daofactory)
    
    def failFiles(self, files):
        """
        Mark a (set of) file(s) as failed. 
        """
        if files and not isinstance(files, list) and not isinstance(files, set):
            files=[files]
        statechanger = ChangeStateAction(self.logger)
        statechanger.execute(subscription = self.id, 
                                  file = [x['id'] for x in files], 
                                  state = "FailFiles",
                                  daofactory = self.daofactory)

    def getJobs(self):
        """
        Return a list of all the jobs associated with a subscription
        """
        return self.daofactory(classname='Subscriptions.Jobs')
        