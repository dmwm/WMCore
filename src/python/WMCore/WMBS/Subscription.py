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
    type    ENUM("Merge", "Frocessing")
"""

__revision__ = "$Id: Subscription.py,v 1.33 2009/03/16 16:58:39 sfoulkes Exp $"
__version__ = "$Revision: 1.33 $"

from sets import Set
import logging

from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.File import File
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.WMBSBase import WMBSBase

from WMCore.DataStructs.Subscription import Subscription as WMSubscription

class Subscription(WMBSBase, WMSubscription):
    def __init__(self, fileset = None, workflow = None, id = -1,
                 whitelist = None, blacklist = None, split_algo = "FileBased",
                 type = "Processing"):
        WMBSBase.__init__(self)

        # If a fileset or workflow isn't passed in the base class will create
        # empty non-WMBS filesets and workflows.  We want WMBS filesets and
        # workflows so we'll create those here.
        if fileset == None:
            fileset = Fileset()
        if workflow == None:
            workflow = Workflow()
            
        WMSubscription.__init__(self, fileset = fileset, workflow = workflow,
                                whitelist = whitelist, blacklist = blacklist,
                                split_algo = split_algo, type = type)

        self.setdefault("id", id)
        return
        
    def create(self):
        """
        Add the subscription to the database
        """
        if self.exists() != False:
            self.load()
            return
        
        action = self.daofactory(classname="Subscriptions.New")
        action.execute(fileset = self["fileset"].id, type = self["type"],
                       split = self["split_algo"],
                       workflow = self["workflow"].id,
                       conn = self.getWriteDBConn(),
                       transaction = self.existingTransaction())
        
        # Reload so we pick up the ID for location entries
        self.load()
        
        # Add white / blacklist entries
        for whiteEntry in self['whitelist']:
            self.markLocation(whiteEntry, True)
        for blackEntry in self['blacklist']:
            self.markLocation(blackEntry, False)
        
        self.load()        
        self.commitIfNew()
        return
    
    def exists(self):
        """
        See if the subscription is in the database
        """
        action = self.daofactory(classname="Subscriptions.Exists")
        value = action.execute(fileset = self["fileset"].id,
                               type = self["type"],
                               workflow = self["workflow"].id,
                               conn = self.getReadDBConn(),
                               transaction = self.existingTransaction())
        return value
    
    def load(self):
        """
        _load_

        Load any meta data about the subscription.  This include the id, type,
        split algorithm, fileset id and workflow id.  Either the subscription id
        or the fileset id and workflow id must be specified for this to work.
        """
        if self["id"] > 0:
            action = self.daofactory(classname = "Subscriptions.LoadFromID")
            result = action.execute(id = self["id"],
                                    conn = self.getReadDBConn(),
                                    transaction = self.existingTransaction())
        else:
            action = self.daofactory(classname = "Subscriptions.LoadFromFilesetWorkflow")
            result = action.execute(fileset = self["fileset"].id,
                                    workflow = self["workflow"].id,
                                    conn = self.getReadDBConn(),
                                    transaction = self.existingTransaction())            

        self["type"] = result["type"]
        self["id"] = result["id"]
        self["split_algo"] = result["split_algo"]

        # Only load the fileset and workflow if they haven't been loaded
        # already.  
        if self["fileset"].id < 0:
            self["fileset"] = Fileset(id = result["fileset"])
        if self["workflow"].id < 0:
            self["workflow"] = Workflow(id = result["workflow"])
            
        return

    def loadData(self):
        """
        _loadData_

        Load all data having to do with the subscription including all the
        files contained in the fileset and the workflow meta data.
        """
        if self["id"] < 0 or self["fileset"].id < 0 or \
               self["workflow"].id < 0:
            self.load()
        
        self["fileset"].loadData()
        self["workflow"].load()

        return    
    
    def markLocation(self, location, whitelist = True):
        """
        Add a location to the subscriptions white or black list
        """
        locationAction = self.daofactory(classname='Locations.New')
        locationAction.execute(location, conn = self.getWriteDBConn(),
                               transaction = self.existingTransaction())
        
        # Mark the location as appropriate
        action = self.daofactory(classname = "Subscriptions.MarkLocation")
        action.execute(self["id"], location, whitelist,
                       conn = self.getWriteDBConn(),
                       transaction = self.existingTransaction())

        self.commitIfNew()
        return
          
    def filesOfStatus(self, status, maxFiles = 100):
        """
        _filesOfStatus_
        
        Return a Set of File objects that have the given status with respect
        to this subscription.  By default this will return at most 100 files,
        or whatever is specified.
        """
        files = Set()
        action = self.daofactory(classname = "Subscriptions.Get%s" % status)
        for f in action.execute(self["id"], maxFiles,
                                conn = self.getReadDBConn(),
                                transaction = self.existingTransaction()):
            fl = File(id = f["file"])
            fl.load()
            files.add(fl)
            
        return files 
    
    def acquireFiles(self, files = None, size = 0):
        """
        Acquire size files, activating them for the subscription. If size = 0 
        acquire all files (default behaviour). Return a list of files objects 
        for those acquired.
        """
        action = self.daofactory(classname = "Subscriptions.AcquireFiles")
        if files:
            files = self.makelist(files)
            action.execute(self['id'], [x['id'] for x in files],
                           conn = self.getWriteDBConn(),
                           transaction = self.existingTransaction())
            self.commitIfNew()
            return files
        
        acq = self.acquiredFiles()
        files = self.availableFiles()
        l = Set()
        if len(files) < size or size == 0:
            size = len(files)
        i = 0
        while i < size:
            l.add(files.pop()['id'])
            i = i + 1
        action.execute(self['id'], [x for x in l], conn = self.getWriteDBConn(),
                       transaction = self.existingTransaction())
        ret = self.acquiredFiles() - acq

        self.commitIfNew()
        return ret
    
    def completeFiles(self, files):
        """
        Mark a (set of) file(s) as completed.
        """
        if files and not isinstance(files, list) and not isinstance(files, set):
            files = [files]

        completeAction = self.daofactory(classname = "Subscriptions.CompleteFiles")
        completeAction.execute(subscription = self["id"],
                               file = [x["id"] for x in files],
                               conn = self.getWriteDBConn(),
                               transaction = self.existingTransaction())
        deleteAction = self.daofactory(classname = "Subscriptions.DeleteAcquiredFiles")
        deleteAction.execute(subscription = self["id"],
                             file = [x["id"] for x in files],
                             conn = self.getWriteDBConn(),
                             transaction = self.existingTransaction())

        self.commitIfNew()
        return
    
    def failFiles(self, files):
        """
        Mark a (set of) file(s) as failed. 
        """
        if files and not isinstance(files, list) and not isinstance(files, set):
            files=[files]

        failAction = self.daofactory(classname = "Subscriptions.FailFiles")
        failAction.execute(subscription = self["id"],
                           file = [x["id"] for x in files],
                           conn = self.getWriteDBConn(),
                           transaction = self.existingTransaction())
        deleteAction = self.daofactory(classname = "Subscriptions.DeleteAcquiredFiles")
        deleteAction.execute(subscription = self["id"],
                             file = [x["id"] for x in files],
                             conn = self.getWriteDBConn(),
                             transaction = self.existingTransaction())

        self.commitIfNew()
        return
    
    def getJobs(self):
        """
        Return a list of all the jobs associated with a subscription
        """
        jobsAction = self.daofactory(classname = "Subscriptions.Jobs")
        jobs = jobsAction.execute(subscription = self["id"],
                                  conn = self.getReadDBConn(),
                                  transaction = self.existingTransaction())
        return jobs
        
    def delete(self):
        """
        _delete_

        """
        action = self.daofactory(classname = "Subscriptions.Delete")
        action.execute(id = self["id"], conn = self.getWriteDBConn(),
                       transaction = self.existingTransaction())

        self.commitIfNew()
        return
