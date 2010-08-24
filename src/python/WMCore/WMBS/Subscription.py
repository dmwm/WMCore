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

__revision__ = "$Id: Subscription.py,v 1.28 2009/01/14 16:49:59 sfoulkes Exp $"
__version__ = "$Revision: 1.28 $"

from sets import Set

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
        self["id"] = int(result["id"])
        self["split_algo"] = result["split_algo"]

        self["fileset"] = Fileset(id = int(result["fileset"]))
        self["workflow"] = Workflow(id = int(result["workflow"]))
        return

    def loadData(self):
        """
        _loadData_

        """
        if self["id"] < 0 or self["fileset"] == None or \
               self["workflow"] == None:
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
          
    def filesOfStatus(self, status = None):
        """
        fids will be a set of id's, we'll then load the corresponding file 
        objects.
        """
        files = Set()
        action = self.daofactory(classname = "Subscriptions.Get%s" % status)
        for f in action.execute(self, conn = self.getReadDBConn(),
                                transaction = self.existingTransaction()):
            fl = File(id=f[0])
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
