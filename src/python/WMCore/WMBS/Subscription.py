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

__revision__ = "$Id: Subscription.py,v 1.45 2009/09/11 19:08:07 mnorman Exp $"
__version__ = "$Revision: 1.45 $"

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
        existingTransaction = self.beginTransaction()

        if self.exists() != False:
            self.load()
            return
        
        action = self.daofactory(classname="Subscriptions.New")
        action.execute(fileset = self["fileset"].id, type = self["type"],
                       split = self["split_algo"],
                       workflow = self["workflow"].id,
                       conn = self.getDBConn(),
                       transaction = self.existingTransaction())
        
        # Reload so we pick up the ID for location entries
        self.load()
        
        # Add white / blacklist entries
        for whiteEntry in self['whitelist']:
            self.markLocation(whiteEntry, True)
        for blackEntry in self['blacklist']:
            self.markLocation(blackEntry, False)
        
        self.load()
        self.commitTransaction(existingTransaction)
        return
    
    def exists(self):
        """
        See if the subscription is in the database
        """
        action = self.daofactory(classname="Subscriptions.Exists")
        result = action.execute(fileset = self["fileset"].id,
                                type = self["type"],
                                workflow = self["workflow"].id,
                                conn = self.getDBConn(),
                                transaction = self.existingTransaction())
        return result
    
    def load(self):
        """
        _load_

        Load any meta data about the subscription.  This include the id, type,
        split algorithm, fileset id and workflow id.  Either the subscription id
        or the fileset id and workflow id must be specified for this to work.
        """
        existingTransaction = self.beginTransaction()

        if self["id"] > 0:
            action = self.daofactory(classname = "Subscriptions.LoadFromID")
            result = action.execute(id = self["id"],
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction())
        else:
            action = self.daofactory(classname = "Subscriptions.LoadFromFilesetWorkflow")
            result = action.execute(fileset = self["fileset"].id,
                                    workflow = self["workflow"].id,
                                    conn = self.getDBConn(),
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
            
        self.commitTransaction(existingTransaction)
        return

    def loadData(self):
        """
        _loadData_

        Load all data having to do with the subscription including all the
        files contained in the fileset and the workflow meta data.
        """
        existingTransaction = self.beginTransaction()
        
        if self["id"] < 0 or self["fileset"].id < 0 or \
               self["workflow"].id < 0:
            self.load()
        
        self["fileset"].loadData()
        self["workflow"].load()

        self.commitTransaction(existingTransaction)
        return
    
    def markLocation(self, location, whitelist = True):
        """
        Add a location to the subscriptions white or black list
        """
        existingTransaction = self.beginTransaction()

        locationAction = self.daofactory(classname = "Locations.New")
        locationAction.execute(location, conn = self.getDBConn(),
                               transaction = self.existingTransaction())
        
        # Mark the location as appropriate
        action = self.daofactory(classname = "Subscriptions.MarkLocation")
        action.execute(self["id"], location, whitelist, conn = self.getDBConn(),
                       transaction = self.existingTransaction())

        self.commitTransaction(existingTransaction)
        return
          
    def filesOfStatus(self, status):
        """
        _filesOfStatus_
        
        Return a Set of File objects that have the given status with respect
        to this subscription.
        """
        existingTransaction = self.beginTransaction()

        status = status.title()
        files  = Set()
        action = self.daofactory(classname = "Subscriptions.Get%sFiles" % status)
        for f in action.execute(self["id"], conn = self.getDBConn(),
                                transaction = self.existingTransaction()):
            fl = File(id = f["file"])
            fl.load()
            if "locations" in f.keys():
                fl.setLocation(f["locations"])
            files.add(fl)


        self.commitTransaction(existingTransaction)
        return files 
    
    def acquireFiles(self, files = None):
        """
        _acuireFiles_
        
        Mark all files objects that are passed in as acquired for this
        subscription.  If now files are passed in then all available files
        will be acquired.
        """
        existingTransaction = self.beginTransaction()

        deleteAction = self.daofactory(classname = "Subscriptions.ClearFileStatus")
        action = self.daofactory(classname = "Subscriptions.AcquireFiles")

        if not files:
            files = self.filesOfStatus("Available")
        else:
            files = self.makelist(files)

        if len(files) == 0:
            return

        deleteAction.execute(subscription = self["id"],
                             file = [x["id"] for x in files],
                             conn = self.getDBConn(),
                             transaction = self.existingTransaction())

        action.execute(self['id'], file = [x["id"] for x in files],
                       conn = self.getDBConn(),
                       transaction = self.existingTransaction())
            
            
        self.commitTransaction(existingTransaction)
        return
    
    def completeFiles(self, files):
        """
        Mark a (set of) file(s) as completed.
        """
        existingTransaction = self.beginTransaction()

        files = self.makelist(files)
        
        deleteAction = self.daofactory(classname = "Subscriptions.ClearFileStatus")
        deleteAction.execute(subscription = self["id"],
                             file = [x["id"] for x in files],
                             conn = self.getDBConn(),
                             transaction = self.existingTransaction())

        completeAction = self.daofactory(classname = "Subscriptions.CompleteFiles")
        completeAction.execute(subscription = self["id"],
                               file = [x["id"] for x in files],
                               conn = self.getDBConn(),
                               transaction = self.existingTransaction())

        self.commitTransaction(existingTransaction)
        return
    
    def failFiles(self, files):
        """
        Mark a (set of) file(s) as failed. 
        """
        existingTransaction = self.beginTransaction()

        files = self.makelist(files)
        
        deleteAction = self.daofactory(classname = "Subscriptions.ClearFileStatus")
        deleteAction.execute(subscription = self["id"],
                             file = [x["id"] for x in files],
                             conn = self.getDBConn(),
                             transaction = self.existingTransaction())
        
        failAction = self.daofactory(classname = "Subscriptions.FailFiles")
        failAction.execute(subscription = self["id"],
                           file = [x["id"] for x in files],
                           conn = self.getDBConn(),
                           transaction = self.existingTransaction())
        
        self.commitTransaction(existingTransaction)
        return
    
    def getJobs(self):
        """
        Return a list of all the jobs associated with a subscription
        """
        jobsAction = self.daofactory(classname = "Subscriptions.Jobs")
        jobs = jobsAction.execute(subscription = self["id"],
                                  conn = self.getDBConn(),
                                  transaction = self.existingTransaction())

        return jobs
        
    def delete(self):
        """
        _delete_

        Delete this subscription from the database.
        """
        action = self.daofactory(classname = "Subscriptions.Delete")
        action.execute(id = self["id"], conn = self.getDBConn(),
                       transaction = self.existingTransaction())

        return
    
    def isCompleteOnRun(self, runID):
        """
        _isCompleteOnRun_
        
        Check all the files in the given subscripton and the given run are completed.
        
        To: check query whether performance can be improved
        """
        statusAction = self.daofactory(classname = "Subscriptions.IsCompleteOnRun")
        fileCount = statusAction.execute(self["id"], runID,
                                      conn = self.getDBConn(),
                                      transaction = self.existingTransaction())

        if fileCount == 0:
            return True
        else:
            return False
        
    def filesOfStatusByRun(self, status, runID):
        """
        _filesOfStatusByRun_
        
        Return all the files in the given subscription and the given run which
        have the given status.
        """
        existingTransaction = self.beginTransaction()

        files = []
        action = self.daofactory(classname = "Subscriptions.Get%sFilesByRun" % status)
        for f in action.execute(self["id"], runID, conn = self.getDBConn(),
                                transaction = self.existingTransaction()):
            fl = File(id = f["file"])
            fl.load()
            files.append(fl)

        self.commitTransaction(existingTransaction)
        return files 


    def getNumberOfJobsPerSite(self, location, state):
        """
        _getNumberOfJobsPerSite_
        
        Access the number of jobs at a site in a given status for a given subscription

        """

        jobLocate  = self.daofactory(classname = "Subscriptions.GetNumberOfJobsPerSite")

        result = jobLocate.execute(location = location, subscription = self['id'], state = state).values()[0]

        return result
 
    def getJobGroups(self):
        """
            Returns a list of job groups associated with the subscription
            that has jobs that arent acquired
            
            NOTE:
                If you have a jobgroup that is partially acquired,
                i.e. there are two jobs within a group and one is acquired
                and the other one isn't, the group will appear in the list
            DOUBLENOTE:
                you shouldn't be doing that anyway. by policy you need to
                either get the whole group or none at all. Halfway is
                naughty
        """
        # first, make sure we have all the necessary data
        self.loadData()
        
        action = self.daofactory( classname = "Subscriptions.GetJobGroups" )
        return action.execute(self['id'], conn = self.getDBConn(),
                                          transaction = self.existingTransaction())
   
