#!/usr/bin/env python
"""
_JobGroup_

Definition of JobGroup:
    Set of jobs running on same input file for same Workflow
    Set of jobs for a single subscription
    Required for certain job splitting Algo's (.g. event split to make complete 
    lumi)
    Subscription:JobGroup == 1:N
    JobGroup:Jobs = 1:N
    JobGroup:InFile = 1:1
    JobGroup:MergedOutFile = N:1
    JobGroup at least one Lumi section

A JobGroup is a set of jobs and a Fileset that contains their output.

JobGroup knows the Subscription and passes the Workflow to Jobs in the group.

Jobs know their status (active, failed, complete) and know the files they run 
on but don't know the group. They do know their subscription and corresponding 
workflow. This means Jobs can update their state in the database without 
talking to the group, and WMBS JobGroups can calculate status from the database
instead of the in memory objects. 

The group has a status call which goes through the jobs and updates the db for 
state changes and then returns the status of the group (active, failed, 
complete).

WMAgent deals with groups and calls group.status periodically

CREATE TABLE wmbs_jobgroup (
     id           INTEGER      PRIMARY KEY AUTOINCREMENT,
     subscription INT(11)    NOT NULL,
     output       INT(11),
     last_update  TIMESTAMP NOT NULL,
     FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
       ON DELETE CASCADE,
     FOREIGN KEY (output) REFERENCES wmbs_fileset(id)
            ON DELETE CASCADE)
"""

__revision__ = "$Id: JobGroup.py,v 1.29 2009/05/12 16:19:23 sfoulkes Exp $"
__version__ = "$Revision: 1.29 $"

from WMCore.DataStructs.JobGroup import JobGroup as WMJobGroup
from WMCore.WMBS.WMBSBase import WMBSBase

from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.Subscription import Subscription

from WMCore.Services.UUID import makeUUID

from sets import Set

class JobGroup(WMBSBase, WMJobGroup):
    """
    A group (set) of Jobs
    """
    def __init__(self, subscription = None, jobs = None, id = -1, uid = None):
        WMBSBase.__init__(self)
        WMJobGroup.__init__(self, subscription=subscription, jobs = jobs)

        self.id = id
        self.lastUpdate = None
        self.uid = uid

        return
    
    def create(self):
        """
        Add the new jobgroup to WMBS, create the output Fileset object
        """
        existingTransaction = self.beginTransaction()
        
        self.output = Fileset(name = makeUUID())
        self.output.create()

        if self.uid == None:
            self.uid = makeUUID()

        action = self.daofactory(classname = "JobGroup.New")
        action.execute(self.uid, self.subscription["id"],
                       self.output.id, conn = self.getDBConn(),
                       transaction = self.existingTransaction())
        
        self.id = self.exists()
        self.commitTransaction(existingTransaction)
        return

    def delete(self):
        """
        Remove a jobgroup from WMBS
        """
        deleteAction = self.daofactory(classname = "JobGroup.Delete")
        deleteAction.execute(id = self.id, conn = self.getDBConn(),
                             transaction = self.existingTransaction())

        return

    def exists(self):
        """
        Does a jobgroup exist with id if id is not provided, use the uid, 
        return the id
        """
        if self.id != -1:
            action = self.daofactory(classname = "JobGroup.ExistsByID")
            result =  action.execute(id = self.id, conn = self.getDBConn(),
                                     transaction = self.existingTransaction())
        else:
            action = self.daofactory(classname = "JobGroup.Exists")
            result = action.execute(uid = self.uid, conn = self.getDBConn(),
                                    transaction = self.existingTransaction())

        return result
    
    def load(self):
        """
        _load_

        Load all meta data associated with the JobGroup.  This includes the
        JobGroup id, uid, last_update time, subscription id and output fileset
        id.  Either the JobGroup id or uid must be specified for this to work.
        """
        existingTransaction = self.beginTransaction()

        if self.id > 0:
            loadAction = self.daofactory(classname = "JobGroup.LoadFromID")
            result = loadAction.execute(self.id, conn = self.getDBConn(),
                                        transaction = self.existingTransaction())
        else:
            loadAction = self.daofactory(classname = "JobGroup.LoadFromUID")
            result = loadAction.execute(self.uid, conn = self.getDBConn(),
                                        transaction = self.existingTransaction())

        self.id = result["id"]
        self.uid = result["uid"]
        self.lastUpdate = result["last_update"]
            
        self.subscription = Subscription(id = result["subscription"])
        self.subscription.load()
        
        self.output = Fileset(id = result["output"])
        self.output.load()
        
        self.jobs = []
        self.commitTransaction(existingTransaction)
        return

    def loadData(self):
        """
        _loadData_
        
        Load all data that is associated with the jobgroup.  This includes
        loading all the subscription information, the output fileset
        information and all the jobs that are associated with the group.
        """
        existingTransaction = self.beginTransaction()

        if self.id < 0 or self.uid == None:
            self.load()

        self.subscription.loadData()
        self.output.loadData()

        loadAction = self.daofactory(classname = "JobGroup.LoadJobs")
        result = loadAction.execute(self.id, conn = self.getDBConn(),
                                    transaction = self.existingTransaction())

        self.jobs = []
        self.newjobs = []

        for jobID in result:
            newJob = Job(id = jobID["id"])
            newJob.loadData()
            self.add(newJob)

        WMJobGroup.commit(self)
        self.commitTransaction(existingTransaction)
        return
    
    def commit(self):
        """
        _commit_

        Write any new jobs to the database, creating them in the database if
        necessary.
        """
        existingTransaction = self.beginTransaction()

        if self.id == -1:
            self.create()
        
        for j in self.newjobs:
            j.create(group = self)

        WMJobGroup.commit(self)
        self.commitTransaction(existingTransaction)
        return
