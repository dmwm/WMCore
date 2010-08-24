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

__revision__ = "$Id: JobGroup.py,v 1.20 2009/02/02 21:01:49 sfoulkes Exp $"
__version__ = "$Revision: 1.20 $"

from WMCore.Database.Transaction import Transaction
from WMCore.DataStructs.JobGroup import JobGroup as WMJobGroup
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.WMBSBase import WMBSBase
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
        self.beginTransaction()
        
        self.groupoutput = Fileset(name = makeUUID())
        self.groupoutput.create()

        if self.uid == None:
            self.uid = makeUUID()

        action = self.daofactory(classname="JobGroup.New")
        action.execute(self.uid, self.subscription["id"],
                       self.groupoutput.id, conn = self.getWriteDBConn(),
                       transaction = self.existingTransaction())
        
        self.id = self.exists()
        self.commitIfNew()
        return

    def delete(self):
        """
        Remove a jobgroup from WMBS
        """
        deleteAction = self.daofactory(classname = "JobGroup.Delete")
        deleteAction.execute(id = self.id, conn = self.getWriteDBConn(),
                             transaction = self.existingTransaction())

        self.commitIfNew()
        return

    def exists(self):
        """
        Does a jobgroup exist with id if id is not provided, use the uid, 
        return the id
        """
        if self.id != -1:
            action = self.daofactory(classname = "JobGroup.ExistsByID")
            return action.execute(id = self.id, conn = self.getReadDBConn(),
                                  transaction = self.existingTransaction())
        else:
            action = self.daofactory(classname = "JobGroup.Exists")
            return action.execute(uid = self.uid, conn = self.getReadDBConn(),
                                  transaction = self.existingTransaction())
    
    def load(self):
        """
        _load_

        Load all meta data associated with the JobGroup.  This includes the
        JobGroup id, uid, last_update time, subscription id and output fileset
        id.  Either the JobGroup id or uid must be specified for this to work.
        """
        if self.id > 0:
            loadAction = self.daofactory(classname = "JobGroup.LoadFromID")
            result = loadAction.execute(self.id, conn = self.getReadDBConn(),
                                        transaction = self.existingTransaction())
        else:
            loadAction = self.daofactory(classname = "JobGroup.LoadFromUID")
            result = loadAction.execute(self.uid, conn = self.getReadDBConn(),
                                        transaction = self.existingTransaction())            

        self.id = result["id"]
        self.uid = result["uid"]
        self.lastUpdate = result["last_update"]
            
        self.subscription = Subscription(id = result["subscription"])
        self.groupoutput = Fileset(id = result["output"])

        self.jobs.clear()
        return

    def loadData(self):
        """
        _loadData_
        
        Load all data that is associated with the jobgroup.  This includes
        loading all the subscription information, the output fileset
        information and all the jobs that are associated with the group.
        """
        if self.id < 0 or self.uid == None:
            self.load()

        self.subscription.loadData()
        self.groupoutput.loadData()

        jobIDs = self.getJobIDs(type="dict")
        
        for jobID in jobIDs:
            newJob = Job(id = jobID["id"])
            newJob.loadData()
            self.jobs.add(newJob)

        return    
    
    def getJobIDs(self, type="dict"):
        """
        return list of Job IDs
        If type is JobList return list of Job object with only id field is 
        filled
        """
        jobAction = self.daofactory(classname = "JobGroup.LoadJobs")
        jobIDs = jobAction.execute(self.id, conn = self.getReadDBConn(),
                                   transaction = self.existingTransaction())
        
        if type == "JobList":
            jobList = []
            for jobID in jobIDs:
                jobList.append(Job(id = jobID["id"]))
            return jobList
        elif type == "list":
            idList = []
            for jobID in jobIDs:
                idList.append(jobID["id"])
            return idList
        else:
            return jobIDs
        
    def commit(self):
        """
        _commit_

        Write any new jobs to the database, creating them in the database if
        necessary.
        """
        self.beginTransaction()

        if self.id == -1:
            self.create()
        
        for j in self.newjobs:
            j.create(group=self)
            
        WMJobGroup.commit(self)
        self.commitIfNew()
        return
    
    def recordAcquire(self, jobs = None):
        """
        _recordAcquire_
        
        Mark a set of jobs that are associated with the job group as acquired.
        If no list of jobs is passed in then mark all jobs as acquired.
        """
        if self.status() != "ACTIVE":
            return False

        if jobs == None:
            jobs = self.getJobIDs(type = "JobList")
                
        for job in jobs: 
            self.subscription.acquireFiles(job.getFileIDs(type = "dict"))

        return True
    
    def recordComplete(self, jobs = None):
        """
        _recordComplete_

        Mark a set of jobs that are associated with the job group as complete.
        If no list of jobs is passed in then mark all jobs as complete.
        """
        if self.status() != "COMPLETE":
            return False

        if jobs == None:
            jobs = self.getJobIDs(type = "JobList")
                
        for job in jobs:
            self.subscription.completeFiles(job.getFileIDs(type = "dict"))

        return True
    
    def recordFail(self, jobs = None):
        """
        _recordFail_

        Mark a set of jobs that are associated with the job group as failed.
        If no list of jobs is passed in then mark all jobs as failed.
        """
        if self.status() != "FAILED":
            return False

        if jobs == None:
            jobs = self.getJobIDs(type = "JobList")
            
        for job in jobs: 
            self.subscription.failFiles(job.getFileIDs(type = "dict"))

        return True
    
    def status(self, detail = False):
        """
        The status of the job group is the sum of the status of all jobs in the
        group.
        
        The status of the jobs can be correctly inferred by comparing the start,
        complete and update times. The groups status is the sum of these 
        statuses.
        
        return: ACTIVE, COMPLETE, FAILED
        """
        statusAction = self.daofactory(classname = "JobGroup.Status")
        av, ac, fa, cm = statusAction.execute(self.id,
                                              conn = self.getReadDBConn(),
                                              transaction = self.existingTransaction())
    
        total = av + ac + fa + cm
        
        if total > 0:
            report = ''
            if detail:
                report = ' (av %s, ac %s, fa %s, cm %s)' % (av, ac, fa, cm)
            if cm == total:
                return 'COMPLETE%s' % report
            elif fa > 0:
                return 'FAILED%s' % report
            else:
                # all the file status should be acquired at this point
                return 'ACTIVE%s' % report
    
    def output(self):
        """
        The output is the files produced by the jobs in the group - these must
        be merged up together.
        """
        if self.status() == 'COMPLETE':
            # output only makes sense if the group is completed
            # load output from DB 
            self.groupoutput.load(method = "Fileset.LoadFromID")
            return self.groupoutput
        self.logger.debug(self.status(detail=True))
        return False
