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

__revision__ = "$Id: JobGroup.py,v 1.27 2009/04/29 23:23:29 sryu Exp $"
__version__ = "$Revision: 1.27 $"

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
        self.subscription.load()
        
        self.groupoutput = Fileset(id = result["output"])
        self.groupoutput.load()
        
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
        self.beginTransaction()
        
        jobAction = self.daofactory(classname = "JobGroup.LoadJobs")
        jobIDs = jobAction.execute(self.id, conn = self.getReadDBConn(),
                                   transaction = self.existingTransaction())

        self.commitIfNew()
        
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
        If no list of jobs is passed in then mark all files in jobGroup 
        as acquired.
        """
        if jobs == None:
            jobs = self.getJobIDs(type = "JobList")
        
        if type(jobs) != list:
            jobs = [jobs]
        self.beginTransaction()
        for job in jobs:
            job.changeStatus("Active")
            inputFiles = job.getFiles()

            if len(inputFiles) > 0:
                self.subscription.acquireFiles(inputFiles)

        self.commitIfNew()
        return True
    
    def _recordFileStatusUponCompletion(self):
        """
        _recordFileStatusUponCompletion_
        
        This checks whether job group is completed 
        (complete status means all the jobs are processed regardless their 
        status (failed, or complete (succeed)) then update the input files'
        status on those jobs if a input file is shared by more than a job, 
        and one of the jobs are failed it will be marked as failed.
        A file is marked as successful only if all the jobs using that file
        as input succeed 
        
        if job group is in complete status return status, if not, False 
        """
        status = self._completeStatus()
        
        if status == "COMPLETE" or status == "FailComplete":
            jobs = self.getJobIDs(type = "JobList")
            inputFiles = []
            failFiles = []
            if status == "COMPLETE":
                for job in jobs:
                    inputFiles.extend(job.getFiles())
            else:
                for job in jobs:
                    if job.getStatus() == "FAIED":
                        failFiles.extend(job.getFile())
                    else:
                        inputFiles.extend(job.getFiles())
                        
            if len(inputFiles) > 0:
                self.subscription.completeFiles(inputFiles)
                
            if len(failFiles) > 0:
                self.subscription.completeFiles(failFiles)
            # Don't commit here this function is only supposed to be used within
            # other function in the class
            #self.commitIfNew()
            return status
        
        return False
    
    def recordComplete(self, job, outputFiles):
        """
        _recordComplete_

        Mark a set of jobs that are associated with the job group as complete.
        If no list of jobs is passed in then mark all files in jobGroup 
        as complete.
        
        calling recordComplete without jobs parameter only make sense when 
        JobGroup is completed. but it will be responsible the one who calls this 
        function to check jobGroup complete status
        """
        #self.beginTransaction()
        #print self.existingTransaction()
        job.changeStatus("Complete")
        #self.myThread.transaction.commit()
        #self.beginTransaction()
        if type(outputFiles) != list:
            outputFiles = [outputFiles]
        for file in outputFiles:
            # add file parentage and run lumi
            job.processSuccessfulJob(file)
            # add output file to group out put
            self.groupoutput.addFile(file)
        self.groupoutput.commit()
        #T0DO check whether it is needed to be committed for 
        #updating the job status
        status = self._recordFileStatusUponCompletion()                
        if status == "COMPLETE":
            output = self.output()
        else:
            output = False
        self.myThread.transaction.commit()
        #self.commitIfNew()
        return output
    
    def recordFail(self, job):
        """
        _recordFail_

        Mark a set of jobs that are associated with the job group as failed.
        If no list of jobs is passed in then mark all files in jobGroup 
        as failed.
        
        calling recordFail without jobs parameter only make sense when 
        JobGroup is failed. but it will be responsible the one who calls this 
        function to check jobGroup failed status
        """
        job.changeStatus("Failed")            
        status = self._recordFileStatusUponCompletion()
        self.myThread.transaction.commit()
        return status
    
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
    
    def isSuccessful(self):
        """
         _isSuccessful_
        
        Check all the jobs in the group are successfully completed.
        self.status can be used for this, but for the fast performance. 
        use this function
        """
        if self._completeStatus() == "COMPLETE":
            return True
        else:
            return False
        
    def _completeStatus(self):
        """
        _isSuccessful_
        
        Check all the jobs in the group are completed.
        self.status can be used for this, but for the fast performance. 
        use this function
        
        To: check query whether performance can be improved
        """
        statusAction = self.daofactory(classname = "JobGroup.IsComplete")
        all, cm, fa = statusAction.execute(self.id,
                                      conn = self.getReadDBConn(),
                                      transaction = self.existingTransaction())
        if all == cm:
            return "COMPLETE"
        elif all == cm + fa:
            return "FailComplete"
        elif fa > 0:
            return "FailIncomplete"
        else:
            return "Incomplete"
        
    def output(self):
        """
        The output is the files produced by the jobs in the group - these must
        be merged up together.
        getting output doesn't make sense if all the jobs are successful in the
        given job group. But it is the caller's responsibility to check where 
        the job group is successfully finished using isSuccessful() function
        """
        # output only makes sense if the group is completed
        # load output from DB 
        self.groupoutput.load()
        return self.groupoutput
        