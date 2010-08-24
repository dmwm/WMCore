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

__revision__ = "$Id: JobGroup.py,v 1.10 2008/12/18 15:10:03 sfoulkes Exp $"
__version__ = "$Revision: 1.10 $"

from WMCore.Database.Transaction import Transaction
from WMCore.DataStructs.JobGroup import JobGroup as WMJobGroup
from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.Subscription import Subscription
from WMCore.Services.UUID import makeUUID

from sets import Set
import threading

class JobGroup(WMJobGroup):
    """
    A group (set) of Jobs
    """
    def __init__(self, subscription = None, jobs=None, id = -1, uid = None):
        WMJobGroup.__init__(self, subscription=subscription, jobs = jobs)

        myThread = threading.currentThread()
        self.logger = myThread.logger
        self.dialect = myThread.dialect
        self.dbi = myThread.dbi
        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = self.logger,
                                     dbinterface = self.dbi)
                                        
        self.id = id
        if uid == None:
            self.uid = makeUUID()
        else:
            self.uid = uid
            
    def create(self):
        """
        Add the new jobgroup to WMBS, create the output Fileset object
        """
        self.groupoutput = Fileset(
                      name="output://%s_%s" % (self.subscription.name(), id))

        if not self.groupoutput.exists():
            self.groupoutput.create()

        action = self.daofactory(classname='JobGroup.New')
        self.id, self.uid = action.execute(self.uid, self.subscription["id"],
                                           self.groupoutput.id)

        return

    def delete(self):
        """
        Remove a jobgroup from WMBS
        """
        self.daofactory(classname='JobGroup.Delete').execute(id = self.id)

    def exists(self):
        """
        Does a jobgroup exist with id if id is not provided, use the uid, 
        return the id
        """
        if self.id != -1:
            action = self.daofactory(classname='JobGroup.ExistsByID')
            return action.execute(id = self.id)
        else:
            action = self.daofactory(classname='JobGroup.Exists')
            return action.execute(uid = self.uid)
    
    def load(self):
        """
        Load the JobGroup from the database
        """
        subID = self.daofactory(classname='JobGroup.LoadSubscription').execute(self.id)
        jobIDs = self.daofactory(classname='JobGroup.LoadJobs').execute(self.id)
        outputID = self.daofactory(classname='JobGroup.LoadOutput').execute(self.id)

        self.subscription = Subscription(id = subID)
        self.subscription.load()

        self.jobs.clear()
        for jobID in jobIDs:
            newJob = Job(id = jobID)
            newJob.load()
            self.jobs.add(newJob)

        self.groupoutput = Fileset(id = outputID)
        self.groupoutput.load(method = "Fileset.LoadFromID")
        return
        
    def commit(self):
        """
        _commit_

        Write any new jobs to the database, creating them in the database if
        necessary.
        """
        trans = Transaction(dbinterface = self.dbi)
        try:
            for j in self.newjobs:
                j.create(group=self)
                j.associateFiles()
            
            WMJobGroup.commit(self)
            trans.commit()
        except Exception, e:
            trans.rollback()
            raise e
    
    def status(self, detail=False):
        """
        The status of the job group is the sum of the status of all jobs in the
        group.
        
        The status of the jobs can be correctly inferred by comparing the start,
        complete and update times. The groups status is the sum of these 
        statuses.
        
        return: ACTIVE, COMPLETE, FAILED
        """        
        
        av, ac, fa, cm = \
                self.daofactory(classname='JobGroup.Status').execute(self.id)
    
        total = av + ac + fa + cm
        
        if total > 0:
            report = ''
            if detail:
                report = ' (av %s, ac %s, fa %s, cm %s)' % (av, ac, fa, cm)
            if cm == total:
                return 'COMPLETE%s' % report
            elif fa == total:
                return 'FAILED%s' % report
            else:
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
