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
"""

__revision__ = "$Id: JobGroup.py,v 1.11 2008/12/18 15:11:20 sfoulkes Exp $"
__version__ = "$Revision: 1.11 $"

from WMCore.DataStructs.Pickleable import Pickleable
from WMCore.DataStructs.Fileset import Fileset
from sets import Set
import datetime

class JobGroup(Pickleable):
    """
    JobGroups are sets of jobs running on files who's output needs to be merged
    together.
    """
    def __init__(self, subscription = None, jobs = None):
        self.jobs = Set()

        if jobs == None:
            self.newjobs = Set()
        else:
            self.newjobs = jobs
            
        self.subscription = subscription
        self.groupoutput = Fileset()
        self.last_update = datetime.datetime.now()
        self.id = self.last_update.__hash__() 
        self.uid = 0
        
    def add(self, job):        
        self.newjobs = self.newjobs | self.makeset(job)

    def commit(self):
        """
        _commit_

        Move any new jobs to the jobs attribute, and empty the newjobs
        attribute.
        """
        self.jobs = self.jobs | self.newjobs
        self.newjobs = Set()
    
    def __len__(self):
        return len(self.jobs) + len(self.newjobs)
    
    def status(self, detail=False):
        """
        The status of the job group is the sum of the status of all jobs in the
        group.
        
        return: ACTIVE, COMPLETE, FAILED
        """
        complete = []
        failed = []
        activated = []
        for j in (self.jobs | self.newjobs):
            if j.last_update < self.last_update:
                # job has been updated
                if j.status == 'ACTIVE':
                    activated.append(j)
                elif j.status == 'FAILED':
                    failed.append(j)
                elif j.status == 'COMPLETE':
                    complete.append(j)
        self.recordAcquire(activated)
        self.recordComplete(complete)
        self.recordFail(failed)
        
        self.last_update = datetime.datetime.now()
    
        ac = len(activated)
        fa = len(failed)
        cm = len(complete)
        av = len(self.jobs) - ac - fa - cm  
    
        total = av + ac + fa + cm
        report = ''
        if detail:
            report = ' (av %s, ac %s, fa %s, cm %s)' % (av, ac, fa, cm)
        if cm == total:
            return 'COMPLETE%s' % report
        elif fa == total:
            return 'FAILED%s' % report
        else:
            return 'ACTIVE%s' % report
     
    def recordAcquire(self, jobs):
        jobs = self.makelist(jobs)
        for j in jobs:
            self.subscription.acquireFiles(j.listFiles())
            
    def recordComplete(self, jobs):
        jobs = self.makelist(jobs)
        for j in jobs:
            self.subscription.completeFiles(j.listFiles())
            
    def recordFail(self, jobs):
        jobs = self.makelist(jobs)
        for j in jobs:
            self.subscription.failFiles(j.listFiles())
    
    def output(self):
        """
        The output is the files produced by the jobs in the group - these must
        be merged up together.
        """
        if self.status() == 'COMPLETE':
            "output only makes sense if the group is completed"
            for j in self.jobs:
                self.addOutput(j.output.listFiles())
            return self.groupoutput
        print self.status(detail=True)
        return False
    
    def addOutput(self, file):
        self.groupoutput.addFile(file)
