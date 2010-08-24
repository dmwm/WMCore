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

__revision__ = "$Id: JobGroup.py,v 1.1 2008/09/12 17:07:19 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.DataStructs.JobGroup import JobGroup as WMJobGroup

class JobGroup(WMJobGroup):
    def __init__(self, subscription = None, jobs=Set(), id = -1):
        BusinessObject.__init__(self, 
                                logger=subscription.logger, 
                                dbfactory=subscription.dbfactory)
        WMJobGroup.__init__(self, subscription=subscription, jobs = jobs)
        self.id = id
        if self.id == -1:
            self.create()
        else:
            self.load()
    
    def add(self, job):  
        pass
    
    def status(self, detail=False):
        """
        The status of the job group is the sum of the status of all jobs in the
        group.
        
        return: ACTIVE, COMPLETE, FAILED
        """
        pass

    def recordAcquire(self, jobs):
        pass
            
    def recordComplete(self, jobs):
        pass
            
    def recordFail(self, jobs):
        pass
    
    def output(self):
        """
        The output is the files produced by the jobs in the group - these must
        be merged up together.
        """
        if self.status() == 'COMPLETE':
            "output only makes sense if the group is completed"
            "load output from DB" 
            return self._output
        print self.status(detail=True)
        return False
    
    def addOutput(self, file):
        pass