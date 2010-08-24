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

__revision__ = "$Id: JobGroup.py,v 1.5 2008/10/28 14:47:15 sfoulkes Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.WMBS.BusinessObject import BusinessObject
from WMCore.DataStructs.JobGroup import JobGroup as WMJobGroup
from WMCore.WMBS.Fileset import Fileset

from sets import Set

class JobGroup(WMJobGroup, BusinessObject):
    """
    A gropu (set) of Jobs
    """
    def __init__(self, subscription = None, jobs=Set(), id = -1):
        BusinessObject.__init__(self, 
                                logger=subscription.logger, 
                                dbfactory=subscription.dbfactory)
        WMJobGroup.__init__(self, subscription=subscription, jobs = jobs)
        self.id = id
        if self.id <= 0:
            self.logger.debug("Creating WMBS JobGroup")
            self.create()
        else:
            self.load()
            
    def create(self):
        """
        Add the new jobgroup to WMBS, create the output Fileset object
        """
        action = self.daofactory(classname='JobGroup.New')
        self.id, self.uid = action.execute(self.subscription.id)
        self.groupoutput = Fileset(
                      name="output://%s_%s" % (self.subscription.name(), id),
                      logger=self.logger, 
                      dbfactory=self.dbfactory)
        if not self.groupoutput.exists():
            self.groupoutput.create()
        return self
    
    def load(self):
        """
        Load the JobGroup from the database
        """
        self.daofactory(classname='JobGroup.Load').execute(self.id)
        id = self.daofactory(classname='JobGroup.Output').execute(self.id)
        self.groupoutput = Fileset(id = id,
                              logger=self.subscription.logger, 
                              dbfactory=self.subscription.dbfactory)
        self.groupoutput.populate()
        return self
        
    def add(self, job):
        """
        Input must be (subclasses of) WMBS jobs. Input may be a list or set as 
        well as single jobs.
        """
        # Iterate through all the jobs in the group
        for j in job:
            j.create(group=self.id)
            j.associateFiles()
            
        self.jobs = self.jobs | self.makeset(job)
    
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
            self.groupoutput.populate()
            return self.groupoutput
        self.logger.debug(self.status(detail=True))
        return False
