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

__revision__ = "$Id: JobGroup.py,v 1.16 2009/05/12 16:20:20 sfoulkes Exp $"
__version__ = "$Revision: 1.16 $"

from WMCore.DataStructs.WMObject import WMObject
from WMCore.DataStructs.Fileset import Fileset

import datetime

class JobGroup(WMObject):
    """
    JobGroups are sets of jobs running on files who's output needs to be merged
    together.
    """
    def __init__(self, subscription = None, jobs = None):
        self.jobs = []
        self.newjobs = []

        if type(jobs) == list:
            self.newjobs = jobs
        elif jobs != None:
            self.newjobs = [jobs]
            
        self.subscription = subscription
        self.output = Fileset()
        self.last_update = datetime.datetime.now()
        
    def add(self, job):
        """
        _add_

        Add a Job to the JobGroup.
        """
        self.newjobs.append(job)
        return

    def commit(self):
        """
        _commit_

        Move any jobs in the newjobs dict to the job dict.  Empty the newjobs
        dict.
        """
        self.jobs.extend(self.newjobs)
        self.newjobs = []
    
    def addOutput(self, file):
        """
        _addOutput_

        Add a File to the JobGroup's output fileset.  The File is committed
        to the Fileset immediately.
        """
        self.output.addFile(file)
        self.output.commit()

    def getJobs(self, type = "list"):
        """
        _getJobs_

        Retrieve all of the jobs in the JobGroup.  The output will either be
        returned as a list of Job objects (when type is "list") or a list of
        Job IDs (when type is "id").
        """
        if type == "list":
            return self.jobs
        elif type == "id":
            jobIDs = []

            for job in self.jobs:
                jobIDs.append(job["id"])

            return jobIDs
        else:
            print "Unknown type: %s" % type

        return

    def getOutput(self, type = "list"):
        """
        _getOutput_

        Retrieve all of the files that are in the JobGroup's output fileset.
        Type can be one of the following: list, set, lfn, id.
        """
        return self.output.getFiles(type = type)

    def __len__(self):
        return len(self.jobs.keys()) + len(self.newjobs.keys())
