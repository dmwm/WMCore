#!/usr/bin/env python
"""
_JobGroup_

Definition of JobGroup:
    Set of jobs running on same input file for same Workflow
    Set of jobs for a single subscription
    Required for certain job splitting Algo's (.g. event split to make complete lumi)
    Subscription:JobGroup == 1:N
    JobGroup:Jobs = 1:N
    JobGroup:InFile = 1:1
    JobGroup:MergedOutFile = N:1
    JobGroup at least one Lumi section

"""

__revision__ = "$Id: JobGroup.py,v 1.1 2008/08/21 07:24:30 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.DataStructs.Pickleable import Pickleable
from sets import Set
import datetime

class JobGroup(Pickleable):
    """
    JobGroups are sets of jobs running on files who's output needs to be merged
    together.
    """
    dict = {}
    def __init__(self, jobs=Set()):
        """
        Store all the jobs as a set in self.dict
        """
        self.dict['jobs'] = jobs
        
    def status(self):
        """
        The status of the job group is the sum of the status of all jobs in the
        group.
        
        return: active, complete, failed
        """
        pass
    
    def output(self):
        """
        The output is the files produced by the jobs in the group - these must
        be merged up together.
        """
        if self.status() == 'complete':
            "output only makes sense if the group is completed"
            pass
        pass