#!/usr/bin/env python
"""
_RunBased_

Run based splitting algorithm that will produce a set of jobs for files in a 
(complete) run. Produces files one run at a time (run:JobGroup) - if the algo
created jobs over more than one run would have confusion in the JobGroup 
creation and/or tracking.

If file spans a run will need to create a mask for that file.
"""
__revision__ = "$Id: RunBased.py,v 1.1 2008/10/22 13:26:55 metson Exp $"
__version__  = "$Revision: 1.1 $"



from sets import Set
from WMCore.JobSplitting.JobFactory import JobFactory

class RunBased(JobFactory):
    def algorithm(self, job_instance = None, jobname = None, *args, **kwargs):
        jobs = Set() # Empty set to populate
        
        # The algorithm
        
        
        # Return the jobs
        return jobs