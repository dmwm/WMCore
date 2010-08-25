#!/usr/bin/env python
"""
_FixedDelay_

If the current time is beyond trigger_time, add
any unacquired jobs to 
"""

__revision__ = "$Id: FixedDelay.py,v 1.1 2009/07/22 21:25:51 meloam Exp $"
__version__  = "$Revision: 1.1 $"

from sets import Set

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.Services.UUID import makeUUID
import time

class FixedDelay(JobFactory):
    """
    if the current time is past trigger_time, 
    pull all the available files into a
    new job
    """
    def algorithm(self, groupInstance = None, jobInstance = None, *args,
                  **kwargs):
        """
        _algorithm_

        A time delay job splitting algorithm, will shove all unacquired
        files into a new job if the trigger_time has been passed
        """
       
        #  //
        # // Resulting job set (shouldnt this be a JobGroup??)
        #//
        #jobs = Set()
        jobs = []

        #  //
        # // get the fileset
        #//
        fileset = self.subscription.getFileset()
        trigger_time = kwargs['trigger_time']
        if (trigger_time < time.gmtime()):
            availFiles = self.subscription.availableFiles()
                
            baseName = makeUUID()
            currentJob = jobInstance(name = '%s-endofrun' % (baseName,))
            
            if (len(availFiles) == 0):
                # no files to acquire
                return []
            
            for f in availFiles:                    
                currentJob.addFile(f)
                    
            jobs.append(currentJob)
            jobGroup = groupInstance(subscription = self.subscription)
            jobGroup.add(jobs)
            jobGroup.commit()
            #jobGroup.recordAcquire(list(jobs))
            return [jobGroup]
        else:
            return []

 