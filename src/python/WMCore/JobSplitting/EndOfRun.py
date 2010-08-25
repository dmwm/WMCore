#!/usr/bin/env python
"""
_EndOfRun_

If a subscription's fileset is closed, make a job that will run over any available
files
"""

__revision__ = "$Id: EndOfRun.py,v 1.1 2009/07/22 19:42:00 meloam Exp $"
__version__  = "$Revision: 1.1 $"

from sets import Set

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.Services.UUID import makeUUID

class EndOfRun(JobFactory):
    """
    if a subscription's fileset is closed, pull all the available files into a
    new job
    """
    def algorithm(self, groupInstance = None, jobInstance = None, *args,
                  **kwargs):
        """
        _algorithm_

        An end-of-run job splitting algorithm, will return a job with all
        unacquired ("available") files if the fileset is marked as closed
        returns nothing otherwise
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
        
        if (not fileset.open):
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
