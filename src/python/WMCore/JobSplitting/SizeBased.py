#!/usr/bin/env python
"""
_SizeBased_

Suze based splitting algorithm that will produce a set of jobs for each file,
or a a set of files for each job.

"""

__revision__ = "$Id: SizeBased.py,v 1.1 2009/08/06 16:51:54 mnorman Exp $"
__version__  = "$Revision: 1.1 $"

from sets import Set
import logging

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.DataStructs.Fileset import Fileset
from WMCore.Services.UUID import makeUUID

class SizeBased(JobFactory):
    def algorithm(self, groupInstance = None, jobInstance = None, *args,
                  **kwargs):
        """
        _algorithm_
        
        Implement size splitting algorithm.

        
        kwargs can take:
        size_per_job 
        """
        sizePerJob = kwargs.get("size_per_job", 1000)

        baseName = makeUUID()
        
        # Resulting job set
        jobs = []

        # List of Job Groups
        jobGroupList = []
        
        locationDict = self.sortByLocation()

        for location in locationDict.keys():
            fileList     = locationDict[location]
            jobs         = []
            currentSize = 0
            currentJob   = jobInstance(name = '%s-%s' % (baseName, len(jobs) + 1))



            for f in fileList:
                sizeOfFile = f['size']
                if sizeOfFile > sizePerJob:
                    logging.error("File %s is too big for a job!" %(f['lfn']))
                    continue
                self.subscription.acquireFiles(f)

                if currentSize + sizeOfFile > sizePerJob:
                    #Create new jobs, because we are out of room
                    jobs.append(currentJob)
                    currentJob   = jobInstance(name = '%s-%s' % (baseName, len(jobs) + 1))
                    currentSize = 0

                if currentSize + sizeOfFile <= sizePerJob:
                    #Add if it will be smaller
                    currentJob.addFile(f)
                    currentSize += sizeOfFile

            #If we have leftover files at the end of all files, create a job just for them
            if currentSize > 0:
                jobs.append(currentJob)

            #Create one jobGroup per location
            jobGroup = groupInstance(subscription = self.subscription)
            jobGroup.add(jobs)
            jobGroup.commit()
            jobGroupList.append(jobGroup)

        return jobGroupList

