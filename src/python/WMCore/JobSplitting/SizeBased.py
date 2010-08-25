#!/usr/bin/env python
"""
_SizeBased_

Suze based splitting algorithm that will produce a set of jobs for each file,
or a a set of files for each job.

"""

__revision__ = "$Id: SizeBased.py,v 1.2 2009/09/30 12:30:54 metson Exp $"
__version__  = "$Revision: 1.2 $"

from sets import Set
import logging

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.DataStructs.Fileset import Fileset
from WMCore.Services.UUID import makeUUID

class SizeBased(JobFactory):
    def algorithm(self, *args, **kwargs):
        """
        _algorithm_
        
        Implement size splitting algorithm.

        
        kwargs can take:
        size_per_job 
        """
        sizePerJob = kwargs.get("size_per_job", 1000)

        baseName = makeUUID()
                
        locationDict = self.sortByLocation()

        for location in locationDict.keys():
            self.newGroup()
            fileList     = locationDict[location]
            currentSize = 0
            
            for f in fileList:
                sizeOfFile = f['size']
                if sizeOfFile > sizePerJob:
                    logging.error("File %s is too big for a job!" %(f['lfn']))
                    continue
                self.subscription.acquireFiles(f)
                
                if currentSize + sizeOfFile > sizePerJob:
                    #Create new jobs, because we are out of room
                    self.newJob(name = '%s-%s' % (baseName, len(self.currentGroup.jobs) + 1))
                    currentSize = 0

                if currentSize + sizeOfFile <= sizePerJob:
                    
                    if not self.currentJob:
                        self.newJob(name = '%s-%s' % (baseName, len(self.currentGroup.jobs) + 1))
                    #Add if it will be smaller
                    self.currentJob.addFile(f)
                    currentSize += sizeOfFile

