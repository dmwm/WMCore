#!/usr/bin/env python
"""
_EventBased_

Event based splitting algorithm that will chop a fileset into
a set of jobs based on event counts
"""

__revision__ = "$Id: FileBased.py,v 1.19 2009/09/30 12:30:54 metson Exp $"
__version__  = "$Revision: 1.19 $"

from sets import Set
from sets import ImmutableSet
import logging
import threading
import time

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.Services.UUID import makeUUID

from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.File    import File

class FileBased(JobFactory):
    """
    Split jobs by number of files.
    """

    def getJobName(self, length=None):
        return '%s-%s' %(makeUUID(), str(length))
        #return baseName+str(length+1)
    
    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        Split up all the available files such that each job will process a
        maximum of 'files_per_job'.  If the 'files_per_job' parameters is not
        passed in jobs will process a maximum of 10 files.
        """
        myThread = threading.currentThread()
        
        self.newGroup()
        
        filesPerJob  = int(kwargs.get("files_per_job", 10))
        filesInJob   = 0
        totalJobs    = 0 
        jobs         = []
        listOfFiles  = []

        #Get a dictionary of sites, files
        locationDict = self.sortByLocation()

        for location in locationDict.keys():
            #Now we have all the files in a certain location
            fileList   = locationDict[location]
            filesInJob = 0
            if len(fileList) == 0:
                continue
            for file in fileList:
                if filesInJob == 0 or filesInJob == filesPerJob:
                    self.newJob(name = self.getJobName(length=filesInJob))
                    totalJobs += 1
                    filesInJob = 0
                    
                filesInJob += 1
                self.currentJob.addFile(file)
                
                listOfFiles.append(file)

        #We need here to acquire all the files we have assigned to jobs
        self.subscription.acquireFiles(files = listOfFiles)