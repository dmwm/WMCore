#!/usr/bin/env python
"""
_EventBased_

Event based splitting algorithm that will chop a fileset into
a set of jobs based on event counts
"""

__revision__ = "$Id: FileBased.py,v 1.15 2009/08/26 14:16:31 sfoulkes Exp $"
__version__  = "$Revision: 1.15 $"

from sets import Set
from sets import ImmutableSet
import logging
import threading

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.Services.UUID import makeUUID

from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.File    import File

class FileBased(JobFactory):
    """
    Split jobs by number of files.
    """
    def algorithm(self, groupInstance = None, jobInstance = None, *args,
                  **kwargs):
        """
        _algorithm_

        Split up all the available files such that each job will process a
        maximum of 'files_per_job'.  If the 'files_per_job' parameters is not
        passed in jobs will process a maximum of 10 files.
        """

        myThread = threading.currentThread()
        
        filesPerJob  = int(kwargs.get("files_per_job", 10))
        filesInJob   = 0
        jobs         = []
        listOfFiles  = []
        jobGroupList = []
        baseName     = makeUUID()

        #Get a dictionary of sites, files
        locationDict = self.sortByLocation()

        for location in locationDict.keys():
            #Now we have all the files in a certain location
            fileList   = locationDict[location]
            filesInJob = 0
            jobs       = []
            if len(fileList) == 0:
                continue
            for file in fileList:
                if filesInJob == 0 or filesInJob == filesPerJob:
                    job = jobInstance(name = "%s-%s" % (baseName, len(jobs) + 1))
                    jobs.append(job)
                    filesInJob = 0
                    
                filesInJob += 1
                job.addFile(file)
                listOfFiles.append(file)

            logging.info('I have %i jobs for location %s' %(len(jobs), location))
            
            jobGroup = groupInstance(subscription = self.subscription)
            jobGroup.add(jobs)

            jobGroup.commit()
            jobGroupList.append(jobGroup)

        #We need here to acquire all the files we have assigned to jobs
        self.subscription.acquireFiles(files = listOfFiles)

        return jobGroupList
        
        

 
