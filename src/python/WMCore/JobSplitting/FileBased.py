#!/usr/bin/env python
"""
_FileBased_

File based splitting algorithm that will chop a fileset into
a set of jobs based on file boundaries
"""

__revision__ = "$Id: FileBased.py,v 1.25 2010/03/12 20:29:52 mnorman Exp $"
__version__  = "$Revision: 1.25 $"

import threading

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.Services.UUID import makeUUID

class FileBased(JobFactory):
    """
    Split jobs by number of files.
    """

    def getJobName(self, baseName = None, length=None):
        """
        Create standard job name

        """
        if not baseName:
            baseName = makeUUID()
        return '%s-%s' % (baseName, str(length))
    
    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        Split up all the available files such that each job will process a
        maximum of 'files_per_job'.  If the 'files_per_job' parameters is not
        passed in jobs will process a maximum of 10 files.
        """
        myThread = threading.currentThread()
        
        filesPerJob  = int(kwargs.get("files_per_job", 10))
        jobsPerGroup = int(kwargs.get("jobs_per_group", 0))
        filesInJob   = 0
        totalJobs    = 0
        listOfFiles  = []

        #Get a dictionary of sites, files
        locationDict = self.sortByLocation()

        for location in locationDict.keys():
            #Now we have all the files in a certain location
            fileList    = locationDict[location]
            filesInJob  = 0
            totalJobs   = 0
            jobsInGroup = 0
            self.newGroup()
            baseName = makeUUID()
            if len(fileList) == 0:
                #No files for this location
                #This isn't supposed to happen, but better safe then sorry
                continue
            for file in fileList:
                if filesInJob == 0 or filesInJob == filesPerJob:
                    if jobsPerGroup:
                        if jobsInGroup > jobsPerGroup:
                            self.newGroup()
                            jobsInGroup = 0

                    self.newJob(name = self.getJobName(baseName = baseName, \
                                                       length=totalJobs))
                    filesInJob   = 0
                    totalJobs   += 1
                    jobsInGroup += 1
                    
                filesInJob += 1
                self.currentJob.addFile(file)
                
                listOfFiles.append(file)

        return
