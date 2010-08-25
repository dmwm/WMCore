#!/usr/bin/env python
"""
_FileBased_

File based splitting algorithm that will chop a fileset into
a set of jobs based on file boundaries
"""

__revision__ = "$Id: FileBased.py,v 1.24 2010/02/12 20:17:29 sfoulkes Exp $"
__version__  = "$Revision: 1.24 $"

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
        filesInJob   = 0
        totalJobs    = 0
        listOfFiles  = []

        #Get a dictionary of sites, files
        locationDict = self.sortByLocation()

        for location in locationDict.keys():
            #Now we have all the files in a certain location
            fileList   = locationDict[location]
            filesInJob = 0
            totalJobs  = 0
            self.newGroup()
            baseName = makeUUID()
            if len(fileList) == 0:
                #No files for this location
                #This isn't supposed to happen, but better safe then sorry
                continue
            for file in fileList:
                file.loadData(parentage = 1)
                if filesInJob == 0 or filesInJob == filesPerJob:
                    self.newJob(name = self.getJobName(baseName = baseName, \
                                                       length=totalJobs))
                    filesInJob = 0
                    totalJobs += 1
                    
                filesInJob += 1
                self.currentJob.addFile(file)
                
                listOfFiles.append(file)

        return
