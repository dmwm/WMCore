#!/usr/bin/env python
"""
_FileBased_

File based splitting algorithm that will chop a fileset into
a set of jobs based on file boundaries
"""

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.WMBS.File               import File

class FileBased(JobFactory):
    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        Split up all the available files such that each job will process a
        maximum of 'files_per_job'.  If the 'files_per_job' parameters is not
        passed in jobs will process a maximum of 10 files.
        """

        filesPerJob   = int(kwargs.get("files_per_job", 10))
        jobsPerGroup  = int(kwargs.get("jobs_per_group", 0))
        runBoundaries = kwargs.get("respect_run_boundaries", False)
        getParents    = kwargs.get("include_parents", False)
        filesInJob    = 0
        listOfFiles   = []
        timePerEvent, sizePerEvent, memoryRequirement = \
                    self.getPerformanceParameters(kwargs.get('performance', {}))

        #Get a dictionary of sites, files
        locationDict = self.sortByLocation()

        for location in locationDict.keys():
            #Now we have all the files in a certain location
            fileList    = locationDict[location]
            filesInJob  = 0
            jobsInGroup = 0
            self.newGroup()
            if len(fileList) == 0:
                #No files for this location
                #This isn't supposed to happen, but better safe then sorry
                continue
            jobRun = None
            for f in fileList:
                if getParents:
                    parentLFNs = self.findParent(lfn = f['lfn'])
                    for lfn in parentLFNs:
                        parent = File(lfn = lfn)
                        f['parents'].add(parent)
                fileRun = f.get('minrun', None)
                if filesInJob == 0 or filesInJob == filesPerJob or (runBoundaries and fileRun != jobRun):
                    if jobsPerGroup:
                        if jobsInGroup > jobsPerGroup:
                            self.newGroup()
                            jobsInGroup = 0

                    self.newJob(name = self.getJobName())
                    self.currentJob.addResourceEstimates(memory = memoryRequirement)

                    filesInJob   = 0
                    jobsInGroup += 1
                    jobRun       = fileRun

                filesInJob += 1
                self.currentJob.addFile(f)
                fileTime = f['events'] * timePerEvent
                fileSize = f['events'] * sizePerEvent
                self.currentJob.addResourceEstimates(jobTime = fileTime,
                                                     disk = fileSize)

                listOfFiles.append(f)

        return
