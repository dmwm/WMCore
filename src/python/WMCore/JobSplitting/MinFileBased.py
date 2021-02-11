#!/usr/bin/env python
"""
_MinFileBased_

File based splitting algorithm that will chop a fileset into
a set of jobs based on file boundaries.  Requires the minimum number
of files to create a new job.  If you do not have the necessary
number of files, it will not create any jobs UNLESS the
fileset is closed.
"""

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.WMBS.File               import File

class MinFileBased(JobFactory):
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

        # Grab the fileset
        fileset = self.subscription.getFileset()
        try:
            fileset.load()
        except AttributeError as ae:
            pass

        #Get a dictionary of sites, files
        locationDict = self.sortByLocation()

        for location in locationDict:
            #Now we have all the files in a certain location
            fileList    = locationDict[location]
            filesInJob  = 0
            jobsInGroup = 0
            fileCounter = 0
            if len(fileList) == 0:
                #No files for this location
                #This isn't supposed to happen, but better safe then sorry
                continue
            if len(fileList) < filesPerJob and fileset.open:
                continue
            self.newGroup()
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

                    filesInJob   = 0
                    # We've finished a job.  Should we create another one?
                    if len(fileList) - fileCounter < filesPerJob and fileset.open:
                        # If we don't have the full job's worth of files
                        # and the fileset is still open
                        # then we shouldn't create a new job
                        continue
                    self.newJob(name = self.getJobName())

                    jobsInGroup += 1
                    jobRun       = fileRun

                filesInJob += 1
                self.currentJob.addFile(f)
                fileCounter += 1

                listOfFiles.append(f)

        return
