#!/usr/bin/env python
"""
_FileBased_

File based splitting algorithm that will chop a fileset into
a set of jobs based on file boundaries
"""

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.WMBS.File import File


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
        totalFiles    = int(kwargs.get("total_files", 0))
        runBoundaries = kwargs.get("respect_run_boundaries", False)
        getParents    = kwargs.get("include_parents", False)
        filesInJob    = 0
        listOfFiles   = []
        timePerEvent, sizePerEvent, memoryRequirement = \
                    self.getPerformanceParameters(kwargs.get('performance', {}))

        #Get a dictionary of sites, files
        locationDict = self.sortByLocation()

        ## Make a list with all the files in the locationDict.
        files = []
        for filesPerLocSet in locationDict.values():
            for file in filesPerLocSet:
                files.append(file)
        ## Here we can apply a lumi-mask and remove files 
        ## that are left with 0 lumis to process.
        ## Sort the list of files by LFN.
        if len(files) != 0:
            files = sorted(files, key = lambda f: f['lfn'])
        ## Keep only the first totalFiles files and remove
        ## the other files from the locationDict.
        if totalFiles > 0 and totalFiles < len(files):
            removedFiles = files[totalFiles:]
            files = files[:totalFiles]
            for file in removedFiles:
                for locSet in locationDict.keys():
                    if file in locationDict[locSet]:
                        locationDict[locSet].remove(file)

        for locSet in locationDict.keys():
            #Now we have all the files in a certain location set
            fileList    = locationDict[locSet]
            filesInJob  = 0
            jobsInGroup = 0
            self.newGroup()
            if len(fileList) == 0:
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

                self.currentJob.addFile(f)
                filesInJob += 1
                fileTime = f['events'] * timePerEvent
                fileSize = f['events'] * sizePerEvent
                self.currentJob.addResourceEstimates(jobTime = fileTime,
                                                     disk = fileSize)
                listOfFiles.append(f)

        return
