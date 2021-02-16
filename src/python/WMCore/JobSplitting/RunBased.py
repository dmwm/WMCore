#!/usr/bin/env python
"""
_RunBased_

Run based splitting algorithm that will produce a set of jobs for files in a
(complete) run. Produces files one run at a time (run:JobGroup) - if the algo
created jobs over more than one run would have confusion in the JobGroup
creation and/or tracking.

If file spans a run will need to create a mask for that file.
"""




from builtins import range
from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.DataStructs.Fileset import Fileset
from WMCore.Services.UUIDLib import makeUUID

class RunBased(JobFactory):
    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        Implement run splitting algorithm. Assumes that if a file with a run is
        present, then all files for that run are also present.

        kwargs can take:
        files_per_job - e.g. 20 - Number of files per each split job
        """
        filesPerJob = int(kwargs.get("files_per_job", 300))
        requireRunClosed = bool(kwargs.get("require_run_closed", False))

        #baseName = makeUUID()

        # Select all primary files for the first present run
        curRun = None
        primaryFiles = []
        #The current objective of this code is to find all runs in
        #a fileset, and then for each run, create a jobGroup
        #in each jobGroup have a list of jobs containing all the
        #files for that run.
        #If files have more then one run, sort that file with
        #the lowest run
        #In future, mask these files?

        runDict = {}

        locationDict = self.sortByLocation()

        for location in locationDict:
            fileList = locationDict[location]
            for f in fileList:

                #If it is a WMBS object, load all data
                if hasattr(f, "loadData"):
                    f.loadData()

                #Die if there are no runs
                if len(f['runs']) < 1:
                    msg = "File %s claims to contain %s runs!" %(f['lfn'], len(f['runs']))
                    raise RuntimeError(msg)

                #First we need to pick the lowest run
                runList = []
                for r in f['runs']:
                    runList.append(r.run)

                run = min(runList)

                #If we don't have the run, we need to add it
                if run not in runDict:
                    runDict[run] = []

                runDict[run].append(f)


            for run in runDict:
                #Find the runs in the dictionary we assembled and split the files in them

                self.newGroup()
                baseName = makeUUID()

                #Now split them into sections according to files per job
                while len(runDict[run]) > 0:
                    jobFiles = []
                    for i in range(filesPerJob):
                        #Watch out if your last job has less then the full number of files
                        if len(runDict[run]) > 0:
                            jobFiles.append(runDict[run].pop())

                    # Create the job
                    currentJob = self.newJob('%s-%s' % (baseName, len(self.currentGroup.newjobs)),
                                             files = jobFiles)
