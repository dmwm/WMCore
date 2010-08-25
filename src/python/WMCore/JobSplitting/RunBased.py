#!/usr/bin/env python
"""
_RunBased_

Run based splitting algorithm that will produce a set of jobs for files in a 
(complete) run. Produces files one run at a time (run:JobGroup) - if the algo
created jobs over more than one run would have confusion in the JobGroup 
creation and/or tracking.

If file spans a run will need to create a mask for that file.
"""

__revision__ = "$Id: RunBased.py,v 1.14 2009/05/28 17:00:33 sfoulkes Exp $"
__version__  = "$Revision: 1.14 $"

from sets import Set

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.DataStructs.Fileset import Fileset
from WMCore.Services.UUID import makeUUID

class RunBased(JobFactory):
    def algorithm(self, groupInstance = None, jobInstance = None, *args,
                  **kwargs):
        """
        _algorithm_
        
        Implement run splitting algorithm. Assumes that if a file with a run is
        present, then all files for that run are also present.
        
        kwargs can take:
        files_per_job - e.g. 20 - Number of files per each split job
        """
        filesPerJob = kwargs.get("files_per_job", 300)
        requireRunClosed = kwargs.get("require_run_closed", False)

        baseName = makeUUID()
        
        # Resulting job set
        jobs = []
        
        # Get the available Fileset
        fileset = list(self.subscription.availableFiles())
        
        # Select all primary files for the first present run
        curRun = None
        primaryFiles = []
        for f in fileset:
            # Check file doesn't span runs
            # In future, mask this file?
            if len(f['runs']) != 1:
                msg = "File %s contains %s runs, should be 1" % \
                    (f['lfn'], len(f['runs']))
                raise RuntimeError, msg
            # Acquire run of interest if required
            elif not curRun:
                for run in f['runs']:
                    curRun = run.run
                
            # Add file to primary interest list if contains current run
            for run in f['runs']:
                if run.run == curRun:
                    primaryFiles.append(f)
        
        # Create jobs for the available files, split by number of files
        num_files = len(primaryFiles)
        while num_files > 0:
            # Extract a subset of primary files
            jobFiles = Fileset(files=primaryFiles[:filesPerJob])
            primaryFiles = primaryFiles[filesPerJob:]
            num_files = num_files - len(jobFiles)

            # Create the job
            job = jobInstance(name = '%s-%s' % (baseName, len(jobs) + 1),
                              files = jobFiles)
            jobs.append(job)
        
        jobGroup = groupInstance(subscription = self.subscription)
        jobGroup.add(jobs)
        jobGroup.commit()

        return jobGroup
