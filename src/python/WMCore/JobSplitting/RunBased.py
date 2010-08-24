#!/usr/bin/env python
"""
_RunBased_

Run based splitting algorithm that will produce a set of jobs for files in a 
(complete) run. Produces files one run at a time (run:JobGroup) - if the algo
created jobs over more than one run would have confusion in the JobGroup 
creation and/or tracking.

If file spans a run will need to create a mask for that file.
"""
__revision__ = "$Id: RunBased.py,v 1.3 2008/10/23 15:58:30 jacksonj Exp $"
__version__  = "$Revision: 1.3 $"

from sets import Set
from WMCore.JobSplitting.JobFactory import JobFactory

class RunBased(JobFactory):
    def algorithm(self, job_instance = None, jobname = None, *args, **kwargs):
        """
        _algorithm_
        
        Implement run splitting algorithm. Assumes that if a file with a run is
        present, then all files for that run are also present.
        
        kwargs can take:
        files_per_job - e.g. 20 - Number of files per each split job
        """
        # Set default inputs if required
        if 'files_per_job' not in kwargs.keys():
            kwargs['files_per_job'] = 300
        logger = None
        dbf = None
        try:
            logger = self.subscription.logger
            dbf = self.subscription.dbfactory
        except:
            pass
        
        # Resulting job set
        jobs = Set()
        
        # Get the available Fileset
        fileset = list(self.subscription.availableFiles())
        
        # Select all primary files for the first present run
        curRun = None
        primaryFiles = []
        for f in fileset:
            # Check file doesn't span runs
            if len(f['runs']) != 1:
                msg = "File %s contains %s runs, should be 1" % \
                    (f['lfn'], len(f['runs']))
                raise RuntimeError, msg
            # Acquire run of interest if required
            elif not curRun:
                curRun = f['run']
                
            # Add file to primary interest list if contains current run
            if f['run'] == curRun:
                primaryFiles.add(f)
        
        # Create jobs for the available files, split by number of files
        num_files = len(primaryFiles)
        while num_files > 0:
            # Extract a subset of primary files
            jobFiles = Fileset(files=primaryFiles[:kwargs['files_per_job']])
            primaryFiles = primaryFiles[kwargs['files_per_job']:]
            num_files = num_files - len(jobFiles)

            # Create the job
            job = job_instance(name = '%s-%s' % (jobname, len(jobs) + 1), 
                               files=jobFiles, logger=logger, dbfactory=dbf)
            jobs.add(job)
        
        # Return the jobs
        return jobs
