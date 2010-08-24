#!/usr/bin/env python
"""
_RunBased_

Run based splitting algorithm that will produce a set of jobs for files in a 
(complete) run. Produces files one run at a time (run:JobGroup) - if the algo
created jobs over more than one run would have confusion in the JobGroup 
creation and/or tracking.

If file spans a run will need to create a mask for that file.
"""
__revision__ = "$Id: RunBased.py,v 1.2 2008/10/23 11:04:17 jacksonj Exp $"
__version__  = "$Revision: 1.2 $"



from sets import Set
from re import match
from WMCore.JobSplitting.JobFactory import JobFactory

class RunBased(JobFactory):
    def algorithm(self, job_instance = None, jobname = None, *args, **kwargs):
        """
        _algorithm_
        
        Implement run splitting algorithm. Assumes that if a file with a run is
        present, then all files for that run are also present
        """
        # Set default inputs if required
        if 'files_per_job' not in kwargs.keys():
            kwargs['files_per_job'] = 50
        primaryRe = ".*"
        doPrimaryLinkage = False
        if 'primary_data_tier' in kwargs.keys():
            primaryRe = "^/store/data/.*/.*/%s/.*$" % kwargs['primary_data_tier']
            doPrimaryLinkage = True
        logger = None
        dbf = None
        try:
            logger = self.subscription.logger
            dbf = self.subscription.dbfactory
        except:
            pass
        
        # Resulting job set
        jobs = Set()
        
        # Get the available FileSet
        fileset = self.subscription.availableFiles()
        
        # Select all primary files for the first present run
        curRun = None
        primaryFiles = []
        for f in fileset:
            # Determine if this file is of primary interest
            if re.match(primaryRe, f['lfn']):
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
                    
        # Resolve secondary file linkages
        secFiles = {}
        if doPrimaryLinkage:
            for f in primaryFiles:
                secFiles[primaryFiles['lfn']] = []
        
        # Create jobs for the available files, split by number of files
        num_files = len(primaryFiles)
        while num_files > 0:
            thefiles = Fileset(files=primaryFiles[:kwargs['files_per_job']])
            primaryFiles = primaryFiles[kwargs['files_per_job']:]
            job = job_instance(name = '%s-%s' % (jobname, len(jobs) + 1), 
                               files=thefiles, logger=logger, dbfactory=dbf)
            jobs.add(job)
            num_files = num_files - len(thefiles)
        
        # Return the jobs
        return jobs