from sets import Set
from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.File import File 
import datetime

class FileBased(JobFactory):
    """
    Return a set of jobs split along file boundaries. Acquiring files from the 
    subscription is slow and needs some optimising.
    """
    def algorithm(self, job_instance=None, jobname=None, *args, **kwargs):
        start = datetime.datetime.now()
        if 'files_per_job' not in kwargs.keys():
            kwargs['files_per_job'] = 10
        jobs = Set()
        thesub = self.subscription
        logger = None 
        dbf = None 
        try:
            logger = thesub.logger
            dbf = thesub.dbfactory
        except:
            pass

        num_files = len(self.subscription.availableFiles())
        allfiles = list(thesub.acquireFiles(size=num_files))
        
        while num_files > 0:
            thefiles = Fileset(files=allfiles[:kwargs['files_per_job']])
            allfiles = allfiles[kwargs['files_per_job']:]
            job = job_instance(name = '%s-%s' % (jobname, len(jobs) +1), 
                               files=thefiles, logger=logger, dbfactory=dbf)
            jobs.add(job)
            num_files = num_files - len(thefiles)
        end = datetime.datetime.now()
        duration = end - start
        if logger:
        	self.logger.debug("FileBased algorithm completed in %s s" % duration)
        else:
        	print "FileBased algorithm completed in %s s" % duration
        return jobs
