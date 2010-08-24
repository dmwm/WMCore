from sets import Set
from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.File import File 

class FileBased(JobFactory):
    """
    Return a set of jobs split along file boundaries
    """
    def algorithm(self, job_instance=None, jobname=None, *args, **kwargs):
        if 'files_per_job' not in kwargs.keys():
            kwargs['files_per_job'] = 10
        jobs = Set()
        thesub = self.subscription 
        
        while len(self.subscription.availableFiles()) > 0:
            thefiles = Fileset(files=thesub.acquireFiles(size=kwargs['files_per_job']))
            job = job_instance(subscription=thesub,
                               name = '%s-%s' % (jobname, len(jobs) +1), 
                               files=thefiles)
            jobs.add(job)
        return jobs
