from sets import Set
from WMCore.JobSplitting.JobFactory import JobFactory

class FileBased(JobFactory):
    """
    Return a set of jobs split along file boundaries
    """
    def algorithm(self, job_instance=None, *args, **kwargs):
        if 'files_per_job' not in kwargs.keys():
            kwargs['files_per_job'] = 10
        jobs = Set()
        while len(self.subscription.availableFiles()) > 0:
            job = job_instance(self.subscription, self.subscription.acquireFiles(size=kwargs['files_per_job']))
            jobs.add(job)
        return jobs
