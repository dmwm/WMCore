from sets import Set
from WMCore.JobSplitting.JobFactory import JobFactory

class EventBased(JobFactory):
    """
    Split jobs by number of events
    """
    def algorithm(self, job_instance=None, *args, **kwargs):
        jobs = Set()
        #### Algorithm here
        kwargs['total_number_of_events']
        if kwargs['events_per_job']:
            # make jobs with kwargs['events_per_job'] events per job
            pass
        elif kwargs['number_of_jobs']:
            # make kwargs['number_of_jobs'] jobs, with 
            #kwargs['total_number_of_events'] / kwargs['number_of_jobs'] events per job
            pass