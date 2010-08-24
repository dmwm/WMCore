#!/usr/bin/env python
#pylint: disable-msg=W0613
"""
_EventBased_

Event based splitting algorithm that will chop a fileset into
a set of jobs based on event counts

"""
__revision__ = "$Id: EventBased.py,v 1.4 2008/09/29 13:22:31 metson Exp $"
__version__  = "$Revision: 1.4 $"



from sets import Set
from WMCore.JobSplitting.JobFactory import JobFactory



class EventBased(JobFactory):
    """
    Split jobs by number of events
    """
    def algorithm(self, job_instance = None, jobname = None, *args, **kwargs):
        """
        _algorithm_

        Implement event based splitting algorithm

        """
        #  //
        # // Resulting job set (shouldnt this be a JobGroup??)
        #//
        jobs = Set()


        #  //
        # // get the fileset
        #//
        fileset = self.subscription.availableFiles()

        #  //
        # // get the event total
        #//
        eventsPerJob = kwargs['events_per_job']
        carryOver = 0
        currentJob = job_instance(subscription=self.subscription,
                                  name = '%s-%s' % (jobname, len(jobs) + 1))
        currentJob.mask.setMaxAndSkipEvents(eventsPerJob, 0)


        for f in fileset:
            eventsInFile = f.dict['events']

            #  //
            # // Take into account offset.
            #//
            startEvent = eventsPerJob - carryOver

            #  //Edge Effect:
            # // if start event is 0, we need to add this file
            #//  otherwise it will be picked up automatically
            if startEvent != 0:
                currentJob.addFile(f)
            #  //
            # // Keep creating job defs while accumulator is within
            #//  file event range
            accumulator = startEvent
            while accumulator < eventsInFile:
                jobs.add(currentJob)
                currentJob = job_instance(subscription = self.subscription)
                currentJob.addFile(f)
                currentJob.mask.setMaxAndSkipEvents(eventsPerJob, accumulator)
                accumulator += eventsPerJob

            #  //
            # // if there was a shortfall in the last job
            #//  pass it on to the next job
            accumulator -= eventsPerJob
            carryOver = eventsInFile - accumulator

        #  //
        # // remainder
        #//
        jobs.add(currentJob)

        return jobs


