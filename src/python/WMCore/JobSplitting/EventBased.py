#!/usr/bin/env python
"""
_EventBased_

Event based splitting algorithm that will chop a fileset into
a set of jobs based on event counts
"""

__revision__ = "$Id: EventBased.py,v 1.12 2009/08/06 16:49:03 mnorman Exp $"
__version__  = "$Revision: 1.12 $"

from sets import Set

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.Services.UUID import makeUUID

class EventBased(JobFactory):
    """
    Split jobs by number of events
    """
    def algorithm(self, groupInstance = None, jobInstance = None, *args,
                  **kwargs):
        """
        _algorithm_

        An event base splitting algorithm.  All available files are split into a
        set number of events per job.  
        """
       
        #  //
        # // Resulting job set (shouldnt this be a JobGroup??)
        #//
        jobs         = []
        jobGroupList = []

        #Get a dictionary of sites, files
        locationDict = self.sortByLocation()



        #  //
        # // get the event total
        #//
        eventsPerJob = int(kwargs.get("events_per_job", 100))
        carryOver = 0

        baseName = makeUUID()

        for location in locationDict:
            fileList     = locationDict[location]
            jobs         = []
            currentEvent = 0
            currentJob   = jobInstance(name = '%s-%s' % (baseName, len(jobs) + 1))

            for f in fileList:
                eventsInFile = f['events']
                self.subscription.acquireFiles(f)

                if eventsInFile >= eventsPerJob:
                    currentEvent   = 0
                    while currentEvent < eventsInFile:
                        currentJob = jobInstance(name = '%s-%s' % (baseName, len(jobs) + 1))
                        currentJob.addFile(f)
                        currentJob["mask"].setMaxAndSkipEvents(eventsPerJob, currentEvent)
                        jobs.append(currentJob)
                        currentEvent += eventsPerJob
                    currentEvent = 0
                else:
                    if currentEvent + eventsInFile > eventsPerJob:
                        #Create new jobs, because we is out of room
                        currentJob["mask"].setMaxAndSkipEvents(eventsPerJob, 0)
                        jobs.append(currentJob)
                        currentJob   = jobInstance(name = '%s-%s' % (baseName, len(jobs) + 1))
                        currentEvent = 0

                    if currentEvent + eventsInFile <= eventsPerJob:
                        #Add if it will be smaller
                        currentJob.addFile(f)
                        currentEvent += eventsInFile

            if currentEvent > 0:
                currentJob["mask"].setMaxAndSkipEvents(eventsPerJob, 0)
                jobs.append(currentJob)


            jobGroup = groupInstance(subscription = self.subscription)
            jobGroup.add(jobs)
            jobGroup.commit()
            jobGroupList.append(jobGroup)

        return jobGroupList
