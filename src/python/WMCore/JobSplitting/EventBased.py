#!/usr/bin/env python
"""
_EventBased_

Event based splitting algorithm that will chop a fileset into
a set of jobs based on event counts
"""

__revision__ = "$Id: EventBased.py,v 1.14 2009/10/26 16:26:01 mnorman Exp $"
__version__  = "$Revision: 1.14 $"

from sets import Set

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.Services.UUID import makeUUID

class EventBased(JobFactory):
    """
    Split jobs by number of events
    """
    def algorithm(self, *args, **kwargs):
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
        #baseName = makeUUID()


        #  //
        # // get the event total
        #//
        eventsPerJob = int(kwargs.get("events_per_job", 100))
        carryOver = 0

        

        for location in locationDict:
            fileList     = locationDict[location]
            currentEvent = 0
            # A group per location...
            self.newGroup()
            baseName = makeUUID()
            
            for f in fileList:
                eventsInFile = f['events']
                self.subscription.acquireFiles(f)

                if eventsInFile >= eventsPerJob:
                    currentEvent   = 0
                    while currentEvent < eventsInFile:
                        self.newJob(name = '%s-%i' % (baseName, len(self.currentGroup.newjobs)))
                        self.currentJob.addFile(f)
                        self.currentJob["mask"].setMaxAndSkipEvents(eventsPerJob, currentEvent)
                        currentEvent += eventsPerJob
                    currentEvent = 0
                else:
                    if currentEvent + eventsInFile > eventsPerJob:
                        #Create new jobs, because we is out of room
                        self.currentJob["mask"].setMaxAndSkipEvents(eventsPerJob, 0)
                        self.newJob(name = '%s-%s' % (baseName, len(self.currentGroup.newjobs)))
                        currentEvent = 0

                    if currentEvent + eventsInFile <= eventsPerJob:
                        #Add if it will be smaller
                        if self.currentJob:
                            self.currentJob.addFile(f)
                        else:
                            self.newJob(name = '%s-%s' % (baseName, len(self.currentGroup.newjobs)))
                            self.currentJob.addFile(f)
                        currentEvent += eventsInFile

            if currentEvent > 0:
                self.currentJob["mask"].setMaxAndSkipEvents(eventsPerJob, 0)
