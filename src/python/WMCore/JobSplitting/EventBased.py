#!/usr/bin/env python
"""
_EventBased_

Event based splitting algorithm that will chop a fileset into
a set of jobs based on event counts
"""

__revision__ = "$Id: EventBased.py,v 1.17 2010/03/11 21:03:55 sfoulkes Exp $"
__version__  = "$Revision: 1.17 $"

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
        eventsPerJob = int(kwargs.get("events_per_job", 100))
        
        locationDict = self.sortByLocation()
        for location in locationDict:
            self.newGroup() 
            baseName = makeUUID()           
            fileList = locationDict[location]
        
            for f in fileList:
                currentEvent = 0
                eventsInFile = f['events']

                if eventsInFile >= eventsPerJob:
                    while currentEvent < eventsInFile:
                        self.newJob(name = '%s-%i' % (baseName, len(self.currentGroup.newjobs)))
                        self.currentJob.addFile(f)
                        self.currentJob["mask"].setMaxAndSkipEvents(eventsPerJob, currentEvent)
                        currentEvent += eventsPerJob
                else:
                    self.newJob(name = '%s-%s' % (baseName, len(self.currentGroup.newjobs)))
                    self.currentJob.addFile(f)
                    self.currentJob["mask"].setMaxAndSkipEvents(eventsPerJob, 0)                    
