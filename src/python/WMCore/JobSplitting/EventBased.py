#!/usr/bin/env python
"""
_EventBased_

Event based splitting algorithm that will chop a fileset into
a set of jobs based on event counts
"""


from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.WMBS.File               import File 

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
        getParents   = kwargs.get("include_parents", False) 
        totalJobs    = 0
        
        locationDict = self.sortByLocation()
        for location in locationDict:
            self.newGroup() 
            fileList = locationDict[location]
        
            for f in fileList:
                currentEvent = 0
                eventsInFile = f['events']

                if getParents: 
                    parentLFNs = self.findParent(lfn = f['lfn']) 
                    for lfn in parentLFNs: 
                        parent = File(lfn = lfn) 
                        f['parents'].add(parent) 

                if eventsInFile >= eventsPerJob:
                    while currentEvent < eventsInFile:
                        self.newJob(name = self.getJobName(length=totalJobs))
                        self.currentJob.addBaggageParameter("eventsPerJob", eventsPerJob)
                        self.currentJob.addFile(f)
                        self.currentJob["mask"].setMaxAndSkipEvents(eventsPerJob, currentEvent)
                        currentEvent += eventsPerJob
                        totalJobs    += 1
                else:
                    self.newJob(name = self.getJobName(length=totalJobs))
                    self.currentJob.addBaggageParameter("eventsPerJob", eventsPerJob)                    
                    self.currentJob.addFile(f)
                    self.currentJob["mask"].setMaxAndSkipEvents(f["events"], 0)
                    totalJobs += 1
