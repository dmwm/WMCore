#!/usr/bin/env python
"""
_FileAndEventBased_

Event based splitting algorithm that will chop a fileset into
a set of jobs based on event counts.  Each jobgroup returned will only
contain jobs for a single file.
"""

__revision__ = "$Id: FileAndEventBased.py,v 1.6 2009/04/09 16:41:08 sfoulkes Exp $"
__version__  = "$Revision: 1.6 $"

from sets import Set

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.Services.UUID import makeUUID

class FileAndEventBased(JobFactory):
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
        jobGroups = []
        fileset = self.subscription.availableFiles()

        #  //
        # // get the event total
        #//
        eventsPerJob = kwargs.get("events_per_job", 5000)
        try:
            selectionAlgorithm = kwargs['selection_algorithm']
        except KeyError, e:
            selectionAlgorithm = None
        carryOver = 0

        for f in fileset:
            if selectionAlgorithm:
                if not selectionAlgorithm( f ):
                    self.subscription.completeFiles( [ f ] )
                    continue
            jobGroup = groupInstance(subscription = self.subscription)
            jobGroups.append(jobGroup)
            eventsInFile = f['events']

            if eventsInFile == 0:
                currentJob = jobInstance(name = makeUUID())
                currentJob.addFile(f)
                jobGroup.add(currentJob)
                jobGroup.commit()
                jobGroup.recordAcquire()                
                continue

            currentEvent = 0
            while currentEvent < eventsInFile:
                currentJob = jobInstance(name = makeUUID())
                currentJob.addFile(f)
                currentJob.mask.setMaxAndSkipEvents(eventsPerJob, currentEvent)
                jobGroup.add(currentJob)
                currentEvent += eventsPerJob

            jobGroup.commit()
            jobGroup.recordAcquire()

        return jobGroups
