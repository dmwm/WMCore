#!/usr/bin/env python
"""
_TwoFileAndEventBased_

"""

__revision__ = "$Id: TwoFileAndEventBased.py,v 1.3 2009/07/30 18:37:07 sfoulkes Exp $"
__version__  = "$Revision: 1.3 $"

from sets import Set

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.Services.UUID import makeUUID

class TwoFileAndEventBased(JobFactory):
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
        eventsPerJob = int(kwargs.get("events_per_job", 5000))
        
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
            f.loadData(parentage = 1)
            jobGroup = groupInstance(subscription = self.subscription)
            jobGroups.append(jobGroup)
            eventsInFile = f['events']

            if eventsInFile == 0:
                currentJob = jobInstance(name = makeUUID())
                currentJob.addFile(f)
                self.subscription.acquireFiles(f)
                jobGroup.add(currentJob)
                jobGroup.commit()
                continue

            currentEvent = 0
            while currentEvent < eventsInFile:
                currentJob = jobInstance(name = makeUUID())
                currentJob.addFile(f)
                currentJob.mask.setMaxAndSkipEvents(eventsPerJob, currentEvent)
                jobGroup.add(currentJob)
                currentEvent += eventsPerJob

            self.subscription.acquireFiles(f)
            jobGroup.commit()

        return jobGroups
