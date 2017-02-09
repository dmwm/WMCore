#!/usr/bin/env python
"""
_TwoFileAndEventBased_

"""





from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.Services.UUIDLib import makeUUID

class TwoFileAndEventBased(JobFactory):
    """
    Split jobs by number of events
    """
    def algorithm(self, *args, **kwargs):
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
        except KeyError as e:
            selectionAlgorithm = None
        carryOver = 0

        for f in fileset:
            if selectionAlgorithm:
                if not selectionAlgorithm( f ):
                    self.subscription.completeFiles( [ f ] )
                    continue
            f.loadData(parentage = 1)
            self.newGroup()
            eventsInFile = f['events']

            if eventsInFile == 0:
                self.newJob(name = makeUUID())
                self.currentJob.addFile(f)
                continue

            currentEvent = 0
            while currentEvent < eventsInFile:
                self.newJob(name = makeUUID())
                self.currentJob.addFile(f)
                self.currentJob["mask"].setMaxAndSkipEvents(eventsPerJob, currentEvent)
                currentEvent += eventsPerJob
