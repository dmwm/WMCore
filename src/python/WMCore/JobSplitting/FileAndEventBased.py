#!/usr/bin/env python
"""
_FileAndEventBased_

Event based splitting algorithm that will chop a fileset into
a set of jobs based on event counts.  Each jobgroup returned will only
contain jobs for a single file.
"""

__revision__ = "$Id: FileAndEventBased.py,v 1.11 2009/09/30 12:30:54 metson Exp $"
__version__  = "$Revision: 1.11 $"

from sets import Set

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.Services.UUID import makeUUID

class FileAndEventBased(JobFactory):
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
        except KeyError, e:
            selectionAlgorithm = None
        carryOver = 0

        for f in fileset:
            if selectionAlgorithm:
                if not selectionAlgorithm( f ):
                    self.subscription.completeFiles( [ f ] )
                    continue
            self.newGroup()
            eventsInFile = int(f["events"])

            if eventsInFile == 0:
                self.newJob(name = makeUUID())
                self.subscription.acquireFiles(f)
                self.currentJob.addFile(f)
                self.currentJob["mask"].setMaxAndSkipEvents(eventsPerJob, 0)
                continue

            currentEvent = 0
            while currentEvent < eventsInFile:
                self.newJob(name = makeUUID())
                self.currentJob.addFile(f)
                self.currentJob["mask"].setMaxAndSkipEvents(eventsPerJob, currentEvent)
                currentEvent += eventsPerJob
                
            self.subscriptions.acquireFiles(f)