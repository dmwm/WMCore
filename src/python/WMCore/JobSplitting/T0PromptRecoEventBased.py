#!/usr/bin/env python
"""
_T0PromptRecoEventBased_

Event based splitting algorithm that will chop a fileset into
a set of jobs based on event counts.  This is meant only to be run
in the T0 and will only return jobs for runs that have been enabled
for RECO.
"""

__revision__ = "$Id: T0PromptRecoEventBased.py,v 1.4 2009/10/29 10:54:35 sfoulkes Exp $"
__version__  = "$Revision: 1.4 $"

from sets import Set
import threading

from WMCore.WMBS.File import File

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.Services.UUID import makeUUID
from WMCore.DAOFactory import DAOFactory

class T0PromptRecoEventBased(JobFactory):
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
        myThread = threading.currentThread()
        daoFactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)
        
        getFilesDAO = daoFactory(classname = "Subscriptions.GetT0PromptRecoAvailableFiles")
        fileset = getFilesDAO.execute(self.subscription["id"])

        #  //
        # // get the event total
        #//
        eventsPerJob = kwargs['events_per_job']
        carryOver = 0

        baseName = makeUUID()
        jobsCreated = 0
        self.newGroup()

        for f in fileset:
            loadedFile = File(id = f["file"])
            loadedFile.load()
            eventsInFile = loadedFile['events']

            if eventsInFile == 0:
                self.newJob(name = makeUUID())
                self.currentJob.addFile(f)
                self.currentJob["mask"].setMaxAndSkipEvents(eventsPerJob, 0)
                continue

            currentEvent = 0
            while currentEvent < eventsInFile:
                self.newJob(name = '%s-%s' % (baseName, jobsCreated))
                jobsCreated += 1
                self.currentJob.addFile(loadedFile)
                self.currentJob["mask"].setMaxAndSkipEvents(eventsPerJob, currentEvent)
                currentEvent += eventsPerJob

        return
