#!/usr/bin/env python
"""
_Periodic_

Periodically create jobs to process all files in a fileset.  A job will not be
created until the previous job has been completed and new data has arrived.

Note that the period here refers to the amount of time between the end of a job
and the creation of a new job.
"""

__revision__ = "$Id: Periodic.py,v 1.9 2010/04/19 14:39:25 sfoulkes Exp $"
__version__  = "$Revision: 1.9 $"

import time
import threading

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.Services.UUID import makeUUID
from WMCore.DAOFactory import DAOFactory

class Periodic(JobFactory):
    """
    _Periodic_

    Periodically create jobs to process all files in a fileset.  A job will not
    be created until the previous job has been completed and new data has
    arrived. 

    Note that the period here refers to the amount of time between the end of a
    job and the creation of a new job.
    """
    def outstandingJobs(self, jobPeriod):
        """
        _outstandingJobs_

        Determine whether or not there are outstanding jobs and whether or not
        enough time has elapsed from the previous job to warrant creating a new
        job.
        """
        myThread = threading.currentThread()
        daoFactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)
        stateDAO = daoFactory(classname = "Jobs.NewestStateChangeForSub")
        results = stateDAO.execute(subscription = self.subscription["id"])

        if len(results) > 0:
            for result in results:
                if result["name"] not in ["closeout", "cleanout", "exhausted"]:
                    myThread.logger.debug("Periodic: Outstanding jobs, returning...")
                    return True

            stateTime = int(results[0]["state_time"])
            if stateTime + jobPeriod > time.time():
                myThread.logger.debug("Periodic: %s seconds remaining." % \
                                      ((stateTime + jobPeriod) - time.time()))
                return True

        return False

    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        Preform periodic job splitting.  Generate a new job only if conditions
        are right.
        """
        jobPeriod = int(kwargs.get("job_period", 60))
       
        fileset = self.subscription.getFileset()
        fileset.load()

        myThread = threading.currentThread()
        if not fileset.open:
            if not self.outstandingJobs(0):
                fileset.loadData()
                allFiles = fileset.getFiles()
                self.subscription.completeFiles(allFiles)
                return []
            else:
                myThread.logger.debug("Periodic: Waiting for jobs to complete.")

        if self.outstandingJobs(jobPeriod):
            return []

        availableFiles = self.subscription.availableFiles()
        if len(availableFiles) == 0:
            myThread.logger.debug("Periodic: No available files...")
            return []

        fileset.loadData()
        allFiles = fileset.getFiles()

        loadedFiles = []
        for file in allFiles:
            file.loadData(parentage = 0)
            loadedFiles.append(file)

        if not fileset.open:
            self.subscription.completeFiles(allFiles)

        self.newGroup()
        self.newJob(name = makeUUID())
        self.currentJob.addFile(loadedFiles)
