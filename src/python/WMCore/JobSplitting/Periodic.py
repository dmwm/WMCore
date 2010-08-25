#!/usr/bin/env python
"""
_Periodic_

Periodically create jobs to process all files in a fileset.  A job will not be
created until the previous job has been completed and new data has arrived.
This algorithm will create one final job containing all files once the input
fileset has been closed.

Note that the period here refers to the amount of time between the end of a job
and the creation of a new job.
"""

__revision__ = "$Id: Periodic.py,v 1.5 2009/09/30 12:30:54 metson Exp $"
__version__  = "$Revision: 1.5 $"

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
    arrived. This algorithm will create one final job containing all files once
    the input fileset has been closed.

    Note that the period here refers to the amount of time between the end of a
    job and the creation of a new job.
    """
    def outstandingJobs(self, jobPeriod, inputOpen):
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

            # If the fileset is closed we don't care about the period.  No new
            # files will be showing up and we want to send out the final job as
            # soon as possible.
            if not inputOpen:
                myThread.logger.debug("Periodic: fileset closed, returning...")
                return False
            
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

        if self.outstandingJobs(jobPeriod, fileset.open):
            return []

        availableFiles = self.subscription.availableFiles()
        if len(availableFiles) == 0:
            myThread = threading.currentThread()
            myThread.logger.debug("Periodic: No available files...")
            return []

        fileset.loadData()
        allFiles = fileset.getFiles()

        if not fileset.open:
            self.subscription.completeFiles(allFiles)
        else:
            self.subscription.acquireFiles(availableFiles)

        self.newGroup()
        self.newJob(name = makeUUID())
        self.currentJob.addFile(allFiles)