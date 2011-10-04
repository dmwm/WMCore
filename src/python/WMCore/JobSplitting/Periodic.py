#!/usr/bin/env python
"""
_Periodic_

"""




import time
import threading

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.Services.UUID import makeUUID
from WMCore.DAOFactory import DAOFactory

class Periodic(JobFactory):
    """
    _Periodic_

    Periodically create jobs to process all files in a fileset.  A job will not
    be created until the previous job (if there is one) has been completed and
    there are available (new) files in the fileset.

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
                myThread.logger.debug("Periodic: %d seconds until next job..." % \
                                      ((stateTime + jobPeriod) - time.time()))
                return True

        return False

    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        """
        myThread = threading.currentThread()

        jobPeriod = int(kwargs.get("job_period", 900))

        fileset = self.subscription.getFileset()
        fileset.load()

        # If fileset is closed just mark all available (new) files
        # as complete. They won't be handled here anymore, but by
        # a chained EndOfRun subscription.
        if not fileset.open:
            availableFiles = self.subscription.availableFiles()
            if len(availableFiles) > 0:
                self.subscription.completeFiles(availableFiles)
            return

        # Wait for enough time after last job completion.
        if self.outstandingJobs(jobPeriod):
            return

        # Do we have available (new) files to run on ?
        availableFiles = self.subscription.availableFiles()
        if len(availableFiles) == 0:
            myThread.logger.debug("Periodic: No available files...")
            return

        fileset.loadData()
        allFiles = fileset.getFiles()

        self.newGroup()
        self.newJob(name = makeUUID())
        self.currentJob.addFile(allFiles)

        return
