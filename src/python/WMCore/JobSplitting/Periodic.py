#!/usr/bin/env python
"""
_Periodic_

Periodically create jobs to process all files in a fileset.  A job will not be
created unless the previous job has been completed.  This algorithm will create
one final job containing all files once the input fileset has been closed.
"""

__revision__ = "$Id: Periodic.py,v 1.2 2009/08/10 16:10:22 sfoulkes Exp $"
__version__  = "$Revision: 1.2 $"

import time
import threading

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.Services.UUID import makeUUID
from WMCore.DAOFactory import DAOFactory

class Periodic(JobFactory):
    """
    _Periodic_

    Periodically create jobs to process all files in a fileset.  A job will not
    be created unless the previous job has been completed.  This algorithm will
    create one final job containing all files once the input fileset has been
    closed.
    """
    def outstandingJobs(self, jobPeriod):
        """
        _outstandingJobs_

        Determine whether or not there are outstanding jobs.
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
                    return True

            # If the query results multiple states they will all have the same
            # state_time.
            stateTime = int(results[0]["state_time"])
            if stateTime + jobPeriod > time.time():
                return True

        return False

    def algorithm(self, groupInstance = None, jobInstance = None, *args,
                  **kwargs):
        """
        _algorithm_

        Do some periodic job splitting.
        """
        jobPeriod = int(kwargs.get("job_period", 60))

        if self.outstandingJobs(jobPeriod):
            return []
        
        fileset = self.subscription.getFileset()
        fileset.load()

        if not fileset.open:
            availableFiles = self.subscription.availableFiles()

            if len(availableFiles) == 0:
                return []
            
            self.subscription.completeFiles(self.subscription.availableFiles())

        fileset.loadData()
        newJob = jobInstance(name = makeUUID())
        newJob.addFile(fileset.getFiles())
        newJobGroup = groupInstance(subscription = self.subscription)
        newJobGroup.add(newJob)
        newJobGroup.commit()

        return [newJobGroup]
