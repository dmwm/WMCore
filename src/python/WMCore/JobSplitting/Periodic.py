#!/usr/bin/env python
"""
_Periodic_

Periodically create jobs to process all files in a fileset.  A job will not be
created unless the previous job has been completed.  This algorithm will stop
creating jobs once the fileset has been closed.
"""

__revision__ = "$Id: Periodic.py,v 1.1 2009/08/04 18:24:29 sfoulkes Exp $"
__version__  = "$Revision: 1.1 $"

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
    stop creating jobs once the fileset has been closed.
    """
    def algorithm(self, groupInstance = None, jobInstance = None, *args,
                  **kwargs):
        """
        _algorithm_

        """
        jobPeriod = int(kwargs.get("job_period", 60))
        
        fileset = self.subscription.getFileset()
        fileset.load()

        if not fileset.open:
            self.subscription.completeFiles(self.subscription.availableFiles())
            return []

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)
        stateDAO = daoFactory(classname = "Jobs.NewestStateChangeForSub")
        results = stateDAO.execute(subscription = self.subscription["id"])

        if len(results) > 0:
            # If the query results multiple states they will all have the same
            # state_time.
            stateTime = int(results[0]["state_time"])
            if stateTime + jobPeriod > time.time():
                return []
        
            for result in results:
                if result["name"] not in ["closeout", "cleanout", "exhausted"]:
                    return []

        newJob = jobInstance(name = makeUUID())
        newJob.addFile(list(self.subscription.availableFiles()))
        newJobGroup = groupInstance(subscription = self.subscription)
        newJobGroup.add(newJob)
        newJobGroup.commit()

        return [newJobGroup]
