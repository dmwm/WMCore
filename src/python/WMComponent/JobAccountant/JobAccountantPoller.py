#!/usr/bin/env python
"""
_JobAccountantPoller_

Poll WMBS for complete jobs and process their framework job reports.
"""




import time
import threading
import logging

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.Agent.Harness import Harness
from WMCore.DAOFactory import DAOFactory

from WMComponent.JobAccountant.AccountantWorker import AccountantWorker

class JobAccountantPoller(BaseWorkerThread):
    def __init__(self, config):
        BaseWorkerThread.__init__(self)
        self.config = config
        return
    
    def setup(self, parameters = None):
        """
        _setup_

        Instantiate the requisite number of accountant workers and create a
        processpool with them.  Also instantiate all the DAOs that we will use.
        """
        self.accountantWorker = AccountantWorker(couchURL = self.config.JobStateMachine.couchurl,
                                                 couchDBName = self.config.JobStateMachine.couchDBName)

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package = "WMCore.WMBS", logger = myThread.logger,
                                dbinterface = myThread.dbi)
        self.getJobsAction = daoFactory(classname = "Jobs.GetFWJRByState")
        return
    
    def algorithm(self, parameters = None):
        """
        _algorithm_

        Poll WMBS for jobs in the 'Complete' state and then pass them to the
        accountant worker.
        """
        completeJobs = self.getJobsAction.execute(state = "complete")
        logging.info("Jobs: %s" % completeJobs)

        if len(completeJobs) == 0:
            # Then we have no work to do.  Bye!
            return

        while len(completeJobs) > 25:
            jobsSlice = completeJobs[0:25]
            completeJobs = completeJobs[25:]
            self.accountantWorker(jobsSlice)

        self.accountantWorker(completeJobs)        
        return
