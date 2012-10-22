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

from WMCore.WMException import WMException

class JobAccountantPollerException(WMException):
    """
    _JobAccountantPollerException_

    JobAccountant error class for general
    errors in the poller (used as a catchall
    from the worker).
    """

class JobAccountantPoller(BaseWorkerThread):
    def __init__(self, config):
        BaseWorkerThread.__init__(self)
        self.config = config
        self.accountantWorkSize = getattr(self.config.JobAccountant,
                                          'accountantWorkSize', 100)
        # initialize the alert framework (if available - config.Alert present)
        #    self.sendAlert will be then be available
        self.initAlerts(compName = "JobAccountant")

        return

    def setup(self, parameters = None):
        """
        _setup_

        Instantiate the requisite number of accountant workers and create a
        processpool with them.  Also instantiate all the DAOs that we will use.
        """
        #self.accountantWorker = AccountantWorker(couchURL = self.config.JobStateMachine.couchurl,
        #                                         couchDBName = self.config.JobStateMachine.couchDBName)
        self.accountantWorker = AccountantWorker(config = self.config)

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
            logging.debug("No work to do; exiting")
            return

        while len(completeJobs) > self.accountantWorkSize:
            try:
                jobsSlice = completeJobs[:self.accountantWorkSize]
                completeJobs = completeJobs[self.accountantWorkSize:]
                self.accountantWorker(jobsSlice)
            except WMException:
                raise
            except Exception, ex:
                msg =  "Hit general exception in JobAccountantPoller while using worker.\n"
                msg += str(ex)
                logging.error(msg)
                self.sendAlert(6, msg = msg)
                logging.debug("jobsSlice:")
                logging.debug(jobsSlice)
                raise JobAccountantPollerException(msg)

        try:
            self.accountantWorker(completeJobs)
        except WMException:
            myThread = threading.currentThread()
            if getattr(myThread, 'transaction', None) != None:
                myThread.transaction.rollback()
            raise
        except Exception, ex:
            myThread = threading.currentThread()
            if getattr(myThread, 'transaction', None) != None:
                myThread.transaction.rollback()
            msg =  "Hit general exception in JobAccountantPoller in last worker use.\n"
            msg += str(ex)
            logging.error(msg)
            self.sendAlert(6, msg = msg)
            logging.debug("jobs left:")
            logging.debug(completeJobs)
            raise JobAccountantPollerException(msg)

        return
