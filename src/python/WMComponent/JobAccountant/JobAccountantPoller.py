#!/usr/bin/env python
"""
_JobAccountantPoller_

Poll WMBS for complete jobs and process their framework job reports.
"""

import threading
import logging

from Utils.IteratorTools import grouper
from Utils.Timers import timeFunction
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.Database.CouchUtils import CouchConnectionError
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
        self.accountantWorkSize = getattr(self.config.JobAccountant, 'accountantWorkSize', 100)

        return

    def setup(self, parameters=None):
        """
        _setup_

        Instantiate the requisite number of accountant workers and create a
        processpool with them.  Also instantiate all the DAOs that we will use.
        """
        self.accountantWorker = AccountantWorker(config=self.config)

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger=myThread.logger,
                                dbinterface=myThread.dbi)
        self.getJobsAction = daoFactory(classname="Jobs.GetFWJRByState")
        return

    @timeFunction
    def algorithm(self, parameters=None):
        """
        _algorithm_

        Poll WMBS for jobs in the 'Complete' state and then pass them to the
        accountant worker.
        """
        completeJobs = self.getJobsAction.execute(state="complete")
        logging.info("Found %d completed jobs", len(completeJobs))

        if len(completeJobs) == 0:
            logging.debug("No work to do; exiting")
            return

        for jobsSlice in grouper(completeJobs, self.accountantWorkSize):
            try:
                self.accountantWorker(jobsSlice)
            except WMException:
                myThread = threading.currentThread()
                if getattr(myThread, 'transaction', None) is not None:
                    myThread.transaction.rollback()
                raise
            except CouchConnectionError as ex:
                msg = "Caught CouchConnectionError exception. Waiting until the next polling cycle.\n"
                msg += str(ex)
                logging.error(msg)
                myThread = threading.currentThread()
                if getattr(myThread, 'transaction', None) is not None:
                    myThread.transaction.rollback()
            except Exception as ex:
                myThread = threading.currentThread()
                if getattr(myThread, 'transaction', None) is not None:
                    myThread.transaction.rollback()
                msg = "Hit general exception in JobAccountantPoller while using worker.\n"
                msg += str(ex)
                logging.exception(msg)
                raise JobAccountantPollerException(msg)

        return
