#!/usr/bin/env python
"""
Perform cleanup actions
"""
import time
import random
import threading

from Utils.Timers import timeFunction
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.DAOFactory import DAOFactory


class WorkQueueManagerCleaner(BaseWorkerThread):
    """
    Cleans expired items, updates element status.
    """
    def __init__(self, queue, config):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.forbiddenStatus = ["aborted", "aborted-completed", "force-complete", "completed"]
        self.queue = queue
        self.config = config
        self.reqmgr2Svc = ReqMgr(self.config.General.ReqMgr2ServiceURL)
        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS",
                                logger=myThread.logger,
                                dbinterface=myThread.dbi)
        self.finishedWorflowCheck = daoFactory(classname="Subscriptions.CountFinishedSubscriptionsByWorkflow")

    def setup(self, parameters):
        """
        Called at startup - introduce random delay
             to avoid workers all starting at once
        """
        t = random.randrange(self.idleTime)
        self.logger.info('Sleeping for %d seconds before 1st loop' % t)
        time.sleep(t)

    @timeFunction
    def algorithm(self, parameters):
        """
        Check & expire negotiation failures
        """
        self.queue.logger.info("Start updating & cleaning...")
        try:
            self.queue.performQueueCleanupActions()
            # this will clean up whatever left over from above clean up.
            # also if the wq replication has problem it won't delay the killing jobs in condor
            # and updating wmbs status
            # state lists which shouldn't be populated in wmbs. (To prevent creating work before WQE status updated)
            # added completed status in the list due to the race condition
            requests = self.reqmgr2Svc.getRequestByStatusFromMemoryCache(self.forbiddenStatus).getData()
            results = self.finishedWorflowCheck.execute(workflowNames=requests)

            requestsToKill = [reqInfo["workflow"] for reqInfo in results if reqInfo["open"] > 0]

            self.queue.logger.info("Killing %d requests in WMBS ...", len(requestsToKill))
            self.queue.killWMBSWorkflows(requestsToKill)
        except Exception as ex:
            self.queue.logger.exception("Error cleaning queue: %s", str(ex))

        self.queue.logger.info("Finished updating & cleaning.")
