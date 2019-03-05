#!/usr/bin/env python
"""
pullWork poller
"""
import random
import time

from Utils.Timers import timeFunction
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.WorkQueue.WMBSHelper import freeSlots
from WMCore.WorkQueue.WorkQueueUtils import cmsSiteNames
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread


class WorkQueueManagerWMBSFileFeeder(BaseWorkerThread):
    """
    Polls for Work
    """

    def __init__(self, queue, config):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)

        self.queue = queue
        self.config = config
        self.reqmgr2Svc = ReqMgr(self.config.General.ReqMgr2ServiceURL)
        # state lists which shouldn't be populated in wmbs. (To prevent creating work before WQE status updated)
        self.abortedAndForceCompleteWorkflowCache = self.reqmgr2Svc.getAbortedAndForceCompleteRequestsFromMemoryCache()

    def setup(self, parameters):
        """
        Called at startup - introduce random delay
             to avoid workers all starting at once
        """
        t = random.randrange(self.idleTime)
        self.logger.info('Sleeping for %d seconds before 1st loop', t)
        time.sleep(t)

    @timeFunction
    def algorithm(self, parameters):
        """
        Get work from local workqueue to be injected into WMBS/DBSBuffer
        """
        self.queue.logger.info("Getting work and feeding WMBS files...")
        try:
            # need to make sure jobs are created
            resources, jobCounts = freeSlots(minusRunning=True, allowedStates=['Normal', 'Draining'],
                                             knownCmsSites=cmsSiteNames())

            for site in resources:
                self.queue.logger.info("I need %d jobs on site %s" % (resources[site], site))

            abortedAndForceCompleteRequests = self.abortedAndForceCompleteWorkflowCache.getData()

            previousWorkList = self.queue.getWork(resources, jobCounts,
                                                  excludeWorkflows=abortedAndForceCompleteRequests)
            self.queue.logger.info("Acquired %s units of work for WMBS file creation", len(previousWorkList))
        except Exception as ex:
            self.queue.logger.error("Error in wmbs inject loop: %s" % str(ex))
