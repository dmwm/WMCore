#!/usr/bin/env python
"""
Perform cleanup actions
"""
__all__ = []



import time
import random
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr

class WorkQueueManagerCleaner(BaseWorkerThread):
    """
    Cleans expired items, updates element status.
    """
    def __init__(self, queue, config):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.queue = queue
        self.config = config
        self.reqmgr2Svc = ReqMgr(self.config.TaskArchiver.ReqMgr2ServiceURL)
        # state lists which shouldn't be populated in wmbs. (To prevent creating work before WQE status updated)
        self.abortedAndForceCompleteWorkflowCache = self.reqmgr2Svc.getAbortedAndForceCompleteRequestsFromMemoryCache()

    def setup(self, parameters):
        """
        Called at startup - introduce random delay
             to avoid workers all starting at once
        """
        t = random.randrange(self.idleTime)
        self.logger.info('Sleeping for %d seconds before 1st loop' % t)
        time.sleep(t)

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
            abortedAndForceCompleteRequests = self.abortedAndForceCompleteWorkflowCache.getData()

            for wf in abortedAndForceCompleteRequests:
                self.queue.killWMBSWorkflow(wf)
                    
        except Exception as ex:
            self.queue.logger.exception("Error cleaning queue: %s" % str(ex))
        self.queue.logger.info("Finished updating & cleaning.")
