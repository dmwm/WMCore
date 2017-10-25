#!/usr/bin/env python
"""
Poll request manager for new work
"""
__all__ = []



import time
import random
from Utils.Timers import timeFunction
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.WorkQueue.WorkQueueReqMgrInterface import WorkQueueReqMgrInterface

class WorkQueueManagerReqMgrPoller(BaseWorkerThread):
    """
    Polls for requests
    """
    def __init__(self, queue, config, reqMgr = None):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.wq = queue
        self.config = config
        if reqMgr:
            self.reqMgr = reqMgr
        else:
            self.reqMgr = WorkQueueReqMgrInterface(**self.config)
        self.previousState = {}

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
        retrieve work from RequestManager and send updates
            """
        try:
            return self.reqMgr(self.wq)
        except Exception as ex:
            self.queue.logger.error("Error in ReqMgr loop: %s" % str(ex))
