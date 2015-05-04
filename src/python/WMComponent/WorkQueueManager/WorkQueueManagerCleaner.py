#!/usr/bin/env python
"""
Perform cleanup actions
"""
__all__ = []



import time
import random
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread


class WorkQueueManagerCleaner(BaseWorkerThread):
    """
    Cleans expired items, updates element status.
    """
    def __init__(self, queue):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.queue = queue

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
        except Exception as ex:
            self.queue.logger.exception("Error cleaning queue: %s" % str(ex))
        self.queue.logger.info("Finished updating & cleaning.")
