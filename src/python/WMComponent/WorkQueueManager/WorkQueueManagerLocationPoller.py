#!/usr/bin/env python
"""
updateLocations poller
"""
__all__ = []


import time
import random

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread


class WorkQueueManagerLocationPoller(BaseWorkerThread):
    """
    Polls for location updates
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
        Update locations
            """

        self.queue.logger.info("Updating data locations")
        try:
            self.queue.updateLocationInfo()
        except Exception, ex:
            self.queue.logger.error("Error updating locations: %s" % str(ex))
