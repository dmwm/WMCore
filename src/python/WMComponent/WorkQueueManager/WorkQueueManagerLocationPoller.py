#!/usr/bin/env python
"""
updateLocations poller
"""
__all__ = []


import time
import random
import logging

from Utils.Timers import timeFunction
from Utils.wmcoreDTools import resetWatchdogTimer, moduleName
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread


class WorkQueueManagerLocationPoller(BaseWorkerThread):
    """
    Polls for location updates
    """
    def __init__(self, queue, config):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.queue = queue
        self.config = config

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
        Update locations
            """

        self.queue.logger.info("Updating data locations")
        try:
            self.queue.updateLocationInfo()
        except Exception as ex:
            self.queue.logger.error("Error updating locations: %s" % str(ex))

        # Reset its own watchdog timer at the end of the run cycle
        logging.info(f"Resetting {moduleName(self)} watchdog timer.")
        if resetWatchdogTimer(self):
            logging.warning(f"Failed to reset {moduleName(self)} watchdog timer. The component might be restarted soon.")
