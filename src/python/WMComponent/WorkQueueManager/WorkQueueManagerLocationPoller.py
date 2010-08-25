#!/usr/bin/env python
"""
updateLocations poller
"""
__all__ = []
__revision__ = "$Id: WorkQueueManagerLocationPoller.py,v 1.2 2010/05/13 18:43:34 sryu Exp $"
__version__ = "$Revision: 1.2 $"


import threading
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
        
    def algorithm(self, parameters):
        """
        Update locations
	    """
        myThread = threading.currentThread()
        myThread.name = "WorkQueueManagerLocationPoller"
        
        self.queue.logger.info("Updating data locations")
        try:
            self.queue.updateLocationInfo()
        except StandardError, ex:
            self.queue.logger.error("Error updating locations: %s" % str(ex))
