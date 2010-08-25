#!/usr/bin/env python
"""
updateLocations poller
"""
__all__ = []
__revision__ = "$Id: WorkQueueManagerLocationPoller.py,v 1.3 2010/05/14 18:56:45 sryu Exp $"
__version__ = "$Revision: 1.3 $"


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
        
        self.queue.logger.info("Updating data locations")
        try:
            self.queue.updateLocationInfo()
        except StandardError, ex:
            self.queue.logger.error("Error updating locations: %s" % str(ex))
