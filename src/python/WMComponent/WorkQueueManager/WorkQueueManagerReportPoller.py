#!/usr/bin/env python
"""
update parent poller
"""
__all__ = []
__revision__ = "$Id: WorkQueueManagerReportPoller.py,v 1.3 2010/05/14 18:56:45 sryu Exp $"
__version__ = "$Revision: 1.3 $"


from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread


class WorkQueueManagerReportPoller(BaseWorkerThread):
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
        
        self.queue.logger.info("Sending update to parent queue")
        try:
            self.queue.updateParent()
        except StandardError, ex:
            self.queue.logger.error("Error reporting to parent: %s" % str(ex))
