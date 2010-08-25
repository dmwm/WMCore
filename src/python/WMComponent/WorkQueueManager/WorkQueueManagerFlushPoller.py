#!/usr/bin/env python
"""
Flus negotiation failures
"""
__all__ = []
__revision__ = "$Id: WorkQueueManagerFlushPoller.py,v 1.3 2010/05/14 18:56:45 sryu Exp $"
__version__ = "$Revision: 1.3 $"


from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread


class WorkQueueManagerFlushPoller(BaseWorkerThread):
    """
    Polls for expired negotiations
    """
    def __init__(self, queue):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.queue = queue
        
    def algorithm(self, parameters):
        """
        Check & expire negotiation failures
        """
        
        self.queue.logger.info("Checking for expired negotiations")
        try:
            self.queue.flushNegotiationFailures()
        except StandardError, ex:
            self.queue.logger.error("Error flushing negotiation failures: %s" % str(ex))
            raise # Do we really want full traceback?
