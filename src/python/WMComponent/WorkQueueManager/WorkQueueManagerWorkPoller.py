#!/usr/bin/env python
"""
pullWork poller
"""
__all__ = []
__revision__ = "$Id: WorkQueueManagerWorkPoller.py,v 1.1 2010/02/12 14:34:37 swakef Exp $"
__version__ = "$Revision: 1.1 $"



from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread


class WorkQueueManagerWorkPoller(BaseWorkerThread):
    """
    Polls for Work
    """
    def __init__(self, queue):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.queue = queue
        
    def algorithm(self, parameters):
        """
        Pull in work
	"""
        self.queue.logger.info("Pulling work from %s" % self.queue.params['ParentQueue'])
        work = 0
        try:
            if self.retrieveCondition():
                work = self.queue.pullWork()
                # force update
                try:
                    self.queue.updateParent()
                except StandardError:
                    pass
        except StandardError, ex:
            self.queue.logger.error("Unable to pull work from parent Error: %s" % str(ex))
        self.queue.logger.info("Obtained %s unit(s) of work" % work)
        return

    def retrieveCondition(self):
        """
        _retrieveCondition_
        set true or false for given retrieve condion
        i.e. thredshod on workqueue 
        """
        return True
