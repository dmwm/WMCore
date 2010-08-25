#!/usr/bin/env python
"""
pullWork poller
"""
__all__ = []
__revision__ = "$Id: WorkQueueManagerWorkPoller.py,v 1.3 2010/05/07 19:56:23 sryu Exp $"
__version__ = "$Revision: 1.3 $"



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
                self.queue.logger.info("Done Pulling work")        
                try:
                    self.queue.logger.info("Updating Parents status")        
                    self.queue.updateParent()
                except StandardError, ex:
                    import traceback
                    self.queue.logger.error("Unable to update Parent Status: %s\n%s" 
                                    % (str(ex), traceback.format_exc()))
                    
        except StandardError, ex:
            import traceback
            self.queue.logger.error("Unable to pull work from parent Error: %s\n%s" 
                                    % (str(ex), traceback.format_exc()))
        self.queue.logger.info("Obtained %s unit(s) of work" % work)
        return

    def retrieveCondition(self):
        """
        _retrieveCondition_
        set true or false for given retrieve condion
        i.e. thredshod on workqueue 
        """
        return True
