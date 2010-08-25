#!/usr/bin/env python
"""
_WorkQueueManagerPoller_

Pull work out of the work queue.
"""

__revision__ = "$Id: WorkQueueManagerWorkPoller.py,v 1.6 2010/05/14 18:56:45 sryu Exp $"
__version__ = "$Revision: 1.6 $"

import socket

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

                self.queue.logger.info("Done Pulling work")        
                try:
                    self.queue.logger.info("Updating Parents status")        
                    self.queue.updateParent()
                except StandardError, ex:
                    import traceback
                    self.queue.logger.error("Unable to update Parent Status: %s\n%s" 
                                    % (str(ex), traceback.format_exc()))
        except socket.error, (value, message):
            self.queue.logger.error("Error %s opening connection to work queue: %s" % (value, message))
            return
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
