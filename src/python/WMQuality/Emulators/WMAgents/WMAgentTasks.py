#!/usr/bin/env python
"""
The actual taskArchiver algorithm
"""
__all__ = []
__revision__ = "$Id: WMAgentTasks.py,v 1.1 2010/02/08 22:21:08 sryu Exp $"
__version__ = "$Revision: 1.1 $"

import threading
import logging
import time

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.WorkQueue.WorkQueue import WorkQueue 

class WMAgentTasks(BaseWorkerThread):
    """
    Polls for Error Conditions, handles them
    """
    def __init__(self, resources):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.resources = resources
        self.wq = WorkQueue()
        
    def algorithm(self, parameters):
        """
        """
        
        data = self.wq.getWork(self.resources)
        self.wq.gotWork(data["subscriptionIDs"])
        time.sleep(1)
        self.wq.doneWork(data["subscriptionIDs"])
        #self.wq.failWork(elementIDs)
        #self.wq.cancelWork(elementIDs)
        