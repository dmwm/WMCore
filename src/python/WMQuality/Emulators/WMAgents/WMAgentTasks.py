#!/usr/bin/env python
"""
The actual taskArchiver algorithm
"""
__all__ = []
__revision__ = "$Id: WMAgentTasks.py,v 1.3 2010/03/05 15:45:10 sryu Exp $"
__version__ = "$Revision: 1.3 $"

import threading
import logging
import time

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.WorkQueue.WorkQueue import WorkQueue, localQueue

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
        self.wq = localQueue()
        
    def algorithm(self, parameters):
        """
        """
        
        data = self.wq.getWork(self.resources)
        print "Data back from workqueue"
        print "%s: %s" % (self.resources, data)
        if len(data) != 0:
            elementIDs = []
            for element in data:
                elementIDs.append(element['element_id'])
            self.wq.gotWork(elementIDs)
            time.sleep(5)
            self.wq.doneWork(elementIDs)
            #self.wq.failWork(elementIDs)
            #self.wq.cancelWork(elementIDs)
        