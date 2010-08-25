#!/usr/bin/env python
# pylint: disable-msg=W0104
"""
_WorkQueueImpl_

container representing work queue

"""
__revision__ = "$Id: WorkQueue.py,v 1.3 2009/05/08 15:32:32 sryu Exp $"
__version__  = "$Revision: 1.3 $"

from WMCore.DataStructs.WMObject import WMObject
import time
             
class WorkQueue(Harness):
    """
    _WorkQueueManager_
    
    Manages the creation, running and destruction of Feeders and associated
    Filesets
    """

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
    
    def preInitialization(self):
        """
        Add required worker modules to work threads
        """
        # in case nothing was configured we have a fallback.
        if not hasattr(self.config.WorkQueueManager, "GiveWork"):
            logging.warning("Using default WorkQueueManager handler")
            self.config.WorkQueueManager.addDatasetWatchHandler =  \
                'WMComponent.WorkQueueManager.Handler.DefaultAddDatasetWatch'

        # use a factory to dynamically load handlers.
        factory = WMFactory('generic')
        self.messages['GiveWork'] = \
            factory.loadObject(\
                self.config.WorkQueueManager.jobQueueHandler, self)