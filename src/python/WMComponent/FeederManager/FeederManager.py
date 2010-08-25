#!/usr/bin/env
#pylint: disable-msg=W0613
"""
The feeder manager itself, set up event listeners and work event thread
"""
__all__ = []



import logging
import threading

from WMCore.Agent.Harness import Harness
from WMCore.WMFactory import WMFactory

from WMComponent.FeederManager.FeederManagerPoller import FeederManagerPoller

class FeederManager(Harness):
    """
    _FeederManager_
    
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
        if not hasattr(self.config.FeederManager, "addDatasetWatch"):
            logging.warning("Using default AddDatasetWatch handler")
            self.config.FeederManager.addDatasetWatchHandler =  \
                'WMComponent.FeederManager.Handler.DefaultAddDatasetWatch'

        # use a factory to dynamically load handlers.
        factory = WMFactory('generic')
        self.messages['AddDatasetWatch'] = \
            factory.loadObject(\
                self.config.FeederManager.addDatasetWatchHandler, self)
        
        myThread = threading.currentThread()
        myThread.runningFeedersLock = threading.Lock()
        myThread.runningFeeders = {}

        pollInterval = self.config.FeederManager.pollInterval
        logging.info("Setting poll interval to %s seconds" % pollInterval)
        myThread.workerThreadManager.addWorker(FeederManagerPoller(), \
                                               pollInterval)

