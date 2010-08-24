#!/usr/bin/env
import logging
import threading

from WMCore.Agent.Harness import Harness
from WMCore.WMFactory import WMFactory

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

class MyWorker(BaseWorkerThread):
    def algorithm(self, parameters):
        threading.currentThread().logger.info("Doing some work!")

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
