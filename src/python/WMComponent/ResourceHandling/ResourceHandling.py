#!/usr/bin/env python
"""
_ResourceHandling_

This component is a general purpose component for resources and processes handling
"""

import logging
import threading

from WMCore.Agent.Harness import Harness
from WMComponent.ResourceHandling.ResourcesUpdate import ResourcesUpdate

class ResourceHandling(Harness):
    """
    Component class for ResourceHandling module
    """
    
    def __init__(self, config):
        """
        __init__

        Initialize the Harness
        """
        Harness.__init__(self, config)
        
        return

    def preInitialization(self):
        """
        _preInitialization_

        Sets up the worker thread
        """
        logging.info("ResourceHandling.preInitialization")
        resourcesPollInterval = self.config.ResourceHandling.resourcesPollInterval
        
        myThread = threading.currentThread()
        
        logging.info("Setting ResourcesUpdate poll interval to %s seconds" % resourcesPollInterval)
        myThread.workerThreadManager.addWorker(ResourcesUpdate(self.config),
                                               resourcesPollInterval)
        return
        
