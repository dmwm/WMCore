#!/usr/bin/env python
"""
_CleanUpManager_

Clean up local couchdb according to the configurable condition.
TODO: move wmbs db cleanup function here
"""


import threading

from WMCore.Agent.Harness import Harness
from WMComponent.CleanUpManager.CleanCouchPoller import CleanCouchPoller

class CleanUpManager(Harness):
    def __init__(self, config):
        Harness.__init__(self, config)
        return

    def preInitialization(self):
        couchInterval = self.config.CleanUpManager.cleanCouchInterval
        myThread = threading.currentThread()
        myThread.workerThreadManager.addWorker(CleanCouchPoller(self.config), 
                                               couchInterval)