#!/usr/bin/env python
"""
_HarvestingScheduler_

Polls PhEDEx for complete transfers and starts the harvesting jobs.
"""

import threading

from WMCore.Agent.Harness import Harness
from WMComponent.HarvestingScheduler.HarvestingPoller import HarvestingPoller

class HarvestingScheduler(Harness):
    def __init__(self, config):
        Harness.__init__(self, config)
        return

    def preInitialization(self):
        pollInterval = self.config.HarvestingScheduler.pollInterval
        myThread = threading.currentThread()        
        myThread.workerThreadManager.addWorker(HarvestingPoller(self.config), pollInterval)
