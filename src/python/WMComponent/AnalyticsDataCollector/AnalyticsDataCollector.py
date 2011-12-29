#!/usr/bin/env python
"""
_ReqMonReporter_

"""


import threading

from WMCore.Agent.Harness import Harness
from WMComponent.AnalyticsDataCollector.AnalyticsPoller import AnalyticsPoller
from WMComponent.AnalyticsDataCollector.CleanUpPoller import CleanUpPoller

class AnalyticsDataCollector(Harness):
    def __init__(self, config):
        Harness.__init__(self, config)
        return

    def preInitialization(self):
        pollInterval = self.config.AnalyticsDataCollector.pollInterval
        cleanUpInterval = self.config.AnalyticsDataCollector.cleanUpInterval
        myThread = threading.currentThread()
        myThread.workerThreadManager.addWorker(AnalyticsPoller(self.config), 
                                               pollInterval)
        myThread.workerThreadManager.addWorker(CleanUpPoller(self.config), 
                                               cleanUpInterval)