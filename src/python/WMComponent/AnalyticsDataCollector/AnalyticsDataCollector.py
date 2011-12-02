#!/usr/bin/env python
"""
_ReqMonReporter_

"""


import threading

from WMCore.Agent.Harness import Harness
from WMComponent.AnalyticsDataCollector.AnalyticsPoller import AnalyticsPoller

class AnalyticsDataCollector(Harness):
    def __init__(self, config):
        Harness.__init__(self, config)
        return

    def preInitialization(self):
        pollInterval = self.config.AnalyticsDataCollector.pollInterval
        myThread = threading.currentThread()
        myThread.workerThreadManager.addWorker(AnalyticsPoller(self.config), 
                                               pollInterval)