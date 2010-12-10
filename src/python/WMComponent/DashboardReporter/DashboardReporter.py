#!/usr/bin/env python
"""
_DashboardReport_

Pulls job information out of couch, pushes it to the dashboard.
"""

import logging
import threading

from WMCore.Agent.Harness import Harness
from WMComponent.DashboardReporter.DashboardReporterPoller import DashboardReporterPoller

class DashboardReporter(Harness):
    def __init__(self, config):
        Harness.__init__(self, config)
        return

    def preInitialization(self):
        pollInterval = self.config.DashboardReporter.pollInterval
        myThread = threading.currentThread()        
        myThread.workerThreadManager.addWorker(DashboardReporterPoller(self.config),
                                               pollInterval)

