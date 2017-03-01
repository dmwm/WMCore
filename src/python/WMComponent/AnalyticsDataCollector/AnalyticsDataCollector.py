#!/usr/bin/env python
"""
_AnalyticsDataCollector_

Collects request/jobs summary data from local couchdb
and put in local summary couch db.
Then summary data will be replicated to central wmstats couchdb
"""


import threading

from WMCore.Agent.Harness import Harness
from WMComponent.AnalyticsDataCollector.AnalyticsPoller import AnalyticsPoller

class AnalyticsDataCollector(Harness):
    """
    Component class for AnalyticsDataCollector module
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

        Sets up the workers threads
        """
        pollInterval = self.config.AnalyticsDataCollector.pollInterval

        myThread = threading.currentThread()

        myThread.workerThreadManager.addWorker(AnalyticsPoller(self.config),
                                               pollInterval)
