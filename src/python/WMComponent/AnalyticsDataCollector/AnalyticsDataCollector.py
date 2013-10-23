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
from WMComponent.AnalyticsDataCollector.AgentStatusPoller import AgentStatusPoller
from WMComponent.AnalyticsDataCollector.DataCollectAPI import DataUploadTime

class AnalyticsDataCollector(Harness):
    def __init__(self, config):
        Harness.__init__(self, config)
        return

    def preInitialization(self):
        pollInterval = self.config.AnalyticsDataCollector.pollInterval
        agentPollInterval =self.config.AnalyticsDataCollector.agentPollInterval
        self.timer = DataUploadTime()
        myThread = threading.currentThread()
        myThread.workerThreadManager.addWorker(AgentStatusPoller(self.config),
                                               agentPollInterval)
        myThread.workerThreadManager.addWorker(AnalyticsPoller(self.config),
                                               pollInterval)
