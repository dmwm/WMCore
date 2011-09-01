#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Rest Model for Global Monitoring.
"""

from WMCore.WebTools.RESTModel import RESTModel

from WMCore.HTTPFrontEnd.GlobalMonitor.API.RequestMonitor \
     import getRequestOverview
from WMCore.HTTPFrontEnd.GlobalMonitor.API.AgentMonitor \
     import getAgentOverview
from WMCore.HTTPFrontEnd.GlobalMonitor.API.SiteMonitor \
     import getSiteOverview

class GlobalMonitorRESTModel(RESTModel):
    """
    A REST Model for WMBS. Currently only supports monitoring, so only
    implementing the GET verb.
    """
    def __init__(self, config = {}):

        RESTModel.__init__(self, config)
        #print config
        self.serviceURL = self.config.serviceURL
        self.serviceLevel = self.config.serviceLevel
        self.workloadSummaryCouchURL = self.config.workloadSummaryCouchURL

        self._addMethod("GET", "requestmonitor", self.getRequestMonitor) #expires=16000
        self._addMethod("GET", "agentmonitor", self.getAgentMonitor,
                       args = ['detail'])
        self._addMethod("GET", "sitemonitor", self.getSiteMonitor)
        self._addMethod("GET", "env", self.getEnvValues)

    def getRequestMonitor(self):
        return getRequestOverview(self.serviceURL, self.serviceLevel)

    def getAgentMonitor(self):
        return getAgentOverview(self.serviceURL, self.serviceLevel)

    def getSiteMonitor(self):
        return getSiteOverview(self.serviceURL, self.serviceLevel)
    
    def getEnvValues(self):
        return {'workload_summary_url': self.workloadSummaryCouchURL}