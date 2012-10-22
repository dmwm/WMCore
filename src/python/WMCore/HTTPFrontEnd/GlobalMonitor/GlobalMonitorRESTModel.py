#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Rest Model for Global Monitoring.
"""
import re
from WMCore.WebTools.RESTModel import RESTModel

from WMCore.HTTPFrontEnd.GlobalMonitor.API.RequestMonitor \
     import getRequestOverview
from WMCore.HTTPFrontEnd.GlobalMonitor.API.AgentMonitor \
     import getAgentOverview
from WMCore.HTTPFrontEnd.GlobalMonitor.API.SiteMonitor \
     import getSiteOverview
from WMCore.HTTPFrontEnd.GlobalMonitor.DataCache import DataCache

class GlobalMonitorRESTModel(RESTModel):
    """
    A REST Model for WMBS. Currently only supports monitoring, so only
    implementing the GET verb.
    """
    def __init__(self, config = {}):

        RESTModel.__init__(self, config)

        self.serviceURL = self.config.serviceURL
        self.serviceLevel = self.config.serviceLevel
        self.workloadSummaryCouchURL = self.config.workloadSummaryCouchURL

        self._addMethod("GET", "requestmonitor", self.getRequestMonitor, secured=True) #expires=16000
        self._addMethod("GET", "agentmonitor", self.getAgentMonitor,
                       args = ['detail'], secured=True)
        self._addMethod("GET", "sitemonitor", self.getSiteMonitor, secured=True)
        self._addMethod("GET", "env", self.getEnvValues, secured=True)
        self._addMethod("GET", "requests", self.getRequests, args=['name'], secured=True) #expires=16000

    def getRequests(self, name):
        if DataCache.isRequestDataExpired():
            DataCache.setRequestData(getRequestOverview(self.serviceURL, self.serviceLevel))
        prog = re.compile(name, re.IGNORECASE)
        filtered = []
        for item in DataCache.getRequestData():
            if prog.search(item['request_name']) != None:
                filtered.append(item)
        return filtered

    def getRequestMonitor(self):
        if DataCache.isRequestDataExpired():
            DataCache.setRequestData(getRequestOverview(self.serviceURL, self.serviceLevel))
        return DataCache.getRequestData()

    def getAgentMonitor(self):
        if DataCache.isAgentDataExpired():
            DataCache.setAgentData(getAgentOverview(self.serviceURL, self.serviceLevel))
        return DataCache.getAgentData()

    def getSiteMonitor(self):
        if DataCache.isSiteDataExpired():
            DataCache.setSiteData(getSiteOverview(self.serviceURL, self.serviceLevel))
        return DataCache.getSiteData()

    def getEnvValues(self):
        if self.config.serviceURL.lower() == 'local':
            reqURL = ""
        else:
            reqURL = "%s/" % self.serviceURL
        return {'workload_summary_url': self.workloadSummaryCouchURL,
                'reqmgr_url': reqURL}
