#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Rest Model for Agents Monitoring.
"""

import time
import logging

from WMCore.WebTools.RESTModel import RESTModel
from WMCore.DAOFactory import DAOFactory
from WMCore.Services.Requests import JSONRequests

class AgentRESTModel(RESTModel):
    """
    A REST Model for Agents. Currently only supports monitoring, so only
    implementing the GET verb.
    """
    def __init__(self, config = {}):
        RESTModel.__init__(self, config)

        self.daofactory = DAOFactory(package = "WMCore.Agent.Database", 
                                     logger = self, dbinterface = self.dbi)

        self.addDAO('GET', "heartbeatInfo", "GetHeartbeatInfo")
        self.addDAO('GET', "heartbeatInfoDetail", "GetAllHeartbeatInfo")
        self.addMethod('GET', "heartbeat", self.getHeartBeatWarning)
        
    
    def getHeartBeatWarning(self):
        results = self.methods["GET"]["heartbeatInfo"]["call"]()
        for result in results:
            result['ago'] = int(time.time()) - result['last_updated']
            result['alarm'] =  result['update_threshold'] - result['ago']
        return results