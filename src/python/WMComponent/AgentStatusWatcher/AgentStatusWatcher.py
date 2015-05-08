#!/usr/bin/env python
"""
_AgentStatusWatcher_

This component is a general purpose component for resources and processes handling
"""

import logging
import threading

from WMCore.Agent.Harness import Harness
from WMComponent.AgentStatusWatcher.ResourceControlUpdater import ResourceControlUpdater
from WMComponent.AgentStatusWatcher.AgentStatusPoller import AgentStatusPoller

class AgentStatusWatcher(Harness):
    """
    Component class for AgentStatusWatcher module
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

        Sets up the worker thread
        """
        logging.info("AgentStatusWatcher.preInitialization")
        resourceUpdaterPollInterval = self.config.AgentStatusWatcher.resourceUpdaterPollInterval
        agentPollInterval = self.config.AgentStatusWatcher.agentPollInterval
        myThread = threading.currentThread()
        
        logging.info("Setting ResourcesUpdate poll interval to %s seconds" % agentPollInterval)
        myThread.workerThreadManager.addWorker(AgentStatusPoller(self.config),
                                               agentPollInterval)
        
        logging.info("Setting ResourcesUpdate poll interval to %s seconds" % resourceUpdaterPollInterval)
        myThread.workerThreadManager.addWorker(ResourceControlUpdater(self.config),
                                               resourceUpdaterPollInterval)
        return
