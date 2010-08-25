#!/usr/bin/env python

"""
Defines default config values for JobAccountant specific
parameters.
"""
__all__ = []
__revision__ = "$Id: DefaultConfig.py,v 1.2 2010/01/22 22:07:26 sryu Exp $"
__version__ = "$Revision: 1.2 $"

import os

from WMCore.Agent.Configuration import Configuration

config = Configuration()
config.component_("WorkQueueManager")
# either request Mgr host url or global workqueue das service url needs to be set
# depending on the level of work queue
# accessing Request Manager.
config.WorkQueueManager.level = "GlobalQueue"

# serviceUrl is either upper level queue Url or request manager url in case of global queue 
config.WorkQueueManager.serviceUrl = 'cmssrv49.fnal.gov:8585'
config.WorkQueueManager.componentDir = config.General.WorkDir + "/WorkQueueManager"
config.WorkQueueManager.namespace = "WMComponent.WorkQueueManager.WorkQueueManager"
# accessing global queue
#config.WorkQueueManager.level = "LocalQueue"
#config.WorkQueueManager.serviceUrl = "http://cmssrv18.fnal.gov:6660"

config.WorkQueueManager.logLevel = 'INFO'
config.WorkQueueManager.pollInterval = 10
