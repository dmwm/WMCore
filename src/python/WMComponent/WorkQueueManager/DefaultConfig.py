#!/usr/bin/env python

"""
Defines default config values for JobAccountant specific
parameters.
"""
__all__ = []
__revision__ = "$Id: DefaultConfig.py,v 1.4 2010/01/28 14:21:42 swakef Exp $"
__version__ = "$Revision: 1.4 $"

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

config.WorkQueueManager.logLevel = 'DEBUG'
config.WorkQueueManager.pollInterval = 10
# add parameters for global or local queue if default param is not what you want
config.WorkQueueManager.queueParams = {}
config.WorkQueueManager.queueParams['CacheDir'] = os.path.join(config.WorkQueueManager.componentDir, 'wf')
# used to identify (contact) this queue. May (or may not) be HTTPFrontend url
config.WorkQueueManager.queueParams['QueueURL'] = 'http://%s' % os.uname()[1]
