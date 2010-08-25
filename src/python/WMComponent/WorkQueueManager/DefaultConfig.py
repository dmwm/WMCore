#!/usr/bin/env python

"""
Defines default config values for JobAccountant specific
parameters.
"""
__all__ = []
__revision__ = "$Id: DefaultConfig.py,v 1.1 2009/12/01 21:55:21 sryu Exp $"
__version__ = "$Revision: 1.1 $"

import os

from WMCore.Agent.Configuration import Configuration

config = Configuration()
config.component_("WorkQueueManager")
#accessType is either REST or DB. REST access is REST access to remote server
#DB access is direct DB access (doens't need for DB reside in the same machine) 
config.WorkQueueManager.accessType = 'REST'
# unit can be either dataset or block
config.WorkQueueManager.unit = 'dataset'
config.WorkQueueManager.team = 'team_usa'
config.WorkQueueManager.requestMgrHost = 'cmssrv49.fnal.gov:8585'

config.WorkQueueManager.logLevel = 'INFO'
config.WorkQueueManager.pollInterval = 10
config.WorkQueueManager.level = "GlobalQueue"
config.WorkQueueManager.serviceUrl = "http://cmssrv18.fnal.gov:6660"
