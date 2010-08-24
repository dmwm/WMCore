#!/usr/bin/env python

"""
Defines default config values for JobAccountant specific
parameters.
"""
__all__ = []



import os

from WMCore.Agent.Configuration import Configuration

config = Configuration()
config.component_("TaskArchiver")
config.TaskArchiver.logLevel = 'SQLDEBUG'
config.TaskArchiver.pollInterval = 10
config.TaskArchiver.timeOut      = 0
config,TaskArchiver.WorkQueueParams = getattr(config.WorkQueueManager, 'queueParams', {})

config.component_('JobStateMachine')
config.JobStateMachine.couchurl        = os.getenv('COUCHURL', 'cmssrv52.fnal.gov:5984')
config.JobStateMachine.default_retries = 1


