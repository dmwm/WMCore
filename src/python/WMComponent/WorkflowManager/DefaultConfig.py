#!/usr/bin/env python
#pylint: disable-msg=E1101,E1103,C0103,R0902
"""
Defines default config values for WorkflowManager specific
parameters.
"""
__all__ = []



import os

from WMCore.Agent.Configuration import Configuration

config = Configuration()
config.component_("WorkflowManager")
config.WorkflowManager.logLevel = "INFO"
config.WorkflowManager.componentName = "WorkflowManager"
config.WorkflowManager.componentDir = \
    os.path.join(os.getenv("TESTDIR"), "WorkflowManager")

# The maximum number of threads to process each message type
config.WorkflowManager.maxThreads = 10

# The poll interval at which to look for new filesets
config.WorkflowManager.pollInterval = 60