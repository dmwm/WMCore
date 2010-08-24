#!/usr/bin/env python
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