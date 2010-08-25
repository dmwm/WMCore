#!/usr/bin/env python
import os

from WMCore.Agent.Configuration import Configuration

config = Configuration()
config.component_("WorkQueueManager")
config.WorkQueueManager.logLevel = "DEBUG"
config.WorkQueueManager.componentName = "WorkQueueManager"
config.WorkQueueManager.componentDir = \
    os.path.join(os.getenv("TESTDIR"), "WorkQueueManager")
config.WorkQueueManager.addDatasetWatchHandler = \
    'WMComponent.FeederManager.Handler.DefaultAddDatasetWatch'

# The maximum number of threads to process each message type
config.FeederManager.maxThreads = 10