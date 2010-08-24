#!/usr/bin/env python
import os

from WMCore.Agent.Configuration import Configuration

config = Configuration()
config.component_("FeederManager")
config.FeederManager.logLevel = "INFO"
config.FeederManager.componentName = "FeederManager"
config.FeederManager.componentDir = \
    os.path.join(os.getenv("TESTDIR"), "FeederManager")
config.FeederManager.addDatasetWatchHandler = \
    'WMComponent.FeederManager.Handler.DefaultAddDatasetWatch'

# The maximum number of threads to process each message type
config.FeederManager.maxThreads = 10