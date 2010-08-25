#!/usr/bin/env python
#pylint: disable-msg=E1101,E1103,C0103,R0902
"""
Defines default config values for FeederManager specific
parameters.
"""
__all__ = []



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

# The poll interval at which to look for new fileset/feeder association
config.FeederManager.pollInterval = 60
