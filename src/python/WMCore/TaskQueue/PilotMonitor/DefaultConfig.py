#!/usr/bin/env python
"""
_DefaultConfig_

the common configuration file for PilotMonitorComponent

"""

import os
import sys
import getopt
import logging

from WMCore.Agent.Configuration import Configuration

#  //
# // Find and load the Configuration
#//
config = Configuration()
config.component_("PilotMonitorComponent")

#The log level of the component.
config.PilotMonitorComponent.logLevel = 'DEBUG'

# maximum number of threads we want to deal
# with messages per pool.
config.PilotMonitorComponent.maxThreads = 30

#emulation mode
config.PilotMonitorComponent.emulationMode = True

#component dir
#config.PilotMonitorComponent.componentDir = '/data/khawar/prototype/PilotMonitor'

config.PilotMonitorComponent.tqAddress = 'vocms13.cern.ch:8030'

config.PilotMonitorComponent.monitorPlugin = 'T0LSFPilotMonitor'

# depending on the application an operator can reconfigure what we use.
# but these are the default settings.
config.PilotMonitorComponent.pilotMonitorHandler = 'PilotMonitor.Handler.PilotMonitorHandler'

