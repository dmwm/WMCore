#!/usr/bin/env python
"""
_StartComponent_

Start the component, reading its configuration from
the common configuration file, which is accessed by environment variable

"""

from WMCore.Agent.Configuration import Configuration

#  //
# // Find and load the Configuration
#//
config = Configuration()
config.component_("PilotMonitorComponent")

#The log level of the component.
config.PilotMonitorComponent.logLevel = 'DEBUG'

#defaul poll interval
config.PilotMonitorComponent.pollInterval="00:20:00"
config.PilotMonitorComponent.plugin='PilotBlSimpleMonitor'
config.PilotMonitorComponent.pilotMonitorHandler = 'PilotMonitor.Handler.PilotMonitorHandler'

