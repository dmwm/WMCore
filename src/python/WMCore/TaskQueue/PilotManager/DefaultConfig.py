#!/usr/bin/env python
"""
_StartComponent_

Start the component, reading its configuration from
the common configuration file, which is accessed by environment variable

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
config.component_("PilotManagerComponent")

#The log level of the component.
config.PilotManagerComponent.logLevel = 'DEBUG'

#emulation mode
config.PilotManagerComponent.emulationMode = True

#component dir
#config.PilotManagerComponent.componentDir = '/data/khawar/prototype/PilotManager'

config.PilotManagerComponent.tqAddress = 'vocms13.cern.ch:8030'

config.PilotManagerComponent.defaultScheduler = 'LSF'
config.PilotManagerComponent.pilotTar = 'Pilot.tar.gz'
config.PilotManagerComponent.tarPath = '/data/khawar/PAProd/0_12_15p1/prodAgent'
config.PilotManagerComponent.pilotCode = '/data/khawar/prototype/LSFPilot_JSON'

# depending on the application an operator can reconfigure what we use.
# but these are the default settings.
config.PilotManagerComponent.pilotManagerHandler = 'PilotManager.Handler.PilotManagerHandler'

