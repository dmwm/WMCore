#!/usr/bin/env python
"""
_StartComponent_

Start the RequestInjector component, reading its configuration from
the common configuration file, which is accessed by environment variable

"""

import os
import sys
import getopt

from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdAgentCore.Configuration import loadProdAgentConfiguration
from WMCore.Agent.Configuration import loadConfigurationFile
from ProdAgentCore.CreateDaemon import createDaemon
from ProdAgentCore.PostMortem import runWithPostMortem
from PilotManager.PilotManagerComponent import PilotManagerComponent

#  //
# // Find and load the Configuration
#//

try:
    config = loadProdAgentConfiguration()
    compCfg = config.getConfig("PilotManager")
except StandardError, ex:
    msg = "Error reading configuration:\n"
    msg += str(ex)
    raise RuntimeError, msg

compCfg['ComponentDir'] = os.path.expandvars(compCfg['ComponentDir'])

config = loadConfigurationFile(compCfg['defaultConfig'])
config.PilotManagerComponent.componentDir=compCfg['ComponentDir']
config.PilotManagerComponent.plugin=compCfg['plugin']
config.section_("CoreDatabase")
#settig up the configuration for PilotMonitor
config.CoreDatabase.dialect  = dbConfig['dbType']
config.CoreDatabase.socket   = dbConfig['socketFileLocation']
config.CoreDatabase.user     = dbConfig['user']
config.CoreDatabase.passwd   = dbConfig['passwd']
config.CoreDatabase.hostname = dbConfig['host']
#config.CoreDatabase.name     = dbConfig['dbName']
config.CoreDatabase.name     = 'WMCoreDB'

#print config

#  //
# // Initialise and start the component
#//
print "Starting PilotManager Component..."
pilotManager = PilotManagerComponent(config)
pilotManager.prepareToStart()
pilotManager.startDeamon()
