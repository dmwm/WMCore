#!/usr/bin/env python
"""
_StartComponent_

Start the RequestInjector component, reading its configuration from
the common configuration file, which is accessed by environment variable

"""

import os
import sys
import getopt

from ProdAgentCore.Configuration import loadProdAgentConfiguration
from WMCore.Agent.Configuration import loadConfigurationFile
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdAgentCore.CreateDaemon import createDaemon
from ProdAgentCore.PostMortem import runWithPostMortem
from PilotMonitor.PilotMonitorComponent import PilotMonitorComponent

#  //
# // Find and load the Configuration
#//

try:
    config = loadProdAgentConfiguration()
    compCfg = config.getConfig("PilotMonitor")
    tqCfg = config.getConfig("TaskQueue");
except StandardError, ex:
    msg = "Error reading configuration:\n"
    msg += str(ex)
    raise RuntimeError, msg

print compCfg
compCfg['ComponentDir'] = os.path.expandvars(compCfg['ComponentDir'])
#settig up the configuration for PilotMonitor
config = loadConfigurationFile(compCfg['defaultConfig'])

#poll Interval
if ( compCfg.has_key("pollInterval") ): 
    config.PilotMonitorComponent.pollInterval = compCfg['pollInterval']

#plugin
if ( compCfg.has_key("plugin") ):
    config.PilotMonitorComponent.plugin = compCfg['plugin']

config.PilotMonitorComponent.componentDir = compCfg['ComponentDir']
config.PilotMonitorComponent.TQConfig     = tqCfg['TaskQueueConfFile']
#set config with DB common settings
config.section_("CoreDatabase")
config.CoreDatabase.dialect = dbConfig['dbType']
config.CoreDatabase.socket = dbConfig['socketFileLocation']
config.CoreDatabase.user = dbConfig['user']
config.CoreDatabase.passwd = dbConfig['passwd']
config.CoreDatabase.hostname = dbConfig['host']
config.CoreDatabase.name = 'WMCoreDB' 

print config

#  //
# // Initialise and start the component
#//
print "Starting PilotMonitor Component..."
pilotMonitor = PilotMonitorComponent(config)
pilotMonitor.prepareToStart()
#pilotMonitor.startComponent()
#pilotMonitor.publishMonitorPilots()
pilotMonitor.startDeamon()
