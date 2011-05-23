#!/usr/bin/env python
"""
Example configuration for CRABServer
"""

import os

from WMCore.WMInit import getWMBASE
from WMCore.Configuration import Configuration

serverHostName = "HOST_NAME"
CRABInterfacePort = 8888
workDirectory = "/CRABInterface/worgkin/dir"
couchURL = "http://user:passwd@host:5984"
workloadCouchDB = "workloadCouchDB"
jsmCacheDBName = "jsmCacheDBName"

databaseUrl = "mysql://root@localhost/ReqMgrDB"
databaseSocket = "/path/mysql.sock"

config = Configuration()

config.section_("General")
config.General.workDir = workDirectory
config.section_("CoreDatabase")
config.CoreDatabase.connectUrl = databaseUrl
config.CoreDatabase.socket = databaseSocket

config.section_("Agent")
config.Agent.hostName = serverHostName
#config.Agent.contact = userEmail
#config.Agent.teamName = teamName
#config.Agent.agentName = agentName
#config.Agent.useMsgService = False
#config.Agent.useTrigger = False
#config.Agent.useHeartbeat = False
config.webapp_("CRABInterface")

config.CRABInterface.componentDir = config.General.workDir + "/CRABInterface"
config.CRABInterface.Webtools.host = serverHostName
config.CRABInterface.Webtools.port = CRABInterfacePort
config.CRABInterface.templates =os.path.join(getWMBASE(),
                                       "src/templates/WMCore/WebTools")

config.CRABInterface.configCacheCouchURL = "YourConfigCacheUrl"
config.CRABInterface.configCacheCouchDB = "configCacheCouchDB-Name"

## TODO once the deploy model has been defined.. we will clarify how 
##      to deal with these params
config.CRABInterface.agentDN = "/Your/Agent/DN.here/"
config.CRABInterface.SandBoxCache_endpoint = "USB-cache-endpoint"
config.CRABInterface.SandBoxCache_port  = "PORT"
config.CRABInterface.SandBoxCache_basepath ="/Path/if/Needed"

config.CRABInterface.views.active.crab.jsmCacheCouchURL = couchURL
config.CRABInterface.views.active.crab.jsmCacheCouchDB = jsmCacheDBName
##

config.CRABInterface.admin = "admin@mail.address"
config.CRABInterface.title = "CRAB REST Interface"
config.CRABInterface.description = "rest interface for crab"
config.CRABInterface.instance = "Analysis WMAGENT"

config.CRABInterface.section_("security")
config.CRABInterface.security.dangerously_insecure = True

config.CRABInterface.section_('views')
config.CRABInterface.views.section_('active')
config.CRABInterface.views.active.section_('crab')
config.CRABInterface.views.active.crab.section_('model')
config.CRABInterface.views.active.crab.section_('formatter')
config.CRABInterface.views.active.crab.object = 'WMCore.WebTools.RESTApi'
config.CRABInterface.views.active.crab.templates = os.path.join(getWMBASE(),
                                       "src/templates/WMCore/WebTools/")
config.CRABInterface.views.active.crab.model.couchUrl = couchURL
config.CRABInterface.views.active.crab.model.workloadCouchDB = workloadCouchDB
config.CRABInterface.views.active.crab.model.object = 'CRABServer.CRABRESTModel'
config.CRABInterface.views.active.crab.formatter.object = 'WMCore.WebTools.RESTFormatter'

## TODO once the deploy model has been defined.. we will clarify how 
##      to deal with these params
config.CRABInterface.views.active.crab.jsmCacheCouchURL = couchURL
config.CRABInterface.views.active.crab.jsmCacheCouchDB = jsmCacheDBName
##
