#!/usr/bin/env python
"""
_ReqMgrConfig_

ReqMgr config.

WARNING: This config is only used for wmagent all-in-one test. Will be deplicated.
Doesn't replex the current reqmgr config which is under deployment
"""

import os

from WMCore.WMInit import getWMBASE
from WMCore.Configuration import Configuration
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrConfiguration import reqMgrConfig

# The following parameters may need to be changed but nothing else in the config
# will.  The server host name needs to match the machine this is running on, the
# port numbers will not need to be changed unless there is more than one
# ReqMgr/GlobalWorkQueue running on the machine.
serverHostName = "REQMGR_SERVER_HOSTNAME"
reqMgrPort = 8687

# The work directory and database need to be separate from the WMAgent
# installation.
workDirectory = "REQMGR_WORK_DIRECTORY"
databaseUrl = "mysql://DBSUSER:DBSPASSWORD@localhost/ReqMgrDB"
databaseSocket = "/opt/MySQL-5.1/var/lib/mysql/mysql.sock"

# This needs to match the information that is input with the requestDBAdmin
# utility.
userName = "OPERATOR NAME"
userEmail = "OPERATOR EMAIL"

# The couch username and password needs to be added.  The GroupUser and
# ConfigCache couch apps need to be installed into the configcache couch
# database.  The JobDump couchapp needs to be installed into the jobdump
# database.
couchURL = "http://USERNAMEPASSWORD@COUCHSERVER:5984"
configCacheDBName = "reqmgr_config_cache"
reqMgrDBName = "reqmgrdb"
wmstatDBName = "wmstats"

# Agent name and team name.
agentName = "WMAgentCommissioning"
teamName = "cmsdataops"

# Root of the YUI javascript library.
yuiRoot = "http://yui.yahooapis.com/2.8.0r4"

# URL of the list of sites from SiteDB
SITEDB = 'https://cmsweb.cern.ch/sitedb/json/index/CEtoCMSName?name'

# set cache directories (moved from manage script)
root = __file__.rsplit('/', 4)[0]
cache_dir = os.path.join(root, 'state', 'reqmgr', 'cache')
os.environ['WMCORE_CACHE_DIR'] = cache_dir
# manage script was also setting the "REQMGR_CACHE_DIR" variable which
# should only be needed when for storing separately from other WMCore services
# leave for reference
# os.environ["REQMGR_CACHE_DIR"] = cache_dir



# Nothing after this point should need to be changed.
config = Configuration()

config.section_("Agent")
config.Agent.hostName = serverHostName
config.Agent.contact = userEmail
config.Agent.teamName = teamName
config.Agent.agentName = agentName
config.Agent.useMsgService = False
config.Agent.useTrigger = False
config.Agent.useHeartbeat = False

config.section_("JobStateMachine")
config.JobStateMachine.couchurl = couchURL
config.JobStateMachine.configCacheDBName = configCacheDBName

config.section_("General")
config.General.workDir = workDirectory

config.section_("CoreDatabase")
config.CoreDatabase.connectUrl = databaseUrl
config.CoreDatabase.socket = databaseSocket

config.webapp_("reqmgr")
config += reqMgrConfig(
    port = reqMgrPort,
    reqMgrHost = serverHostName,
    user = userName,
    couchurl = couchURL,
    componentDir = os.path.join(config.General.workDir, "ReqMgr"),
    workloadCouchDB = reqMgrDBName,
    configCouchDB = configCacheDBName,
    wmstatCouchDB = wmstatDBName,
    connectURL = databaseUrl,
    startup = "wmcoreD")

config.reqmgr.admin = userEmail
config.reqmgr.section_("security")
config.reqmgr.security.dangerously_insecure = True