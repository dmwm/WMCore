#!/usr/bin/env python
"""
_GlobalWorkQueueConfig_

Global WorkQueue config.
WARNING: This config is only used for wmagent all-in-one test. Will be deplicated.
Doesn't replex the current workqueue config which is under deployment
"""

import os

from WMCore.Configuration import Configuration

# The following parameters may need to be changed but nothing else in the config
# will.  The server host name needs to match the machine this is running on, the
# port numbers will not need to be changed unless there is more than one
# ReqMgr/GlobalWorkQueue running on the machine.
serverHostName = "SERVER_HOSTNAME"
reqMgrHostName = "REQMGR_HOSTNAME"
reqMgrPort = 8687

# The work directory and database need to be separate from the WMAgent
# installation.
workDirectory = "WORKQUEUE_WORK_DIRECTORY"
databaseUrl = "mysql://DBSUSER:DBSPASSWORD@localhost/ReqMgrDB"
databaseSocket = "/opt/MySQL-5.1/var/lib/mysql/mysql.sock"

# The couch username and password needs to be added.  The GroupUser and
# ConfigCache couch apps need to be installed into the configcache couch
# database.  The JobDump couchapp needs to be installed into the jobdump
# database.  The GroupUser and ACDC couchapps needs to be install into the
# acdc database.
couchURL = "http://USERNAME:PASSWORD@COUCHSERVER:5984"
workqueueDBName = 'workqueue'
workqueueInboxDbName = 'workqueue_inbox'
wmstatDBName = "wmstats"

# Agent name and team name.
agentName = "WMAgent"
teamName = "cmsdataops"
contactName = "cmsdataops@cern.ch"

# Nothing after this point should need to be changed.
config = Configuration()

config.section_("Agent")
config.Agent.hostName = serverHostName
config.Agent.teamName = teamName
config.Agent.agentName = agentName
config.Agent.useMsgService = False
config.Agent.useTrigger = False
config.Agent.useHeartbeat = False
config.Agent.contact = contactName

config.section_("General")
config.General.workDir = workDirectory

config.section_("CoreDatabase")
config.CoreDatabase.connectUrl = databaseUrl
config.CoreDatabase.socket = databaseSocket

reqMgrUrl = "http://%s:%s" % (reqMgrHostName, reqMgrPort)

config.component_("WorkQueueManager")
config.WorkQueueManager.namespace = "WMComponent.WorkQueueManager.WorkQueueManager"
config.WorkQueueManager.componentDir = os.path.join(config.General.workDir, "WorkQueueManager")
config.WorkQueueManager.couchurl = couchURL
config.WorkQueueManager.dbname = workqueueDBName
config.WorkQueueManager.wmstatDBName = wmstatDBName
config.WorkQueueManager.inboxDatabase = workqueueInboxDbName
config.WorkQueueManager.level = "GlobalQueue"
config.WorkQueueManager.pollInterval = 600
config.WorkQueueManager.queueParams = {'WMStatsCouchUrl': "%s/%s" % (config.WorkQueueManager.couchurl.rstrip(), config.WorkQueueManager.wmstatDBName)}
config.WorkQueueManager.reqMgrConfig = {'teamName' : config.Agent.teamName,
                                        'endpoint': "%s/reqMgr/" % reqMgrUrl}
