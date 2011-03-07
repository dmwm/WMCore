#!/usr/bin/env python
"""
_ReqMgrGlobalWorkQueueConfig_

Global WorkQueue and ReqMgr config.
"""

import os

from WMCore.WMInit import getWMBASE
from WMCore.Configuration import Configuration

# The following parameters may need to be changed but nothing else in the config
# will.  The server host name needs to match the machine this is running on, the
# port numbers will not need to be changed unless there is more than one
# ReqMgr/GlobalWorkQueue running on the machine.
serverHostName = "REQMGR_SERVER_HOSTNAME"
reqMgrPort = 8687
globalWorkQueuePort = 8571

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
configCacheDBName = "wmagent_configcache"
jobDumpDBName = "wmagent_jobdump"
reqMgrDBName = "reqmgrdb"
workqueueDBName = 'workqueue'
workqueueInboxDbName = 'workqueue_inbox'


# Agent name and team name.
agentName = "WMAgentCommissioning"
teamName = "cmsdataops"

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

config.section_("General")
config.General.workDir = workDirectory

config.section_("CoreDatabase")
config.CoreDatabase.connectUrl = databaseUrl
config.CoreDatabase.socket = databaseSocket

config.webapp_("ReqMgr")
reqMgrUrl = "http://%s:%s" % (serverHostName, reqMgrPort)
config.ReqMgr.componentDir = os.path.join(config.General.workDir, "ReqMgr")
config.ReqMgr.Webtools.host = serverHostName
config.ReqMgr.Webtools.port = reqMgrPort
config.ReqMgr.Webtools.environment = "devel"
config.ReqMgr.templates = os.path.join(getWMBASE(),
                                       "src/templates/WMCore/WebTools/RequestManager")
config.ReqMgr.requestor = userName
config.ReqMgr.admin = userEmail
config.ReqMgr.title = "CMS Request Manager"
config.ReqMgr.description = "CMS Request Manager"
config.ReqMgr.couchURL = couchURL
config.ReqMgr.default_expires = 0
config.ReqMgr.yuiroot = yuiRoot
config.ReqMgr.couchUrl = couchURL
config.ReqMgr.configDBName = configCacheDBName

config.ReqMgr.section_("security")
config.ReqMgr.security.dangerously_insecure = True

views = config.ReqMgr.section_('views')
active = views.section_('active')

active.section_('GlobalMonitor')
active.GlobalMonitor.object = 'WMCore.HTTPFrontEnd.GlobalMonitor.GlobalMonitorPage'
active.GlobalMonitor.templates = os.path.join(getWMBASE(), 'src/templates/WMCore/WebTools')
active.GlobalMonitor.javascript = os.path.join(getWMBASE(), 'src/javascript')
active.GlobalMonitor.html = os.path.join(getWMBASE(), 'src/html')

active.section_('monitorSvc')
active.monitorSvc.serviceURL = "%s/reqmgr/reqMgr" % reqMgrUrl
active.monitorSvc.serviceLevel = 'RequestManager'
active.monitorSvc.section_('model')
active.monitorSvc.section_('formatter')
active.monitorSvc.object = 'WMCore.WebTools.RESTApi'
active.monitorSvc.model.object = 'WMCore.HTTPFrontEnd.GlobalMonitor.GlobalMonitorRESTModel'
active.monitorSvc.default_expires = 0 # no caching
active.monitorSvc.formatter.object = 'WMCore.WebTools.RESTFormatter'

config.component_("WorkQueueManager")
config.WorkQueueManager.namespace = "WMComponent.WorkQueueManager.WorkQueueManager"
config.WorkQueueManager.componentDir = os.path.join(config.General.workDir, "WorkQueueManager")
config.WorkQueueManager.couchurl = couchURL
config.WorkQueueManager.dbname = workqueueDBName
config.WorkQueueManager.inboxDatabase = workqueueInboxDbName
config.WorkQueueManager.level = "GlobalQueue"
config.WorkQueueManager.pollInterval = 600
config.WorkQueueManager.queueParams = {}
config.WorkQueueManager.reqMgrConfig = {'teamName' : config.Agent.teamName,
                                        'endpoint': "%s/reqMgr/" % reqMgrUrl}

config.webapp_('WorkQueueService')
config.WorkQueueService.default_expires = 0
config.WorkQueueService.componentDir = os.path.join(config.General.workDir, "WorkQueueService")
config.WorkQueueService.Webtools.port = globalWorkQueuePort
config.WorkQueueService.Webtools.host = serverHostName
config.WorkQueueService.Webtools.environment = "devel"
config.WorkQueueService.templates = os.path.join(getWMBASE(), 'src/templates/WMCore/WebTools')
config.WorkQueueService.admin = config.Agent.contact
config.WorkQueueService.title = 'WorkQueue Data Service'
config.WorkQueueService.description = 'Provide WorkQueue related service call'

config.WorkQueueService.section_("security")
config.WorkQueueService.security.dangerously_insecure = True

config.WorkQueueService.section_('views')
active = config.WorkQueueService.views.section_('active')
workqueue = active.section_('workqueue')
workqueue.object = 'WMCore.WebTools.RESTApi'
workqueue.templates = os.path.join(getWMBASE(), 'src/templates/WMCore/WebTools/')
workqueue.section_('model')
workqueue.model.object = 'WMCore.HTTPFrontEnd.WorkQueue.WorkQueueRESTModel'
workqueue.level = config.WorkQueueManager.level
workqueue.section_('formatter')
workqueue.formatter.object = 'WMCore.WebTools.RESTFormatter'
workqueue.serviceModules = ['WMCore.HTTPFrontEnd.WorkQueue.Services.WorkQueueService',
                            'WMCore.HTTPFrontEnd.WorkQueue.Services.WorkQueueMonitorService']
workqueue.queueParams = getattr(config.WorkQueueManager, 'queueParams', {})
workqueue.queueParams.setdefault('QueueURL', 'http://%s:%s/%s' % (serverHostName,
                                                                  config.WorkQueueService.Webtools.port,
                                                                  'workqueue'))
workqueuemonitor = active.section_('workqueuemonitor')
workqueuemonitor.object = 'WMCore.HTTPFrontEnd.WorkQueue.WorkQueueMonitorPage'
workqueuemonitor.templates = os.path.join(getWMBASE(), 'src/templates/WMCore/WebTools/WorkQueue')
workqueuemonitor.javascript = os.path.join(getWMBASE(), 'src/javascript/')
workqueuemonitor.html = os.path.join(getWMBASE(), 'src/html/')

