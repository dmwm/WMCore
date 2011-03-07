#!/usr/bin/env python
"""
_GlobalWorkQueueConfig_

Global WorkQueue config.
"""

import os

from WMCore.WMInit import getWMBASE
from WMCore.Configuration import Configuration

# The following parameters may need to be changed but nothing else in the config
# will.  The server host name needs to match the machine this is running on, the
# port numbers will not need to be changed unless there is more than one
# ReqMgr/GlobalWorkQueue running on the machine.
serverHostName = "SERVER_HOSTNAME"
reqMgrHostName = "REQMGR_HOSTNAME"
reqMgrPort = 8687
globalWorkQueuePort = 8571

# The work directory and database need to be separate from the WMAgent
# installation.
workDirectory = "WORKQUEUE_WORK_DIRECTORY"
databaseUrl = "mysql://DBSUSER:DBSPASSWORD@localhost/ReqMgrDB"
databaseSocket = "/opt/MySQL-5.1/var/lib/mysql/mysql.sock"

# Agent name and team name.
agentName = "WMAgentCommissioning"
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
config.WorkQueueManager.level = "GlobalQueue"
config.WorkQueueManager.pollInterval = 120
config.WorkQueueManager.queueParams = {'LocationRefreshInterval': 1800}
config.WorkQueueManager.reqMgrConfig = {'teamName' : config.Agent.teamName,
                                        'endpoint': "%s/reqMgr/" % reqMgrUrl}

config.webapp_('WorkQueueService')
config.WorkQueueService.default_expires = 0
config.WorkQueueService.componentDir = os.path.join(config.General.workDir, "WorkQueueService")
config.WorkQueueService.Webtools.port = globalWorkQueuePort
config.WorkQueueService.Webtools.host = serverHostName
config.WorkQueueService.Webtools.environment = "devel"
config.WorkQueueService.templates = os.path.join(getWMBASE(), 'src/templates/WMCore/WebTools')
config.WorkQueueService.admin = "cmsdataops@cern.ch"
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
