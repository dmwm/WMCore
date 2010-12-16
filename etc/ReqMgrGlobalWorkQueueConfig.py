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

# This is the path to the CMS software installation on the location machine.
cmsPath = "/uscmst1/prod/sw/cms"

# The couch username and password needs to be added.  The GroupUser and
# ConfigCache couch apps need to be installed into the configcache couch
# database.  The JobDump couchapp needs to be installed into the jobdump
# database.
couchURL = "http://USERNAMEPASSWORD@COUCHSERVER:5984"
configCacheDBName = "wmagent_configcache"
jobDumpDBName = "wmagent_jobdump"
reqMgrDBName = "reqmgrdb"

# Agent name and team name.
agentName = "WMAgentCommissioning"
teamName = "cmsdataops"

# Root of the YUI javascript library.
yuiRoot = "http://yui.yahooapis.com/2.8.0r4"

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
config.ReqMgr.templates = os.path.join(getWMBASE(),
                                       "src/templates/WMCore/WebTools")
config.ReqMgr.requestor = userName
config.ReqMgr.admin = userEmail
config.ReqMgr.title = "CMS Request Manager"
config.ReqMgr.description = "CMS Request Manager"
config.ReqMgr.couchURL = couchURL
config.ReqMgr.default_expires = 0

config.ReqMgr.section_("security")
config.ReqMgr.security.dangerously_insecure = True

views = config.ReqMgr.section_('views')
active = views.section_('active')

active.section_("reqMgrBrowser")
active.reqMgrBrowser.object = "WMCore.HTTPFrontEnd.RequestManager.ReqMgrBrowser"
active.reqMgrBrowser.reqMgrHost = reqMgrUrl
active.reqMgrBrowser.couchUrl = config.ReqMgr.couchURL
active.reqMgrBrowser.configDBName = configCacheDBName
active.reqMgrBrowser.yuiroot = yuiRoot

active.section_('RequestOverview')
active.RequestOverview.object = 'WMCore.HTTPFrontEnd.RequestManager.RequestOverview'
active.RequestOverview.templates = os.path.join(getWMBASE(), 'src/templates/WMCore/WebTools')
active.RequestOverview.javascript = os.path.join(getWMBASE(), 'src/javascript')
active.RequestOverview.html = os.path.join(getWMBASE(), 'src/html')

active.section_("reqMgr")
active.reqMgr.object = "WMCore.WebTools.RESTApi"
active.reqMgr.section_("model")
active.reqMgr.model.object = "WMCore.HTTPFrontEnd.RequestManager.ReqMgrRESTModel"
active.reqMgr.model.reqMgrHost = reqMgrUrl
active.reqMgr.model.couchUrl = couchURL
active.reqMgr.model.workloadCouchDB = reqMgrDBName
active.reqMgr.section_("formatter") 
active.reqMgr.formatter.object = "WMCore.WebTools.RESTFormatter"
active.reqMgr.formatter.templates = config.ReqMgr.templates

active.section_("WebRequestSchema")
active.WebRequestSchema.object = "WMCore.HTTPFrontEnd.RequestManager.WebRequestSchema"
active.WebRequestSchema.reqMgrHost = reqMgrUrl
active.WebRequestSchema.cmsswDefaultVersion = "CMSSW_3_8_6"
active.WebRequestSchema.couchUrl = couchURL
active.WebRequestSchema.configCacheDBName = configCacheDBName
active.WebRequestSchema.templates = config.ReqMgr.templates
active.WebRequestSchema.yuiroot = yuiRoot

config.component_("WorkQueueManager")
config.WorkQueueManager.namespace = "WMComponent.WorkQueueManager.WorkQueueManager"
config.WorkQueueManager.componentDir = os.path.join(config.General.workDir, "WorkQueueManager")
config.WorkQueueManager.level = "GlobalQueue"
config.WorkQueueManager.pollInterval = 10
config.WorkQueueManager.queueParams = {'LocationRefreshInterval': 1800}
config.WorkQueueManager.reqMgrConfig = {'teamName' : config.Agent.teamName,
                                        'endpoint': "%s/reqMgr/" % reqMgrUrl}

config.webapp_('WorkQueueService')
config.WorkQueueService.default_expires = 0
config.WorkQueueService.componentDir = os.path.join(config.General.workDir, "WorkQueueService")
config.WorkQueueService.Webtools.port = globalWorkQueuePort
config.WorkQueueService.Webtools.host = serverHostName
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
workqueue.section_('formatter')
workqueue.formatter.object = 'WMCore.HTTPFrontEnd.WorkQueue.WorkQueueRESTFormatter'
workqueue.serviceModules = ['WMCore.HTTPFrontEnd.WorkQueue.Services.WorkQueueService',
                            'WMCore.HTTPFrontEnd.WorkQueue.Services.WorkQueueMonitorService']
workqueue.queueParams = getattr(config.WorkQueueManager, 'queueParams', {})
workqueue.queueParams.setdefault('CacheDir', config.General.workDir + '/WorkQueueManager/wf')
workqueue.queueParams.setdefault('QueueURL', 'http://%s:%s/%s' % (serverHostName,
                                                                  config.WorkQueueService.Webtools.port,
                                                                  'workqueue'))

workqueuemonitor = active.section_('workqueuemonitor')
workqueuemonitor.object = 'WMCore.HTTPFrontEnd.WorkQueue.WorkQueueMonitorPage'
workqueuemonitor.templates = os.path.join(getWMBASE(), 'src/templates/WMCore/WebTools/WorkQueue')
workqueuemonitor.javascript = os.path.join(getWMBASE(), 'src/javascript/')
workqueuemonitor.html = os.path.join(getWMBASE(), 'src/html/')
workqueue.queueParams = getattr(config.WorkQueueManager, 'queueParams', {})
