#!/usr/bin/env python
"""
Global WorkQueue and ReqMgr config.

"""




import os

import WMCore.WMInit
from WMCore.Configuration import Configuration

# The following parameters may need to be changed, nothing else in the config
# will.  The server host name needs to match the machine this is running on, the
# port numbers will not need to be changed unless there is more than one
# ReqMgr/GlobalWorkQueue running on the machine.
serverHostName = "cmssrv52.fnal.gov"
reqMgrPort = 8686
globalWorkQueuePort = 8570

# The work directory and database need to be separate from the WMAgent
# installation.
workDirectory = "/storage/local/data1/wmagent/reqwork"
databaseUrl = "mysql://sfoulkes:@localhost/ReqMgrDB_sfoulkes"
databaseSocket = "/opt/MySQL-5.1/var/lib/mysql/mysql.sock"

# This needs to match the information that is input with the requestDBAdmin
# utility.
userName = "sfoulkes"
userEmail = "sfoulkes@fnal.gov"

# This is the path to the CMS software installation on the location machine.
cmsPath = "/uscmst1/prod/sw/cms"

# The couch username and password needs to be added.
couchURL = "http://USERNAME:PASSWORD@cmssrv52.fnal.gov:5984"
configCacheDBName = "wmagent_config_cache"


config = Configuration()

config.section_("Agent")
config.Agent.hostName = serverHostName
config.Agent.contact = userEmail
config.Agent.teamName = "cmsdataops"
config.Agent.agentName = "WMAgentCommissioning"
config.Agent.useMsgService = False
config.Agent.useTrigger = False

config.section_("General")
config.General.workDir = workDirectory

config.section_("CoreDatabase")
config.CoreDatabase.connectUrl = databaseUrl
config.CoreDatabase.dbsock = databaseSocket

config.webapp_("ReqMgr")
config.ReqMgr.componentDir = os.path.join(config.General.workDir, "ReqMgr")
config.ReqMgr.Webtools.host = serverHostName
config.ReqMgr.Webtools.port = reqMgrPort
config.ReqMgr.templates = os.path.join(WMCore.WMInit.getWMBASE(),
                                       "src/templates/WMCore/WebTools")
config.ReqMgr.requestor = userName
config.ReqMgr.admin = userEmail
config.ReqMgr.title = "CMS Request Manager"
config.ReqMgr.description = "CMS Request Manager"
config.ReqMgr.couchURL = couchURL

views = config.ReqMgr.section_('views')
active = views.section_('active')

active.section_("download")
active.download.object = "WMCore.HTTPFrontEnd.Downloader"
active.download.dir = config.ReqMgr.componentDir

active.section_("reqMgrBrowser")
active.reqMgrBrowser.object = "WMCore.HTTPFrontEnd.RequestManager.ReqMgrBrowser"
active.reqMgrBrowser.reqMgrHost = "%s:%s" % (serverHostName, reqMgrPort)
active.reqMgrBrowser.workloadCache = active.download.dir
active.reqMgrBrowser.configCacheUrl = config.ReqMgr.couchURL

active.section_("reqMgr")
active.reqMgr.object = "WMCore.WebTools.RESTApi"
active.reqMgr.section_("model")
active.reqMgr.model.object = "WMCore.HTTPFrontEnd.RequestManager.ReqMgrRESTModel"
active.reqMgr.model.workloadCache = active.download.dir
active.reqMgr.model.reqMgrHost = "%s:%s" % (serverHostName, reqMgrPort)
active.reqMgr.section_("formatter") 
active.reqMgr.formatter.object = "WMCore.WebTools.RESTFormatter"
active.reqMgr.formatter.templates = config.ReqMgr.templates

active.section_("WebRequestSchema")
active.WebRequestSchema.object = "WMCore.HTTPFrontEnd.RequestManager.WebRequestSchema"
active.WebRequestSchema.reqMgrHost = "%s:%s" % (serverHostName, reqMgrPort)
active.WebRequestSchema.cmsswInstallation = cmsPath
active.WebRequestSchema.cmsswDefaultVersion = "CMSSW_3_5_8"
active.WebRequestSchema.configCacheUrl = couchURL
active.WebRequestSchema.configCacheDBName = configCacheDBName
active.WebRequestSchema.templates = config.ReqMgr.templates

config.component_("WorkQueueManager")
config.WorkQueueManager.namespace = "WMComponent.WorkQueueManager.WorkQueueManager"
config.WorkQueueManager.componentDir = os.path.join(config.General.workDir, "WorkQueueManager")
config.WorkQueueManager.level = "GlobalQueue"
config.WorkQueueManager.pollInterval = 10
config.WorkQueueManager.queueParams = {'LocationRefreshInterval': 1800}
config.WorkQueueManager.reqMgrConfig = {'teamName' : config.Agent.teamName,
                                        'endpoint': "http://%s:%s/reqMgr/" % (serverHostName, reqMgrPort)}

config.webapp_('WorkQueueService')
config.WorkQueueService.componentDir = os.path.join(config.General.workDir, "WorkQueueService")
config.WorkQueueService.Webtools.port = globalWorkQueuePort
config.WorkQueueService.Webtools.host = serverHostName
config.WorkQueueService.templates = os.path.join(WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools')
config.WorkQueueService.admin = config.Agent.contact
config.WorkQueueService.title = 'WorkQueue Data Service'
config.WorkQueueService.description = 'Provide WorkQueue related service call'
config.WorkQueueService.section_('views')
active = config.WorkQueueService.views.section_('active')
workqueue = active.section_('workqueue')
workqueue.object = 'WMCore.WebTools.RESTApi'
workqueue.templates = os.path.join(WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools/')
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
workqueuemonitor.templates = os.path.join(WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools')
workqueuemonitor.javascript = os.path.join(WMCore.WMInit.getWMBASE(), 'src/javascript/')
workqueuemonitor.html = os.path.join(WMCore.WMInit.getWMBASE(), 'src/html/')

workqueue.queueParams = getattr(config.WorkQueueManager, 'queueParams', {})
