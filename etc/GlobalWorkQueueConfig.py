#!/usr/bin/env python
"""
WMAgent Configuration

Sample WMAgent configuration.
"""

__revision__ = "$Id: GlobalWorkQueueConfig.py,v 1.6 2010/05/21 21:36:18 sryu Exp $"
__version__ = "$Revision: 1.6 $"
from os import path

from WMCore.Configuration import Configuration
config = Configuration()

config.section_("Agent")
config.Agent.hostName = "cmssrv52.fnal.gov"
config.Agent.contact = "stuart.wakefield@imperial.ac.uk"
config.Agent.teamName = "DMWM"
config.Agent.agentName = "GlobalWorkQueue"

config.section_("General")
config.General.workDir = "/home/sfoulkes/WMAgent/work"

config.section_("CoreDatabase")
config.CoreDatabase.socket = "/opt/MySQL-5.1/var/lib/mysql/mysql.sock"
config.CoreDatabase.connectUrl = "mysql://sfoulkes:@localhost/WMAgentDB_sfoulkes"

config.component_("WorkQueueManager")
config.WorkQueueManager.namespace = "WMComponent.WorkQueueManager.WorkQueueManager"
config.WorkQueueManager.componentDir = path.join(config.General.workDir, "WorkQueueManager")
config.WorkQueueManager.level = "GlobalQueue"
# needs to change to request manager host
config.WorkQueueManager.serviceUrl = 'cmssrv52.fnal.gov:8888'
config.WorkQueueManager.queueParams = {'LocationRefreshInterval': 10}

config.webapp_('WorkQueueService')
config.WorkQueueService.server.port = 8570
config.WorkQueueService.server.host = config.Agent.hostName
config.WorkQueueService.templates = path.join(WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools')
config.WorkQueueService.admin = config.Agent.contact
config.WorkQueueService.title = 'WorkQueue Data Service'
config.WorkQueueService.description = 'Provide WorkQueue related service call'
config.WorkQueueService.section_('views')
active = config.WorkQueueService.views.section_('active')
workqueue = active.section_('workqueue')
workqueue.object = 'WMCore.WebTools.RESTApi'
workqueue.templates = path.join(WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools/')
workqueue.section_('model')
workqueue.model.object = 'WMCore.HTTPFrontEnd.WorkQueue.WorkQueueRESTModel'
workqueue.section_('formatter')
workqueue.formatter.object = 'WMCore.HTTPFrontEnd.WorkQueue.WorkQueueRESTFormatter'
workqueue.serviceModules = ['WMCore.HTTPFrontEnd.WorkQueue.Services.WorkQueueService',
                            'WMCore.HTTPFrontEnd.WorkQueue.Services.WorkQueueMonitorService']
workqueue.queueParams = getattr(config.WorkQueueManager, 'queueParams', {})
workqueue.queueParams.setdefault('CacheDir', config.General.workDir + '/WorkQueueManager/wf')
workqueue.queueParams.setdefault('QueueURL', 'http://%s:%s/%s' % (config.Agent.hostName,
                                                                  config.WorkQueueService.server.port,
                                                                  'workqueue'))

workqueuemonitor = active.section_('workqueuemonitor')
workqueuemonitor.object = 'WMCore.HTTPFrontEnd.WorkQueue.WorkQueueMonitorPage'
workqueuemonitor.templates = path.join(WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools')
workqueuemonitor.javascript = path.join(WMCore.WMInit.getWMBASE(), 'src/javascript/WMCore/WebTools')
workqueuemonitor.html = path.join(WMCore.WMInit.getWMBASE(), 'src/html/WorkQueue')

workqueue.queueParams = getattr(config.WorkQueueManager, 'queueParams', {})
