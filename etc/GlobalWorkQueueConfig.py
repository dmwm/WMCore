########################
# the default config should work out of the box with minimal change
# Under the '## User specific parameter' line need to be changed to make the config correctly
########################

"""
WMAgent Configuration

Sample WMAgent configuration.
"""

__revision__ = "$Id: GlobalWorkQueueConfig.py,v 1.9 2010/06/29 20:45:40 sryu Exp $"
__version__ = "$Revision: 1.9 $"
import WMCore.WMInit
from os import path
from WMCore.Configuration import Configuration

config = Configuration()

config.section_("Agent")
## User specific parameter
config.Agent.hostName = "cmssrv52.fnal.gov"
## User specific parameter
config.Agent.contact = "stuart.wakefield@imperial.ac.uk"
## User specific parameter
config.Agent.teamName = "DMWM"
## User specific parameter
config.Agent.agentName = "GlobalWorkQueue"
config.Agent.useMsgService = False
config.Agent.useTrigger = False

config.section_("General")
## User specific parameter
config.General.workDir = "/home/sfoulkes/WMAgent/work"

config.section_("CoreDatabase")
## User specific parameter
config.CoreDatabase.socket = "/opt/MySQL-5.1/var/lib/mysql/mysql.sock"
## User specific parameter
config.CoreDatabase.connectUrl = "mysql://sfoulkes:@localhost/WMAgentDB_sfoulkes"

config.component_("WorkQueueManager")
config.WorkQueueManager.namespace = "WMComponent.WorkQueueManager.WorkQueueManager"
config.WorkQueueManager.componentDir = path.join(config.General.workDir, "WorkQueueManager")
config.WorkQueueManager.level = "GlobalQueue"
# needs to change to proper request manager host
## User specific parameter
config.WorkQueueManager.serviceUrl = 'cmssrv52.fnal.gov:8888'
config.WorkQueueManager.queueParams = {'LocationRefreshInterval': 10}
config.WorkQueueManager.reqMgrConfig = {'teamName' : config.Agent.teamName}

config.webapp_('WorkQueueService')
# port number which workqueue service will run
## User specific parameter
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

# REST service for WMComponents running (WorkQueueManager in this case)
wmagent = active.section_('wmagent')
# The class to load for this view/page
wmagent.object = 'WMCore.WebTools.RESTApi'
wmagent.templates = path.join( WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools/')
wmagent.section_('model')
wmagent.model.object = 'WMCore.HTTPFrontEnd.Agent.AgentRESTModel'
wmagent.section_('formatter')
wmagent.formatter.object = 'WMCore.WebTools.RESTFormatter'

workqueuemonitor = active.section_('workqueuemonitor')
workqueuemonitor.object = 'WMCore.HTTPFrontEnd.WorkQueue.WorkQueueMonitorPage'
workqueuemonitor.templates = path.join(WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools')
workqueuemonitor.javascript = path.join(WMCore.WMInit.getWMBASE(), 'src/javascript/')
workqueuemonitor.html = path.join(WMCore.WMInit.getWMBASE(), 'src/html/')

workqueue.queueParams = getattr(config.WorkQueueManager, 'queueParams', {})
