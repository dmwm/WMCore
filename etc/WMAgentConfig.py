########################
# the default config should work out of the box with minimal change
# Under the '## User specific parameter' line need to be changed to make the config correctly
########################

"""
WMAgent Configuration

Sample WMAgent configuration.
"""




import os
import WMCore.WMInit

from WMCore.Configuration import Configuration
config = Configuration()

config.section_("Agent")
## User specific parameter
config.Agent.hostName = "cmssrv52.fnal.gov"
## User specific parameter
config.Agent.contact = "sfoulkes@fnal.gov"
## User specific parameter
config.Agent.teamName = "DMWM"
## User specific parameter
config.Agent.agentName = "WMAgentCommissioning"
config.Agent.useMsgService = False
config.Agent.useTrigger = False

config.section_("General")
config.General.workDir = "/storage/local/data1/wmagent/work"

config.section_("JobStateMachine")
## User specific parameter
config.JobStateMachine.couchurl = "http://cmssrv52.fnal.gov:8570"
## User specific parameter
config.JobStateMachine.couchDBName = "wmagent_commissioning"
## User specific parameter
config.JobStateMachine.configCacheDBName = "wmagent_config_cache"
config.JobStateMachine.default_retries = 5

config.section_("CoreDatabase")
## User specific parameter
config.CoreDatabase.socket = "/opt/MySQL-5.1/var/lib/mysql/mysql.sock"
## User specific parameter
config.CoreDatabase.connectUrl = "mysql://sfoulkes:@localhost/WMAgentDB_sfoulkes"

config.component_('WorkQueueManager')
config.WorkQueueManager.namespace = "WMComponent.WorkQueueManager.WorkQueueManager"
config.WorkQueueManager.componentDir = config.General.workDir + "/WorkQueueManager"
config.WorkQueueManager.level = 'LocalQueue'
config.WorkQueueManager.logLevel = 'INFO'
config.WorkQueueManager.serviceUrl = 'cmssrv52.fnal.gov:8570'
config.WorkQueueManager.pollInterval = 10
config.WorkQueueManager.queueParams = {"PopulateFilesets": True,
                                       "ParentQueue": "http://%s/workqueue/" % config.WorkQueueManager.serviceUrl,
                                       "QueueURL": "http://cmssrv52.fnal.gov:9997"} 

config.component_("DBSUpload")
config.DBSUpload.namespace = "WMComponent.DBSUpload.DBSUpload"
config.DBSUpload.componentDir = config.General.workDir + "/DBSUpload"
config.DBSUpload.logLevel = "DEBUG"
config.DBSUpload.maxThreads = 1
config.DBSUpload.pollInterval = 100
config.DBSUpload.workerThreads = 4


config.section_("DBSInterface")
config.DBSInterface.globalDBSUrl     = "https://cmsdbsprod.cern.ch:8443/cms_dbs_prod_global_writer/servlet/DBSServlet"
config.DBSInterface.globalDBSVersion = 'DBS_2_0_8'
config.DBSInterface.DBSUrl           = "https://cmst0dbs.cern.ch:8443/cms_dbs_prod_tier0_writer/servlet/DBSServlet"
config.DBSInterface.DBSVersion       = 'DBS_2_0_8'
config.DBSInterface.DBSBlockMaxFiles = 10
config.DBSInterface.DBSBlockMaxSize  = 9999999999
config.DBSInterface.DBSBlockMaxTime  = 21600
config.DBSInterface.MaxFilesToCommit = 10

config.component_("PhEDExInjector")
config.PhEDExInjector.namespace = "WMComponent.PhEDExInjector.PhEDExInjector"
config.PhEDExInjector.componentDir = config.General.workDir + "/PhEDExInjector"
config.PhEDExInjector.logLevel = "DEBUG"
config.PhEDExInjector.maxThreads = 1
config.PhEDExInjector.phedexurl = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/"
config.PhEDExInjector.pollInterval = 100

config.component_("JobAccountant")
config.JobAccountant.namespace = "WMComponent.JobAccountant.JobAccountant"
config.JobAccountant.componentDir = config.General.workDir + "/JobAccountant"
config.JobAccountant.logLevel = "DEBUG"
config.JobAccountant.workerThreads = 1
config.JobAccountant.pollInterval = 20

config.component_("JobCreator")
config.JobCreator.namespace = "WMComponent.JobCreator.JobCreator"
config.JobCreator.componentDir = config.General.workDir + "/JobCreator"
config.JobCreator.logLevel = "DEBUG"
config.JobCreator.maxThreads = 1
config.JobCreator.UpdateFromResourceControl = True
config.JobCreator.pollInterval = 10
config.JobCreator.jobCacheDir = config.General.workDir + "/JobCache"
config.JobCreator.defaultJobType = "Processing"
config.JobCreator.workerThreads = 2

config.component_("JobSubmitter")
config.JobSubmitter.namespace = "WMComponent.JobSubmitter.JobSubmitter"
config.JobSubmitter.componentDir = config.General.workDir + "/JobSubmitter"
config.JobSubmitter.logLevel = "DEBUG"
config.JobSubmitter.maxThreads = 1
config.JobSubmitter.pollInterval = 10
config.JobSubmitter.pluginName = "CondorGlideInPlugin"
config.JobSubmitter.pluginDir = "JobSubmitter.Plugins"
config.JobSubmitter.submitNode = "cmssrv52.fnal.gov"
config.JobSubmitter.submitDir = config.General.workDir + "/SubmitJDLs"
config.JobSubmitter.workerThreads = 1
config.JobSubmitter.jobsPerWorker = 100

config.component_("JobTracker")
config.JobTracker.namespace = "WMComponent.JobTracker.JobTracker"
config.JobTracker.componentDir  = config.General.workDir + "/JobTracker"
config.JobTracker.logLevel = "DEBUG"
config.JobTracker.pollInterval = 10
config.JobTracker.trackerName = "CondorTracker"
config.JobTracker.pluginDir = "WMComponent.JobTracker.Plugins"
config.JobTracker.runTimeLimit = 7776000
config.JobTracker.idleTimeLimit = 7776000
config.JobTracker.heldTimeLimit = 7776000
config.JobTracker.unknTimeLimit = 7776000

config.component_("ErrorHandler")
config.ErrorHandler.namespace = "WMComponent.ErrorHandler.ErrorHandler"
config.ErrorHandler.componentDir  = config.General.workDir + "/ErrorHandler"
config.ErrorHandler.logLevel = "DEBUG"
config.ErrorHandler.maxRetries = 3
config.ErrorHandler.pollInterval = 10

config.component_("RetryManager")
config.RetryManager.namespace = "WMComponent.RetryManager.RetryManager"
config.RetryManager.componentDir  = config.General.workDir + "/RetryManager"
config.RetryManager.logLevel = "DEBUG"
config.RetryManager.pollInterval = 10
config.RetryManager.coolOffTime = {"create": 10, "submit": 10, "job": 10}
config.RetryManager.pluginName = "RetryAlgo"

config.component_("JobArchiver")
config.JobArchiver.namespace = "WMComponent.JobArchiver.JobArchiver"
config.JobArchiver.componentDir  = config.General.workDir + "/JobArchiver"
config.JobArchiver.pollInterval = 60
config.JobArchiver.logLevel = "DEBUG"
config.JobArchiver.logDir = config.General.workDir + "/JobArchives"
config.JobArchiver.numberOfJobsToCluster = 1000

config.component_("TaskArchiver")
config.TaskArchiver.namespace = "WMComponent.TaskArchiver.TaskArchiver"
config.TaskArchiver.componentDir  = config.General.workDir + "/TaskArchiver"
config.TaskArchiver.logLevel = "DEBUG"
config.TaskArchiver.pollInterval = 10
config.TaskArchiver.timeOut      = 3600
# TaskArchiver workQueueParams should be empty.
# Or both ParentQueue and QueueURL need to be set (Although they are not used)
config.TaskArchiver.WorkQueueParams = {}
config.TaskArchiver.useWorkQueue = True

config.webapp_('WorkQueueService')
config.WorkQueueService.componentDir = config.General.workDir + "/WorkQueueService"
## User specific parameter
config.WorkQueueService.Webtools.port = 9997
config.WorkQueueService.Webtools.host = config.Agent.hostName
config.WorkQueueService.templates = os.path.join(WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools')
config.WorkQueueService.admin = config.Agent.contact
config.WorkQueueService.title = 'WorkQueue Data Service'
config.WorkQueueService.description = 'Provide WorkQueue related service call'
config.WorkQueueService.section_('views')
config.WorkQueueService.views.section_('active')
workqueue = config.WorkQueueService.views.active.section_('workqueue')
workqueue.object = 'WMCore.WebTools.RESTApi'
workqueue.templates = os.path.join(WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools/')
workqueue.section_('model')
workqueue.model.object = 'WMCore.HTTPFrontEnd.WorkQueue.WorkQueueRESTModel'
workqueue.section_('formatter')
workqueue.formatter.object = 'WMCore.HTTPFrontEnd.WorkQueue.WorkQueueRESTFormatter'
workqueue.serviceModules = ['WMCore.HTTPFrontEnd.WorkQueue.Services.WorkQueueService']
workqueue.queueParams = getattr(config.WorkQueueManager, 'queueParams', {})
workqueue.queueParams.setdefault('CacheDir', config.General.workDir + 'WorkQueueManager/wf')
workqueue.queueParams.setdefault('QueueURL', 'http://%s:%s/%s' % (config.Agent.hostName,
                                                                  config.WorkQueueService.Webtools.port,
                                                                  'workqueue'))
workqueuemonitor = config.WorkQueueService.views.active.section_('workqueuemonitor')
workqueuemonitor.object = 'WMCore.HTTPFrontEnd.WorkQueue.WorkQueueMonitorPage'
workqueuemonitor.templates = os.path.join(WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools')
workqueuemonitor.javascript = os.path.join(WMCore.WMInit.getWMBASE(), 'src/javascript/')
workqueuemonitor.html = os.path.join(WMCore.WMInit.getWMBASE(), 'src/html/')

config.webapp_("WMBSMonitoring")
config.WMBSMonitoring.componentDir = config.General.workDir + "/WMBSMonitoring"
config.WMBSMonitoring.Webtools.host = config.Agent.hostName
## User specific parameter
config.WMBSMonitoring.Webtools.port = 8087
config.WMBSMonitoring.templates = WMCore.WMInit.getWMBASE() + '/src/templates/WMCore/WebTools'
config.WMBSMonitoring.admin = "sfoulkes@fnal.gov"
config.WMBSMonitoring.title = "WMBS Monitoring"
config.WMBSMonitoring.description = "Monitoring of a WMBS instance"
config.WMBSMonitoring.instance = "ReReco WMAGENT"
## User specific parameter
config.WMBSMonitoring.couchURL = "http://cmssrv52:5984/_utils/document.html?wmagent_commissioning/"
config.WMBSMonitoring.section_('views')
config.WMBSMonitoring.views.section_('active')
config.WMBSMonitoring.views.active.section_('wmbs')
config.WMBSMonitoring.views.active.wmbs.section_('model')
config.WMBSMonitoring.views.active.wmbs.section_('formatter')
config.WMBSMonitoring.views.active.wmbs.object = 'WMCore.WebTools.RESTApi'
config.WMBSMonitoring.views.active.wmbs.templates = WMCore.WMInit.getWMBASE() + '/src/templates/WMCore/WebTools/'
config.WMBSMonitoring.views.active.wmbs.model.object = 'WMCore.HTTPFrontEnd.WMBS.WMBSRESTModel'
config.WMBSMonitoring.views.active.wmbs.formatter.object = 'WMCore.WebTools.DASRESTFormatter'

wmbsmonitor = config.WMBSMonitoring.views.active.section_('wmbsmonitor')
wmbsmonitor.object = 'WMCore.HTTPFrontEnd.WMBS.WMBSMonitorPage'
wmbsmonitor.templates = os.path.join(WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools')
wmbsmonitor.javascript = os.path.join(WMCore.WMInit.getWMBASE(), 'src/javascript')
wmbsmonitor.html = os.path.join(WMCore.WMInit.getWMBASE(), 'src/html')

# REST service for WMComponents running (WorkQueueManager in this case)
wmagent = config.WMBSMonitoring.views.active.section_('wmagent')
# The class to load for this view/page
wmagent.object = 'WMCore.WebTools.RESTApi'
wmagent.templates = os.path.join( WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools/')
wmagent.section_('model')
wmagent.model.object = 'WMCore.HTTPFrontEnd.Agent.AgentRESTModel'
wmagent.section_('formatter')
wmagent.formatter.object = 'WMCore.WebTools.RESTFormatter'

wmagentmonitor = config.WMBSMonitoring.views.active.section_('wmagentmonitor')
wmagentmonitor.object = 'WMCore.HTTPFrontEnd.Agent.AgentMonitorPage'
wmagentmonitor.templates = os.path.join(WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools')
wmagentmonitor.javascript = os.path.join(WMCore.WMInit.getWMBASE(), 'src/javascript/')
wmagentmonitor.html = os.path.join(WMCore.WMInit.getWMBASE(), 'src/html/')
