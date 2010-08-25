#!/usr/bin/env python
"""
WMAgent Configuration

Sample WMAgent configuration.
"""

__revision__ = "$Id: WMAgentConfig.py,v 1.6 2010/03/22 19:06:24 sryu Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.Configuration import Configuration
config = Configuration()

config.section_("Agent")
config.Agent.hostName = "cmssrv52.fnal.gov"
config.Agent.contact = "sfoulkes@fnal.gov"
config.Agent.teamName = "DMWM"
config.Agent.agentName = "ReRecoDOMINATOR"

config.section_("General")
config.General.workDir = "/home/sfoulkes/WMAgent/work"

config.section_("JobStateMachine")
config.JobStateMachine.couchurl = "cmssrv52.fnal.gov:5984"
config.JobStateMachine.couchDBName = "wmagent_commissioning"
config.JobStateMachine.default_retries = 5

config.section_("CoreDatabase")
config.CoreDatabase.socket = "/opt/MySQL-5.1/var/lib/mysql/mysql.sock"
config.CoreDatabase.connectUrl = "mysql://sfoulkes:@localhost/WMAgentDB_sfoulkes"

config.component_("DBSUpload")
config.DBSUpload.namespace = "WMComponent.DBSUpload.DBSUpload"
config.DBSUpload.componentDir = config.General.workDir + "/DBSUpload"
config.DBSUpload.logLevel = "DEBUG"
config.DBSUpload.maxThreads = 1
config.DBSUpload.dbsurl = "https://cmst0dbs.cern.ch:8443/cms_dbs_prod_tier0_writer/servlet/DBSServlet"
config.DBSUpload.dbsversion = "DBS_2_0_8"
config.DBSUpload.globalDBSUrl = "https://cmsdbsprod.cern.ch:8443/cms_dbs_prod_global_writer/servlet/DBSServlet"
config.DBSUpload.globalDBSVer = "DBS_2_0_8"
config.DBSUpload.uploadFileMax = 10
config.DBSUpload.pollInterval = 100
config.DBSUpload.DBSMaxSize = 1000000000000
config.DBSUpload.DBSMaxFiles = 100
config.DBSUpload.DBSBlockTimeout = 21600

config.component_("PhEDExInjector")
config.PhEDExInjector.namespace = "WMComponent.PhEDExInjector.PhEDExInjector"
config.PhEDExInjector.componentDir = config.General.workDir + "/PhEDExInjector"
config.PhEDExInjector.logLevel = "DEBUG"
config.PhEDExInjector.maxThreads = 1
config.PhEDExInjector.phedexurl = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/"
config.PhEDExInjector.pollInterval = 100


config.component_("WorkQueueManager")
config.WorkQueueManager.namespace = "WMComponent.WorkQueueManager.WorkQueueManager"
config.WorkQueueManager.componentDir = config.General.workDir + "/WorkQueueManager"
config.WorkQueueManager.level = "LocalQueue"
config.WorkQueueManager.queueParams = {'ParentQueue' : 'http://cmssrv52.fnal.gov:8570/workqueue'}

config.webapp_('WorkQueueService')
config.WorkQueueService.server.port = 8579
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
workqueue.serviceModules = ['WMCore.HTTPFrontEnd.WorkQueue.Services.WorkQueueService']
workqueue.queueParams = getattr(config.WorkQueueManager, 'queueParams', {})


config.component_("JobAccountant")
config.JobAccountant.namespace = "WMComponent.JobAccountant.JobAccountant"
config.JobAccountant.componentDir = config.General.workDir + "/JobAccountant"
config.JobAccountant.logLevel = "DEBUG"
config.JobAccountant.workerThreads = 2
config.JobAccountant.pollInterval = 60

config.component_("JobCreator")
config.JobCreator.namespace = "WMComponent.JobCreator.JobCreator"
config.JobCreator.componentDir = config.General.workDir + "/JobCreator"
config.JobCreator.logLevel = "DEBUG"
config.JobCreator.maxThreads = 1
config.JobCreator.UpdateFromSiteDB = True
config.JobCreator.pollInterval = 10
config.JobCreator.jobCacheDir = config.General.workDir + "/JobCache"
config.JobCreator.defaultJobType = "Processing"
config.JobCreator.workerThreads = 2
config.JobCreator.useWorkQueue = False
if config.JobCreator.useWorkQueue:
    # take queueParams from WorkQueueManager - specify here to override
    config.JobCreator.WorkQueueParams = getattr(config.WorkQueueManager, 'queueParams', {})

config.component_("JobSubmitter")
config.JobSubmitter.namespace = "WMComponent.JobSubmitter.JobSubmitter"
config.JobSubmitter.componentDir = config.General.workDir + "/JobSubmitter"
config.JobSubmitter.logLevel = "DEBUG"
config.JobSubmitter.maxThreads = 1
config.JobSubmitter.pollInterval = 10
config.JobSubmitter.pluginName = "ShadowPoolPlugin"
config.JobSubmitter.pluginDir = "JobSubmitter.Plugins"
config.JobSubmitter.submitDir = config.General.workDir + "/SubmitJDLs"
config.JobSubmitter.submitNode = "cms-sleepgw.fnal.gov"
config.JobSubmitter.submitScript = "submit.sh"
config.JobSubmitter.workerThreads = 1
config.JobSubmitter.jobsPerWorker = 100

config.component_("JobTracker")
config.JobTracker.namespace = "WMComponent.JobTracker.JobTracker"
config.JobTracker.componentDir  = config.General.workDir + "/JobTracker"
config.JobTracker.logLevel = "DEBUG"
config.JobTracker.pollInterval = 10
config.JobTracker.trackerName = "TestTracker"
config.JobTracker.pluginDir = "WMComponent.JobTracker.Plugins"
config.JobTracker.runTimeLimit = 7776000
config.JobTracker.idleTimeLimit = 7776000
config.JobTracker.heldTimeLimit = 7776000
config.JobTracker.unknTimeLimit = 7776000

config.component_("ErrorHandler")
config.ErrorHandler.namespace = "WMComponent.ErrorHandler.ErrorHandler"
config.ErrorHandler.componentDir  = config.General.workDir + "/ErrorHandler"
config.ErrorHandler.logLevel = "DEBUG"
config.ErrorHandler.maxRetries = 10
config.ErrorHandler.pollInterval = 10

config.component_("RetryManager")
config.RetryManager.namespace = "WMComponent.RetryManager.RetryManager"
config.RetryManager.componentDir  = config.General.workDir + "/RetryManager"
config.RetryManager.logLevel = "DEBUG"
config.RetryManager.pollInterval = 10
config.RetryManager.coolOffTime = {"create": 10, "submit": 10, "job": 10}
config.RetryManager.pluginPath = 'WMComponent.RetryManager.PlugIns'
config.RetryManager.pluginName = "CreateRetryAlgo"

config.component_("JobArchiver")
config.JobArchiver.namespace = "WMComponent.JobArchiver.JobArchiver"
config.JobArchiver.componentDir  = config.General.workDir + "/JobArchiver"
config.JobArchiver.pollInterval = 10
config.JobArchiver.logLevel = "DEBUG"
config.JobArchiver.logDir = config.General.workDir + "/JobArchives"

config.component_("TaskArchiver")
config.TaskArchiver.logLevel = "DEBUG"
config.TaskArchiver.pollInterval = 10
config.TaskArchiver.timeOut      = 0
config.TaskArchiver.WorkQueueParams = getattr(config.WorkQueueManager, 'queueParams', {})
