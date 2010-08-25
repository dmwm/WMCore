#!/usr/bin/env python
"""
WMAgent Configuration

Sample WMAgent configuration.
"""

__revision__ = "$Id: WMAgentConfig.py,v 1.7 2010/04/06 18:10:01 sfoulkes Exp $"
__version__ = "$Revision: 1.7 $"

import os
import WMCore.WMInit

from WMCore.Configuration import Configuration
config = Configuration()

config.section_("Agent")
config.Agent.hostName = "cmssrv52.fnal.gov"
config.Agent.contact = "sfoulkes@fnal.gov"
config.Agent.teamName = "DMWM"
config.Agent.agentName = "WMAgentCommissioning"
config.Agent.useMsgService = False
config.Agent.useTrigger = False

config.section_("General")
config.General.workDir = "/storage/local/data1/wmagent2/work"

config.section_("JobStateMachine")
config.JobStateMachine.couchurl = "cmssrv52.fnal.gov:5984"
config.JobStateMachine.couchDBName = "wmagent_commissioning"
config.JobStateMachine.default_retries = 5

config.section_("CoreDatabase")
config.CoreDatabase.socket = "/opt/MySQL-5.1/var/lib/mysql/mysql.sock"
config.CoreDatabase.connectUrl = "mysql://sfoulkes:@localhost/WMAgentDB2_sfoulkes"

config.component_('WorkQueueManager')
config.WorkQueueManager.namespace = "WMComponent.WorkQueueManager.WorkQueueManager"
config.WorkQueueManager.componentDir = config.General.workDir + "/WorkQueueManager"
config.WorkQueueManager.level = 'LocalQueue'
config.WorkQueueManager.logLevel = 'INFO'
config.WorkQueueManager.serviceUrl = 'cmssrv52.fnal.gov:8570'
config.WorkQueueManager.pollInterval = 10
config.WorkQueueManager.queueParams = {'PopulateFilesets' : True,
                                       'ParentQueue':'http://%s/workqueue/' % config.WorkQueueManager.serviceUrl,
                                       'QueueURL': "/storage/local/data1/workqueue/workWorkQueueManager"}

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

config.component_("JobAccountant")
config.JobAccountant.namespace = "WMComponent.JobAccountant.JobAccountant"
config.JobAccountant.componentDir = config.General.workDir + "/JobAccountant"
config.JobAccountant.logLevel = "DEBUG"
config.JobAccountant.workerThreads = 1
config.JobAccountant.pollInterval = 60

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
config.JobCreator.useWorkQueue = False
config.JobCreator.WorkQueueParams = getattr(config.WorkQueueManager, 'queueParams', {})

config.component_("JobSubmitter")
config.JobSubmitter.namespace = "WMComponent.JobSubmitter.JobSubmitter"
config.JobSubmitter.componentDir = config.General.workDir + "/JobSubmitter"
config.JobSubmitter.logLevel = "DEBUG"
config.JobSubmitter.maxThreads = 1
config.JobSubmitter.pollInterval = 10
config.JobSubmitter.pluginName = "CondorGlobusPlugin"
config.JobSubmitter.pluginDir = "JobSubmitter.Plugins"
config.JobSubmitter.submitNode = "cmssrv52.fnal.gov"
config.JobSubmitter.submitDir = config.General.workDir + "/SubmitJDLs"
config.JobSubmitter.submitScript  = os.path.join(WMCore.WMInit.getWMBASE(), 'test/python/WMComponent_t/JobSubmitter_t', 'submit.sh')
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
config.RetryManager.WMCoreBase = WMCore.WMInit.getWMBASE()

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
config.TaskArchiver.timeOut      = 0
config.TaskArchiver.useWorkQueue = False

config.webapp_('WorkQueueService')
config.WorkQueueService.componentDir = config.General.workDir + "/WorkQueueService"
config.WorkQueueService.server.port = 9997
config.WorkQueueService.server.host = config.Agent.hostName
config.WorkQueueService.templates = os.path.join(WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools')
config.WorkQueueService.admin = config.Agent.contact
config.WorkQueueService.title = 'WorkQueue Data Service'
config.WorkQueueService.description = 'Provide WorkQueue related service call'
config.WorkQueueService.section_('views')
config.WorkQueueService.views.section_('active')
config.WorkQueueService.views.active.section_('workqueue')
config.WorkQueueService.views.active.workqueue.object = 'WMCore.WebTools.RESTApi'
config.WorkQueueService.views.active.workqueue.templates = os.path.join(WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools/')
config.WorkQueueService.views.active.workqueue.section_('model')
config.WorkQueueService.views.active.workqueue.model.object = 'WMCore.HTTPFrontEnd.WorkQueue.WorkQueueRESTModel'
config.WorkQueueService.views.active.workqueue.section_('formatter')
config.WorkQueueService.views.active.workqueue.formatter.object = 'WMCore.HTTPFrontEnd.WorkQueue.WorkQueueRESTFormatter'
config.WorkQueueService.views.active.workqueue.serviceModules = ['WMCore.HTTPFrontEnd.WorkQueue.Services.WorkQueueService']
config.WorkQueueService.views.active.workqueue.queueParams = getattr(config.WorkQueueManager, 'queueParams', {})
