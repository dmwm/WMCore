#/usr/bin/env python
"""
WMAgent Configuration

Sample WMAgent configuration.
"""

__revision__ = "$Id: WMAgentConfig.py,v 1.2 2010/01/26 22:03:40 mnorman Exp $"
__version__ = "$Revision: 1.2 $"

import os

from WMCore.WMInit import getWMBASE
from WMCore.Configuration import Configuration

# The following parameters may need to be changed.
serverHostName = "HOSTNAME OF WMAGENT MACHINE"
reqMgrServerHostName = "HOSTNAME OF REQMGR MACHINE"
reqMgrPort = 8687
globalWorkQueuePort = 8571
localWorkQueuePort = 9997

# The work directory and database need to be separate from the ReqMgr
# installation.
workDirectory = "WMAGENT WORK DIR"
databaseUrl = "mysql://DBUSER:DBSPASSWORD@localhost/WMAgentDB"
databaseSocket = "/opt/MySQL-5.1/var/lib/mysql/mysql.sock"

# The couch username and password needs to be added.  The GroupUser and
# ConfigCache couch apps need to be installed into the configcache couch
# database.  The JobDump couchapp needs to be installed into the jobdump
# database.  The GroupUser and ACDC couchapps needs to be install into the
# acdc database.
couchURL = "http://USERNAME:PASSWORD@COUCHSERVER:5984"
configCacheDBName = "wmagent_configcache"
jobDumpDBName = "wmagent_jobdump"
acdcDBName = "wmagent_acdc"

# Information for the workqueue, email of the administrator and the team names
# for this agent.
userEmail = "OP EMAIL"
agentTeams = "team1,team2,cmsdataops"

# List of BossAir plugins that this agent will use.
bossAirPlugins = ["CondorPlugin"]

# DBS Information.
localDBSUrl = "https://cmst0dbs.cern.ch:8443/cms_dbs_prod_tier0_writer/servlet/DBSServlet"
localDBSVersion = "DBS_2_0_8"
globalDBSUrl = "https://cmsdbsprod.cern.ch:8443/cms_dbs_prod_global_writer/servlet/DBSServlet"
globalDBSVersion = "DBS_2_0_8"
dbsMaxBlockSize = 1000000000000
dbsMaxBlockFiles = 100
dbsBlockTimeout = 21600

# Job retry information.  This includes the number of times a job will tried and
# how long it will sit in cool off.
maxJobRetries = 3
retryAlgo = "RetryAlgo"
retryAlgoParams = {"create": 3600, "submit": 3600, "job": 3600}

# The amount of time to wait after a workflow has completed before archiving it.
workflowArchiveTimeout = 3600

# Nothing beyond this point should need to be changed.
config = Configuration()

config.section_("Agent")
config.Agent.hostName = serverHostName
config.Agent.contact = userEmail
config.Agent.teamName = agentTeams
config.Agent.agentName = "WMAgentCommissioning"
config.Agent.useMsgService = False
config.Agent.useTrigger = False
config.Agent.useHeartbeat = False

config.section_("General")
config.General.workDir = workDirectory

config.section_("JobStateMachine")
config.JobStateMachine.couchurl = couchURL
config.JobStateMachine.couchDBName = jobDumpDBName
config.JobStateMachine.configCacheDBName = configCacheDBName
config.JobStateMachine.default_retries = 5

config.section_("ACDC")
config.ACDC.couchurl = couchURL
config.ACDC.database = acdcDBName

config.section_("BossAir")
config.BossAir.pluginDir = "WMCore.BossAir.Plugins"
config.BossAir.pluginNames = bossAirPlugins
config.BossAir.nCondorProcesses = 1

config.section_("JobStatus")
config.JobStatus.stateTimeouts = {}
config.JobStatus.pollInterval = 10

config.section_("CoreDatabase")
config.CoreDatabase.socket = databaseSocket
config.CoreDatabase.connectUrl = databaseUrl

config.component_('WorkQueueManager')
config.WorkQueueManager.namespace = "WMComponent.WorkQueueManager.WorkQueueManager"
config.WorkQueueManager.componentDir = config.General.workDir + "/WorkQueueManager"
config.WorkQueueManager.level = 'LocalQueue'
config.WorkQueueManager.logLevel = 'DEBUG'
config.WorkQueueManager.serviceUrl = "%s:%s" % (reqMgrServerHostName, globalWorkQueuePort)
config.WorkQueueManager.pollInterval = 10
config.WorkQueueManager.queueParams = {"PopulateFilesets": True,
                                       "ParentQueue": "http://%s/workqueue/" % config.WorkQueueManager.serviceUrl,
                                       "QueueURL": "http://%s:%s" % (serverHostName, localWorkQueuePort),
                                       "JobSlotMultiplier": 1000,
                                       "Teams": agentTeams,
                                       "FullReportInterval": 300} 

config.component_("DBSUpload")
config.DBSUpload.namespace = "WMComponent.DBSUpload.DBSUpload"
config.DBSUpload.componentDir = config.General.workDir + "/DBSUpload"
config.DBSUpload.logLevel = "DEBUG"
config.DBSUpload.workerThreads = 1
config.DBSUpload.pollInterval = 100

config.section_("DBSInterface")
config.DBSInterface.DBSUrl = localDBSUrl
config.DBSInterface.DBSVersion = localDBSVersion
config.DBSInterface.globalDBSUrl = globalDBSUrl
config.DBSInterface.globalDBSVersion = globalDBSVersion
config.DBSInterface.DBSBlockMaxSize = dbsMaxBlockSize
config.DBSInterface.DBSBlockMaxFiles = dbsMaxBlockFiles
config.DBSInterface.DBSBlockMaxTime = dbsBlockTimeout
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
config.JobCreator.workerThreads = 1

config.component_("JobSubmitter")
config.JobSubmitter.namespace = "WMComponent.JobSubmitter.JobSubmitter"
config.JobSubmitter.componentDir = config.General.workDir + "/JobSubmitter"
config.JobSubmitter.logLevel = "DEBUG"
config.JobSubmitter.maxThreads = 1
config.JobSubmitter.pollInterval = 10
config.JobSubmitter.submitDir = config.General.workDir + "/SubmitJDLs"
config.JobSubmitter.workerThreads = 1
config.JobSubmitter.jobsPerWorker = 100
config.JobSubmitter.submitScript = os.path.join(getWMBASE(), "src/python/WMComponent/JobSubmitter/submit.sh")

config.component_("JobTracker")
config.JobTracker.namespace = "WMComponent.JobTracker.JobTracker"
config.JobTracker.componentDir  = config.General.workDir + "/JobTracker"
config.JobTracker.logLevel = "DEBUG"
config.JobTracker.pollInterval = 10

config.component_("JobStatusLite")
config.JobStatusLite.namespace = "WMComponent.JobStatusLite.JobStatusLite"
config.JobStatusLite.componentDir  = config.General.workDir + "/JobStatusLite"
config.JobStatusLite.logLevel = "DEBUG"
config.JobStatusLite.pollInterval = 10

config.component_("ErrorHandler")
config.ErrorHandler.namespace = "WMComponent.ErrorHandler.ErrorHandler"
config.ErrorHandler.componentDir  = config.General.workDir + "/ErrorHandler"
config.ErrorHandler.logLevel = "DEBUG"
config.ErrorHandler.maxRetries = maxJobRetries
config.ErrorHandler.pollInterval = 10

config.component_("RetryManager")
config.RetryManager.namespace = "WMComponent.RetryManager.RetryManager"
config.RetryManager.componentDir  = config.General.workDir + "/RetryManager"
config.RetryManager.logLevel = "DEBUG"
config.RetryManager.pollInterval = 10
config.RetryManager.coolOffTime = retryAlgoParams
config.RetryManager.pluginName = retryAlgo

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
config.TaskArchiver.timeOut      = workflowArchiveTimeout
config.TaskArchiver.WorkQueueParams = {}
config.TaskArchiver.useWorkQueue = True

config.webapp_('WorkQueueService')
config.WorkQueueService.default_expires = 0
config.WorkQueueService.componentDir = os.path.join(config.General.workDir, "WorkQueueService")
config.WorkQueueService.Webtools.port = localWorkQueuePort
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
workqueuemonitor.css = os.path.join(getWMBASE(), 'src/css/')
workqueuemonitor.html = os.path.join(getWMBASE(), 'src/html/')

workqueue.queueParams = getattr(config.WorkQueueManager, 'queueParams', {})
