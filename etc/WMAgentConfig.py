# /usr/bin/env python
# pylint: disable=E1101,C0103
# E1101: ignore no member complaints here (created dynamically)
# C0103: also ignore invalid variable name
"""
WMAgent Configuration

Sample WMAgent configuration.
"""
import os
from Utils.PythonVersion import PY3
from WMCore.Configuration import Configuration

# job wrapper script
if PY3:
    submitScript = "etc/submit_py3.sh"
else:
    submitScript = "etc/submit.sh"

# The following parameters may need to be changed.
serverHostName = "HOSTNAME OF WMAGENT MACHINE"
wmbsServicePort = 9997

# The work directory and database need to be separate from the ReqMgr
# installation.
workDirectory = "WMAGENT WORK DIR"
databaseUrl = "oracle://DBUSER:DBPASSWORD@TNSNAME"
databaseUrl = "mysql://DBUSER:DBSPASSWORD@localhost/WMAgentDB"
databaseSocket = "/opt/MySQL-5.1/var/lib/mysql/mysql.sock"

# The couch username and password needs to be added.  The GroupUser and
# ConfigCache couch apps need to be installed into the configcache couch
# database.  The JobDump couchapp needs to be installed into the jobdump
# database.  The GroupUser and ACDC couchapps needs to be install into the
# acdc database.
couchURL = "http://USERNAME:PASSWORD@COUCHSERVER:5984"
logDBName = "wmagent_logdb"
jobDumpDBName = "wmagent_jobdump"
jobSummaryDBName = "wmagent_summary"
summaryStatsDBName = "stat_summary"
acdcDBName = "acdcserver"
workqueueDBName = 'workqueue'
workqueueInboxDbName = 'workqueue_inbox'
# example of workloadSummary url
workloadSummaryDB = "workloadsummary"
workloadSummaryURL = couchURL

# List of BossAir plugins that this agent will use.
bossAirPlugins = ["SimpleCondorPlugin"]

# Required for global pool accounting
glideInAcctGroup = "production"
glideInAcctGroupUser = "cmsdataops"

# DBS Information.
localDBSVersion = "DBS_2_0_8"
globalDBSUrl = "https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader"
globalDBSVersion = "DBS_2_0_8"

# Job retry information.  How long it will sit in cool off.
retryAlgoParams = {"create": 5000, "submit": 5000, "job": 5000}

# The amount of time to wait after a workflow has completed before archiving it.
workflowArchiveTimeout = 3600

# Global LogLevel
# For setting the general log level of the components
globalLogLevel = 'INFO'

# Contact information
# Used for email alerting
contactName = "cms-service-production-admins@cern.ch"

# Nothing beyond this point should need to be changed.
config = Configuration()

config.section_("Agent")
config.Agent.hostName = serverHostName
config.Agent.contact = contactName
config.Agent.teamName = "REPLACE_TEAM_NAME"
config.Agent.agentName = "WMAgent"
config.Agent.agentNumber = 0
config.Agent.useMsgService = False
config.Agent.useTrigger = False
config.Agent.useHeartbeat = True
config.Agent.isDocker = False

config.section_("General")
config.General.workDir = workDirectory
config.General.logdb_name = logDBName
config.General.central_logdb_url = "need to get from secrets file"
config.General.ReqMgr2ServiceURL = "ReqMgr2 rest service"
config.General.centralWMStatsURL = "Central WMStats URL"
# ReqMgrAux disk cache duration (in hours), set to 5 minutes: 5 / 60 = 0.083
config.General.ReqMgrAuxCacheDuration = 0.083

config.section_("JobStateMachine")
config.JobStateMachine.couchurl = couchURL
config.JobStateMachine.couchDBName = jobDumpDBName
config.JobStateMachine.jobSummaryDBName = jobSummaryDBName
config.JobStateMachine.summaryStatsDBName = summaryStatsDBName
# Amount of documents allowed in the ChangeState module for bulk commits
config.JobStateMachine.maxBulkCommitDocs = 250
# total allowed serialized size for the FJR document that is uploaded to wmagent_jobdump/fwjrs
# NOTE: this needs to be in sync with CouchDB couchdb.max_document_size parameter
# see: https://docs.couchdb.org/en/latest/config/couchdb.html#couchdb/max_document_size
config.JobStateMachine.fwjrLimitSize = 8 * 1000**2  # default: 8 million bytes (not 8MB!!!)

config.section_("ACDC")
config.ACDC.couchurl = "https://cmsweb.cern.ch/couchdb"
config.ACDC.database = acdcDBName

config.section_("WorkloadSummary")
config.WorkloadSummary.couchurl = couchURL
config.WorkloadSummary.database = workloadSummaryDB

config.section_("BossAir")
config.BossAir.pluginDir = "WMCore.BossAir.Plugins"
config.BossAir.pluginNames = bossAirPlugins
config.BossAir.nCondorProcesses = 1
config.BossAir.submitWMSMode = True
config.BossAir.acctGroup = glideInAcctGroup
config.BossAir.acctGroupUser = glideInAcctGroupUser

config.section_("CoreDatabase")
config.CoreDatabase.connectUrl = databaseUrl
# config.CoreDatabase.socket = databaseSocket

config.section_("DashboardReporter")
config.DashboardReporter.dashboardHost = "cms-jobmon.cern.ch"
config.DashboardReporter.dashboardPort = 8884

config.component_('WorkQueueManager')
config.WorkQueueManager.namespace = "WMComponent.WorkQueueManager.WorkQueueManager"
config.WorkQueueManager.componentDir = config.General.workDir + "/WorkQueueManager"
config.WorkQueueManager.level = 'LocalQueue'
config.WorkQueueManager.logLevel = globalLogLevel
config.WorkQueueManager.pollInterval = 180  # 3 min
config.WorkQueueManager.couchurl = couchURL
config.WorkQueueManager.dbname = workqueueDBName
config.WorkQueueManager.inboxDatabase = workqueueInboxDbName
config.WorkQueueManager.rucioUrl = "OVER_WRITE_BY_SECRETS"
config.WorkQueueManager.rucioAuthUrl = "OVER_WRITE_BY_SECRETS"
config.WorkQueueManager.queueParams = {}
config.WorkQueueManager.queueParams["ParentQueueCouchUrl"] = "https://cmsweb.cern.ch/couchdb/workqueue"
# this has to be unique for different work queue. This is just place holder
config.WorkQueueManager.queueParams["QueueURL"] = "http://%s:5984" % (config.Agent.hostName)
config.WorkQueueManager.queueParams["WorkPerCycle"] = 200  # don't pull more than this number of elements per cycle
config.WorkQueueManager.queueParams["QueueDepth"] = 0.5  # pull work from GQ for only half of the resources
# number of available elements to be retrieved within a single CouchDB http request
config.WorkQueueManager.queueParams["RowsPerSlice"] = 2500
# maximum number of available elements rows to be evaluated when acquiring GQ to LQ work
config.WorkQueueManager.queueParams["MaxRowsPerCycle"] = 50000
# Rucio accounts for input data locks and secondary data locks
config.WorkQueueManager.queueParams["rucioAccount"] = "wmcore_transferor"
config.WorkQueueManager.queueParams["rucioAccountPU"] = "wmcore_pileup"


config.component_("DBS3Upload")
config.DBS3Upload.namespace = "WMComponent.DBS3Buffer.DBS3Upload"
config.DBS3Upload.componentDir = config.General.workDir + "/DBS3Upload"
config.DBS3Upload.logLevel = globalLogLevel
config.DBS3Upload.workerThreads = 1
config.DBS3Upload.pollInterval = 100
# "https://cmsweb-prod.cern.ch/dbs/prod/global/DBSWriter" - production one
config.DBS3Upload.dbsUrl = "OVERWRITE_BY_SECRETS"
config.DBS3Upload.primaryDatasetType = "mc"
# provided a block name, this will dump all the block info in a json file
config.DBS3Upload.dumpBlockJsonFor = ""
# set DbsApi requests to use gzip enconding, thus sending compressed data
config.DBS3Upload.gzipEncoding = True
config.DBS3Upload.uploaderName = "WMAgent"

config.section_("DBSInterface")
config.DBSInterface.DBSUrl = globalDBSUrl
config.DBSInterface.DBSVersion = localDBSVersion
config.DBSInterface.globalDBSUrl = globalDBSUrl
config.DBSInterface.globalDBSVersion = globalDBSVersion
config.DBSInterface.MaxFilesToCommit = 200
config.DBSInterface.doGlobalMigration = False
config.DBSInterface.primaryDatasetType = "mc"

config.component_("JobAccountant")
config.JobAccountant.namespace = "WMComponent.JobAccountant.JobAccountant"
config.JobAccountant.componentDir = config.General.workDir + "/JobAccountant"
config.JobAccountant.logLevel = globalLogLevel
config.JobAccountant.workerThreads = 1
config.JobAccountant.pollInterval = 300
config.JobAccountant.specDir = config.General.workDir + "/JobAccountant/SpecCache"

config.component_("JobCreator")
config.JobCreator.namespace = "WMComponent.JobCreator.JobCreator"
config.JobCreator.componentDir = config.General.workDir + "/JobCreator"
config.JobCreator.logLevel = globalLogLevel
config.JobCreator.maxThreads = 1
config.JobCreator.UpdateFromResourceControl = True
config.JobCreator.pollInterval = 120
# This is now OPTIONAL: It defaults to the componentDir
# However: In a production instance, this should be run on a high performance
# disk, and should probably NOT be run on the same disk as the JobArchiver
config.JobCreator.jobCacheDir = config.General.workDir + "/JobCache"
config.JobCreator.defaultJobType = "Processing"
config.JobCreator.workerThreads = 1
# glidein restrictions used for resource estimation (per core)
config.JobCreator.GlideInRestriction = {"MinWallTimeSecs": 1 * 3600,  # 1h
                                        "MaxWallTimeSecs": 45 * 3600,  # pilot lifetime is usually 48h
                                        "MinRequestDiskKB": 1 * 1000 * 1000,  # 1GB
                                        "MaxRequestDiskKB": 20 * 1000 * 1000}  # site limit is ~27GB
config.component_("JobSubmitter")
config.JobSubmitter.namespace = "WMComponent.JobSubmitter.JobSubmitter"
config.JobSubmitter.componentDir = config.General.workDir + "/JobSubmitter"
config.JobSubmitter.logLevel = globalLogLevel
config.JobSubmitter.maxThreads = 1
config.JobSubmitter.pollInterval = 120
config.JobSubmitter.workerThreads = 1
config.JobSubmitter.jobsPerWorker = 100
config.JobSubmitter.maxJobsPerPoll = 1000
config.JobSubmitter.maxJobsToCache = 100000  # used to be 50k
config.JobSubmitter.cacheRefreshSize = 30000  # set -1 if cache need to refresh all the time.
config.JobSubmitter.skipRefreshCount = 20  # (If above the threshold meet, cache will updates every 20 polling cycle) 120 * 20 = 40 minutes
config.JobSubmitter.submitScript = os.path.join(os.environ["WMCORE_ROOT"], submitScript)
config.JobSubmitter.extraMemoryPerCore = 500  # in MB
config.JobSubmitter.drainGraceTime = 2 * 24 * 60 * 60  # in seconds

config.component_("JobTracker")
config.JobTracker.namespace = "WMComponent.JobTracker.JobTracker"
config.JobTracker.componentDir = config.General.workDir + "/JobTracker"
config.JobTracker.logLevel = globalLogLevel
config.JobTracker.pollInterval = 60

config.component_("JobStatusLite")
config.JobStatusLite.namespace = "WMComponent.JobStatusLite.JobStatusLite"
config.JobStatusLite.componentDir = config.General.workDir + "/JobStatusLite"
config.JobStatusLite.logLevel = globalLogLevel
config.JobStatusLite.pollInterval = 60
config.JobStatusLite.stateTimeouts = {"Error": 300, "Running": 169200, "Pending": 432000}

config.component_("JobUpdater")
config.JobUpdater.namespace = "WMComponent.JobUpdater.JobUpdater"
config.JobUpdater.componentDir = config.General.workDir + "/JobUpdater"
config.JobUpdater.logLevel = globalLogLevel
config.JobUpdater.pollInterval = 120

config.component_("ErrorHandler")
config.ErrorHandler.namespace = "WMComponent.ErrorHandler.ErrorHandler"
config.ErrorHandler.componentDir = config.General.workDir + "/ErrorHandler"
config.ErrorHandler.logLevel = globalLogLevel
config.ErrorHandler.pollInterval = 240
config.ErrorHandler.readFWJR = True
config.ErrorHandler.maxFailTime = 120000
config.ErrorHandler.maxProcessSize = 500

config.component_("RetryManager")
config.RetryManager.namespace = "WMComponent.RetryManager.RetryManager"
config.RetryManager.componentDir = config.General.workDir + "/RetryManager"
config.RetryManager.logLevel = globalLogLevel
config.RetryManager.pollInterval = 240
config.RetryManager.plugins = {"default": "SquaredAlgo"}
config.RetryManager.section_("SquaredAlgo")
config.RetryManager.SquaredAlgo.section_("default")
config.RetryManager.SquaredAlgo.default.coolOffTime = retryAlgoParams

config.component_("JobArchiver")
config.JobArchiver.namespace = "WMComponent.JobArchiver.JobArchiver"
config.JobArchiver.componentDir = config.General.workDir + "/JobArchiver"
config.JobArchiver.pollInterval = 120
config.JobArchiver.logLevel = globalLogLevel
config.JobArchiver.numberOfJobsToCluster = 1000
config.JobArchiver.numberOfJobsToArchive = 10000
# This is now OPTIONAL, it defaults to the componentDir
# HOWEVER: Is is HIGHLY recommended that you do NOT run this on the same
# disk as the JobCreator
# config.JobArchiver.logDir = config.General.workDir + "/JobArchives"

config.component_("TaskArchiver")
config.TaskArchiver.namespace = "WMComponent.TaskArchiver.TaskArchiver"
config.TaskArchiver.componentDir = config.General.workDir + "/TaskArchiver"
config.TaskArchiver.logLevel = globalLogLevel
config.TaskArchiver.pollInterval = 240
config.TaskArchiver.timeOut = workflowArchiveTimeout
config.TaskArchiver.useWorkQueue = True
config.TaskArchiver.workloadSummaryCouchURL = workloadSummaryURL
config.TaskArchiver.workloadSummaryCouchDBName = workloadSummaryDB
config.TaskArchiver.histogramKeys = ["PeakValueRss", "PeakValueVsize", "TotalJobTime", "AvgEventTime"]
config.TaskArchiver.perfPrimaryDatasets = ['SingleMu', 'MuHad', 'MinimumBias']
config.TaskArchiver.perfDashBoardMinLumi = 50
config.TaskArchiver.perfDashBoardMaxLumi = 9000
# dqm address -'https://cmsweb.cern.ch/dqm/dev/'
config.TaskArchiver.dqmUrl = "OVER_WRITE_BY_SECETES"
config.TaskArchiver.requireCouch = True
# set to False couch data if request mgr is not used (Tier0, PromptSkiming)
config.TaskArchiver.useReqMgrForCompletionCheck = True
config.TaskArchiver.localCouchURL = "%s/%s" % (config.JobStateMachine.couchurl,
                                               config.JobStateMachine.couchDBName)
config.TaskArchiver.localQueueURL = "%s/%s" % (config.WorkQueueManager.couchurl,
                                               config.WorkQueueManager.dbname)
config.TaskArchiver.localWMStatsURL = "%s/%s" % (config.JobStateMachine.couchurl,
                                                 config.JobStateMachine.jobSummaryDBName)
config.TaskArchiver.DataKeepDays = 0.125  # couhch history keeping days.
config.TaskArchiver.cleanCouchInterval = 60 * 20  # 20 min
config.TaskArchiver.archiveDelayHours = 24  # delay the archiving so monitor can still show. default 24 hours

# Alert framework configuration

# common 'Alert' section (Alert "senders" use these values to determine destination)
config.section_("Alert")
# destination for the alert messages
config.Alert.address = "tcp://127.0.0.1:6557"
# control channel (internal alert system commands)
config.Alert.controlAddr = "tcp://127.0.0.1:6559"


# mysql*Poller sections were made optional and are defined in the
# wmagent-mod-config file

# Email alert configuration
config.section_("EmailAlert")
config.EmailAlert.toAddr = [contactName] # additional emails can be appended to the list
config.EmailAlert.fromAddr = "noreply@cern.ch"
config.EmailAlert.smtpServer = "localhost"

config.component_("AnalyticsDataCollector")
config.AnalyticsDataCollector.namespace = "WMComponent.AnalyticsDataCollector.AnalyticsDataCollector"
config.AnalyticsDataCollector.componentDir = config.General.workDir + "/AnalyticsDataCollector"
config.AnalyticsDataCollector.logLevel = globalLogLevel
config.AnalyticsDataCollector.pollInterval = 600
config.AnalyticsDataCollector.localCouchURL = "%s/%s" % (config.JobStateMachine.couchurl,
                                                         config.JobStateMachine.couchDBName)
config.AnalyticsDataCollector.localQueueURL = "%s/%s" % (config.WorkQueueManager.couchurl,
                                                         config.WorkQueueManager.dbname)
config.AnalyticsDataCollector.localWMStatsURL = "%s/%s" % (config.JobStateMachine.couchurl,
                                                           config.JobStateMachine.jobSummaryDBName)
config.AnalyticsDataCollector.centralRequestDBURL = "Cental Request DB URL"
config.AnalyticsDataCollector.summaryLevel = "task"
config.AnalyticsDataCollector.couchProcessThreshold = 50
config.AnalyticsDataCollector.pluginName = None

config.component_("ArchiveDataReporter")
config.ArchiveDataReporter.namespace = "WMComponent.ArchiveDataReporter.ArchiveDataReporter"
config.ArchiveDataReporter.componentDir = config.General.workDir + "/ArchiveDataReporter"
config.ArchiveDataReporter.pollInterval = 300
config.ArchiveDataReporter.WMArchiveURL = None
config.ArchiveDataReporter.numDocsRetrievePerPolling = 1000  # number of documents needed to be polled each time
config.ArchiveDataReporter.numDocsUploadPerCall = 200  # number of documents upload each time in bulk to WMArchive

# AgentStatusWatcher has to be the last one in the config to avoid false alarms during startup
config.component_("AgentStatusWatcher")
config.AgentStatusWatcher.namespace = "WMComponent.AgentStatusWatcher.AgentStatusWatcher"
config.AgentStatusWatcher.componentDir = config.General.workDir + "/AgentStatusWatcher"
config.AgentStatusWatcher.logLevel = globalLogLevel
config.AgentStatusWatcher.resourceUpdaterPollInterval = 900  # [second]
config.AgentStatusWatcher.grafanaURL = "https://monit-grafana.cern.ch"
config.AgentStatusWatcher.grafanaToken = "OVERWRITE_BY_SECRETS"
config.AgentStatusWatcher.grafanaSSB = 9475  # monit-grafana API number for Site Status Board
config.AgentStatusWatcher.pendingSlotsSitePercent = 75  # [percent] Pending slots percent over site max running for a site
config.AgentStatusWatcher.pendingSlotsTaskPercent = 70  # [percent] Pending slots percent over task max running for tasks
config.AgentStatusWatcher.runningExpressPercent = 30  # [percent] Only used for tier0 agent
config.AgentStatusWatcher.runningRepackPercent = 10  # [percent] Only used for tier0 agent
config.AgentStatusWatcher.t1SitesCores = 30  # [percent] Only used for tier0 agent
config.AgentStatusWatcher.forceSiteDown = []  # List of sites to be forced to Down status
config.AgentStatusWatcher.onlySSB = False  # Set thresholds for sites only in SSB (Force all other to zero/down)
config.AgentStatusWatcher.enabled = True  # switch to enable or not this component
config.AgentStatusWatcher.agentPollInterval = 300
config.AgentStatusWatcher.drainStatusPollInterval = 3600
config.AgentStatusWatcher.defaultAgentsNumByTeam = 5
config.AgentStatusWatcher.enableAMQ = False
config.AgentStatusWatcher.userAMQ = "OVERWRITE_BY_SECRETS"
config.AgentStatusWatcher.passAMQ = "OVERWRITE_BY_SECRETS"
config.AgentStatusWatcher.topicAMQ = "OVERWRITE_BY_SECRETS"

config.component_("RucioInjector")
config.RucioInjector.namespace = "WMComponent.RucioInjector.RucioInjector"
config.RucioInjector.componentDir = config.General.workDir + "/RucioInjector"
config.RucioInjector.logLevel = globalLogLevel
config.RucioInjector.pollInterval = 300
config.RucioInjector.pollIntervalRules = 43200
config.RucioInjector.cacheExpiration = 2 * 24 * 60 * 60  # two days
config.RucioInjector.createBlockRules = True
config.RucioInjector.RSEPostfix = False  # enable it to append _Test to the RSE names
config.RucioInjector.metaDIDProject = "Production"
config.RucioInjector.containerDiskRuleParams = {"weight": "ddm_quota", "copies": 2, "grouping": "DATASET"}
config.RucioInjector.blockRuleParams = {}
# this RSEExpr below might be updated by wmagent-mod-config script
config.RucioInjector.containerDiskRuleRSEExpr = "(tier=2|tier=1)&cms_type=real&rse_type=DISK"
config.RucioInjector.rucioAccount = "OVER_WRITE_BY_SECRETS"
config.RucioInjector.rucioUrl = "OVER_WRITE_BY_SECRETS"
config.RucioInjector.rucioAuthUrl = "OVER_WRITE_BY_SECRETS"

config.component_("WorkflowUpdater")
config.WorkflowUpdater.namespace = "WMComponent.WorkflowUpdater.WorkflowUpdater"
config.WorkflowUpdater.componentDir = config.General.workDir + "/WorkflowUpdater"
config.WorkflowUpdater.logLevel = globalLogLevel
config.WorkflowUpdater.pollInterval = 8 * 60 * 60  # every 8 hours
config.WorkflowUpdater.rucioAccount = "wmcore_pileup"
config.WorkflowUpdater.rucioUrl = "OVER_WRITE_BY_SECRETS"
config.WorkflowUpdater.rucioAuthUrl = "OVER_WRITE_BY_SECRETS"
config.WorkflowUpdater.msPileupUrl = "OVER_WRITE_BY_SECRETS"
