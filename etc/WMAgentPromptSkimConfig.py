#!/usr/bin/env python
"""
WMAgent Configuration

Sample WMAgent configuration for PromptSkimming.
"""




import os
import WMCore.WMInit

from WMCore.Configuration import Configuration
config = Configuration()

config.section_("Agent")
config.Agent.hostName = "cmssrv52.fnal.gov"
config.Agent.contact = "sfoulkes@fnal.gov"
config.Agent.teamName = "CMSDataOps"
config.Agent.agentName = "PrompSkimming"
config.Agent.useMsgService = False
config.Agent.useTrigger = False

config.section_("General")
config.General.workDir = "/storage/local/data1/wmagent/work"

config.section_("JobStateMachine")
config.JobStateMachine.couchurl = "http://cmssrv52.fnal.gov:8570"
config.JobStateMachine.couchDBName = "promptskim_commissioning"
config.JobStateMachine.configCacheDBName = "promptskim_config_cache"
config.JobStateMachine.default_retries = 5

config.section_("CoreDatabase")
config.CoreDatabase.socket = "/opt/MySQL-5.1/var/lib/mysql/mysql.sock"
config.CoreDatabase.connectUrl = "mysql://sfoulkes:@localhost/WMAgentDB_sfoulkes"

config.component_("PromptSkimScheduler")
config.PromptSkimScheduler.namespace = "WMComponent.PromptSkimScheduler.PromptSkimScheduler"
config.PromptSkimScheduler.componentDir  = config.General.workDir + "/PromptSkimScheduler"
config.PromptSkimScheduler.logLevel = "DEBUG"
config.PromptSkimScheduler.pollInterval = 10
config.PromptSkimScheduler.workloadCache = config.General.workDir + "/PromptSkimWorkloads"
config.PromptSkimScheduler.scramArch = "slc5_ia32_gcc434"
config.PromptSkimScheduler.cmsPath = "/uscmst1/prod/sw/cms"
config.PromptSkimScheduler.filesPerJob = 1
config.PromptSkimScheduler.maxMergeEvents = 100000
config.PromptSkimScheduler.maxMergeSize = 4294967296
config.PromptSkimScheduler.minMergeSize = 500000000
config.PromptSkimScheduler.maxMergeFiles = 50
config.PromptSkimScheduler.phedexURL = "http://cmsweb.cern.ch/phedex/datasvc/json/prod/"
config.PromptSkimScheduler.t0astURL = "oracle://sfoulkes:PASSWORD@cmscald:1521"


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
config.DBSInterface.DBSBlockMaxSize  = 1000000000000
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
config.TaskArchiver.timeOut      = 172800 # 2 days.
config.TaskArchiver.WorkQueueParams = {}
config.TaskArchiver.useWorkQueue = False
