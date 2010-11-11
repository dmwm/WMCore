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
config.Agent.hostName = "localhost"
## User specific parameter
config.Agent.contact = "sfoulkes@fnal.gov"
## User specific parameter
config.Agent.teamName = "DMWM"
## User specific parameter
config.Agent.agentName = "WMAgentCommissioning"
config.Agent.useMsgService = False
config.Agent.useTrigger = False
config.Agent.useHeartbeat = False



config.section_("General")
config.General.workDir = "/storage/local/data1/wmagent/work"



config.section_("JobStateMachine")
## User specific parameter
config.JobStateMachine.couchurl = "http://localhost:5984"
## User specific parameter
config.JobStateMachine.couchDBName = "wmagent_commissioning"
## User specific parameter
config.JobStateMachine.configCacheDBName = "wmagent_config_cache"
config.JobStateMachine.default_retries = 5



# ACDC - Analysis Collection Data Collection
# service which collects information about failed jobs for their resubmitting,
# it's for use if you intend to use ACDC in the !ErrorHandler
config.section_('ACDC')
## User specific parameter
config.ACDC.couchurl = "http://localhost:5984"
## User specific parameter
config.ACDC.database = "wmagent_acdc"


# database connection information
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
config.WorkQueueManager.serviceUrl = 'localhost:8570'
config.WorkQueueManager.pollInterval = 10
config.WorkQueueManager.queueParams = {"PopulateFilesets": True,
                                       "ParentQueue": "http://%s/workqueue/" % config.WorkQueueManager.serviceUrl,
                                       "QueueURL": "http://localhost:9997"} 



config.component_("DBSUpload")
config.DBSUpload.namespace = "WMComponent.DBSUpload.DBSUpload"
config.DBSUpload.componentDir = config.General.workDir + "/DBSUpload"
config.DBSUpload.logLevel = "DEBUG"
config.DBSUpload.maxThreads = 1
# period (in seconds) at which the DBSUpload components looks for new files
config.DBSUpload.pollInterval = 100
config.DBSUpload.workerThreads = 4



config.section_("DBSInterface")
# writer url to the global dbs instance
config.DBSInterface.globalDBSUrl     = "https://cmsdbsprod.cern.ch:8443/cms_dbs_prod_global_writer/servlet/DBSServlet"
# version of the software running the global DBS instance
config.DBSInterface.globalDBSVersion = 'DBS_2_0_8'
# writer url to the local dbs instance
config.DBSInterface.DBSUrl           = "https://cmst0dbs.cern.ch:8443/cms_dbs_prod_tier0_writer/servlet/DBSServlet"
# version of the software running the local DBS instance
config.DBSInterface.DBSVersion       = 'DBS_2_0_8'
# maximum number of files in a DBS block
config.DBSInterface.DBSBlockMaxFiles = 10
# maximum size (in bytes) of a block in DBS
config.DBSInterface.DBSBlockMaxSize  = 9999999999
config.DBSInterface.DBSBlockMaxTime  = 21600
config.DBSInterface.MaxFilesToCommit = 10



config.component_("PhEDExInjector")
config.PhEDExInjector.namespace = "WMComponent.PhEDExInjector.PhEDExInjector"
config.PhEDExInjector.componentDir = config.General.workDir + "/PhEDExInjector"
config.PhEDExInjector.logLevel = "DEBUG"
config.PhEDExInjector.maxThreads = 1
# has to point at the JSON version of the dataservice
config.PhEDExInjector.phedexurl = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/"
# determines how frequently the component will poll DBSBuffer looking for new files to inject [seconds]
config.PhEDExInjector.pollInterval = 100


# JobAccountant - responsible for parsing framework job reports from completed job and
# re-injecting files into WMBS and DBSBuffer
config.component_("JobAccountant")
config.JobAccountant.namespace = "WMComponent.JobAccountant.JobAccountant"
config.JobAccountant.componentDir = config.General.workDir + "/JobAccountant"
config.JobAccountant.logLevel = "DEBUG"
# determines how many worker threads are used by the component
config.JobAccountant.workerThreads = 1
# determines how often it will check WMBS looking for completed jobs
config.JobAccountant.pollInterval = 20



# JobCreator - this component will run the job splitting algorithms in WMBS
# and actually create the jobs
config.component_("JobCreator")
config.JobCreator.namespace = "WMComponent.JobCreator.JobCreator"
config.JobCreator.componentDir = config.General.workDir + "/JobCreator"
config.JobCreator.logLevel = "DEBUG"
config.JobCreator.maxThreads = 1
config.JobCreator.UpdateFromResourceControl = True
# determines the period at which the JobCreator polls the database looking for jobs to create
config.JobCreator.pollInterval = 10
config.JobCreator.jobCacheDir = config.General.workDir + "/JobCache"
config.JobCreator.defaultJobType = "Processing"
config.JobCreator.workerThreads = 2



# JobSubmitter - this component will submit jobs to the correct batch system/grid system
config.component_("JobSubmitter")
config.JobSubmitter.namespace     = "WMComponent.JobSubmitter.JobSubmitter"
config.JobSubmitter.componentDir  = config.General.workDir + "/JobSubmitter"
config.JobSubmitter.logLevel      = "DEBUG"
# determines the period at which the JobSubmitter polls the database looking for jobs to submit
config.JobSubmitter.pollInterval  = 10
# determines which node jobs will be submitted to
config.JobSubmitter.submitNode    = "localhost"
config.JobSubmitter.submitDir     = config.General.workDir + "/SubmitJDLs"
config.JobSubmitter.jobsPerWorker = 100
config.JobSubmitter.submitScript  = os.path.join(WMCore.WMInit.getWMBASE(),
                                                'src/python/WMComponent/JobSubmitter',
                                                'submit.sh')



config.component_("JobStatusLite")
config.JobStatusLite.namespace    = "WMComponent.JobStatusLite.JobStatusLite"
config.JobStatusLite.componentDir = config.General.workDir + "/JobStatusLite"
config.JobStatusLite.logLevel     = "DEBUG"
config.JobStatusLite.pollInterval = 30



config.section_("JobStatus")
config.JobStatus.stateTimeouts = {'Pending': 86400, 'Running': 86400, 'Error': 86400}
config.JobStatus.pollInterval  = 30



# JobTracker will poll the batch system to determine the state of submitted jobs,
# it will then update job state in the !JobStateMachine
config.component_("JobTracker")
config.JobTracker.namespace = "WMComponent.JobTracker.JobTracker"
config.JobTracker.componentDir  = config.General.workDir + "/JobTracker"
config.JobTracker.logLevel = "DEBUG"
config.JobTracker.pollInterval = 10



# ErrorHandler - this component handles jobs that have failed, it will either move them
# to a cooloff state or fail them if they have exhausted all their retries
config.component_("ErrorHandler")
config.ErrorHandler.namespace = "WMComponent.ErrorHandler.ErrorHandler"
config.ErrorHandler.componentDir  = config.General.workDir + "/ErrorHandler"
config.ErrorHandler.logLevel = "DEBUG"
# determines the number of times that a job will be retried
config.ErrorHandler.maxRetries = 3
# determines the frequency at which the ErrorHandler will poll the database looking for failed jobs
config.ErrorHandler.pollInterval = 10


# RetryManager -this component will move failed jobs from their cooloff state back
# into a state they can run again
config.component_("RetryManager")
config.RetryManager.namespace = "WMComponent.RetryManager.RetryManager"
config.RetryManager.componentDir  = config.General.workDir + "/RetryManager"
config.RetryManager.logLevel = "DEBUG"
# determines the frequency at which the RetryManager will poll the database looking for failed jobs
config.RetryManager.pollInterval = 10
# dictionary that defines the cool off period in seconds for the different failure modes
#    create - Job has errors being created
#    submit - Job fails during the submit stage
#    job - The job itself fails, or anything else after it has been submitted.
config.RetryManager.coolOffTime = {"create": 10, "submit": 10, "job": 10}
# determines which retry plugin to use
config.RetryManager.pluginName = "RetryAlgo"



# JobArchiver - this component will archive completed jobs and clean completed
# subscriptions out of WMBS
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
# determines the length between polling cycles 
config.TaskArchiver.pollInterval = 10
config.TaskArchiver.timeOut      = 3600
# TaskArchiver workQueueParams should be empty.
# Or both ParentQueue and QueueURL need to be set (Although they are not used)
config.TaskArchiver.WorkQueueParams = {}
config.TaskArchiver.useWorkQueue = True



config.section_("BossAir")
config.BossAir.pluginNames = ['TestPlugin', 'CondorPlugin']
config.BossAir.pluginDir   = 'WMCore.BossAir.Plugins'



config.webapp_('WorkQueueService')
config.WorkQueueService.default_expires = 0
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
workqueuemonitor.templates = os.path.join(WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools/WorkQueue')
workqueuemonitor.javascript = os.path.join(WMCore.WMInit.getWMBASE(), 'src/javascript/')
workqueuemonitor.html = os.path.join(WMCore.WMInit.getWMBASE(), 'src/html/')

config.WorkQueueService.views.active.section_('wmbs')
config.WorkQueueService.views.active.wmbs.section_('model')
config.WorkQueueService.views.active.wmbs.section_('formatter')
config.WorkQueueService.views.active.wmbs.object = 'WMCore.WebTools.RESTApi'
config.WorkQueueService.views.active.wmbs.templates = WMCore.WMInit.getWMBASE() + '/src/templates/WMCore/WebTools/'
config.WorkQueueService.views.active.wmbs.model.object = 'WMCore.HTTPFrontEnd.WMBS.WMBSRESTModel'
config.WorkQueueService.views.active.wmbs.formatter.object = 'WMCore.WebTools.DASRESTFormatter'

wmbsmonitor = config.WorkQueueService.views.active.section_('wmbsmonitor')
wmbsmonitor.object = 'WMCore.HTTPFrontEnd.WMBS.WMBSMonitorPage'
wmbsmonitor.templates = os.path.join(WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools/WMBS')
wmbsmonitor.javascript = os.path.join(WMCore.WMInit.getWMBASE(), 'src/javascript')
wmbsmonitor.html = os.path.join(WMCore.WMInit.getWMBASE(), 'src/html')

# REST service for WMComponents running (WorkQueueManager in this case)
wmagent = config.WorkQueueService.views.active.section_('wmagent')
# The class to load for this view/page
wmagent.object = 'WMCore.WebTools.RESTApi'
wmagent.templates = os.path.join( WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools/')
wmagent.section_('model')
wmagent.model.object = 'WMCore.HTTPFrontEnd.Agent.AgentRESTModel'
wmagent.section_('formatter')
wmagent.formatter.object = 'WMCore.WebTools.RESTFormatter'

wmagentmonitor = config.WorkQueueService.views.active.section_('wmagentmonitor')
wmagentmonitor.object = 'WMCore.HTTPFrontEnd.Agent.AgentMonitorPage'
wmagentmonitor.templates = os.path.join(WMCore.WMInit.getWMBASE(), 'src/templates/WMCore/WebTools')
wmagentmonitor.javascript = os.path.join(WMCore.WMInit.getWMBASE(), 'src/javascript/')
wmagentmonitor.html = os.path.join(WMCore.WMInit.getWMBASE(), 'src/html/')
