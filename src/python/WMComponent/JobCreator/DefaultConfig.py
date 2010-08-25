#!/usr/bin/env python
#pylint: disable-msg=E1101,E1103,C0103,R0902
"""
Defines default config values for DBSUpload specific
parameters.
"""
__all__ = []




from WMCore.Agent.Configuration import Configuration
import os
import os.path


config = Configuration()
config.component_("JobCreator")
config.JobCreator.namespace = 'WMComponent.JobCreator.JobCreator'
#The log level of the component. 
#config.JobCreator.logLevel = 'SQLDEBUG'
config.JobCreator.logLevel = 'INFO'

# maximum number of threads we want to deal
# with messages per pool.
config.JobCreator.maxThreads                = 1
config.JobCreator.UpdateFromResourceControl = True
config.JobCreator.pollInterval              = 10
config.JobCreator.jobCacheDir               = os.path.join(os.getcwd(), 'test')
config.JobCreator.defaultJobType            = 'processing' #Type of jobs that we run, used for resource control
config.JobCreator.workerThreads             = 2
config.JobCreator.componentDir              = os.getcwd()
config.JobCreator.useWorkQueue              = False

if config.JobCreator.useWorkQueue:
    # take queueParams from WorkQueueManager - specify here to override
    config.JobCreator.WorkQueuParams = getattr(config.WorkQueueManager, 'queueParams', {})
    
#We now call the JobMaker from here
config.component_('JobMaker')
config.JobMaker.logLevel        = 'INFO'
config.JobMaker.namespace       = 'WMCore.WMSpec.Makers.JobMaker'
config.JobMaker.maxThreads      = 1
config.JobMaker.makeJobsHandler = 'WMCore.WMSpec.Makers.Handlers.MakeJobs'

jsm = config.component_('JobStateMachine')

if (os.getenv('COUCHURL') != None):
    couchurl = os.getenv('COUCHURL')
else:
    couchurl = 'cmssrv48.fnal.gov:5984'

jsm.couchurl = couchurl
jsm.default_retries = 1
jsm.couchDBName     = "mnorman_test"
